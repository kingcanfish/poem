use std::{
    io::Error as IoError,
    sync::{Arc, Mutex},
};

use bytes::{Buf, BufMut, Bytes, BytesMut};
use futures_util::StreamExt;
use http_body_util::{BodyExt, StreamBody};
use hyper::{HeaderMap, body::Frame};
use poem::Body;
use sync_wrapper::SyncStream;

use crate::{
    Code, CompressionEncoding, Status, Streaming,
    client::BoxBody,
    codec::{Decoder, Encoder},
};

/// The default limit on the size of a received message, as in the other gRPC
/// implementations.
pub(crate) const DEFAULT_MAX_DECODING_MESSAGE_SIZE: usize = 4 * 1024 * 1024;

/// The default limit on the size of a sent message: none, besides the 4 GiB
/// its length prefix can describe.
pub(crate) const DEFAULT_MAX_ENCODING_MESSAGE_SIZE: usize = usize::MAX;

/// Encodes `message` into a length-prefixed message of at most `max_size`
/// bytes, not counting the prefix. The limit applies to the bytes sent, i.e.
/// after compression.
async fn encode_data_frame<T: Encoder>(
    encoder: &mut T,
    buf: &mut BytesMut,
    message: T::Item,
    compression: Option<CompressionEncoding>,
    max_size: usize,
) -> Result<Bytes, Status> {
    let Some(compression) = compression else {
        return encode_uncompressed_data_frame(encoder, buf, message, max_size);
    };

    buf.put_slice(&[1, 0, 0, 0, 0]);
    let mut data = BytesMut::new();
    let res = match encoder.encode(message, &mut data) {
        Ok(()) => compression.encode(&data).await,
        Err(err) => Err(err),
    };
    match res {
        Ok(data) => {
            buf.extend(data);
            finish_data_frame(buf, max_size)
        }
        Err(err) => {
            // Don't leave this frame's header in front of the next one.
            buf.clear();
            Err(Status::from_std_error(err))
        }
    }
}

fn encode_uncompressed_data_frame<T: Encoder>(
    encoder: &mut T,
    buf: &mut BytesMut,
    message: T::Item,
    max_size: usize,
) -> Result<Bytes, Status> {
    buf.put_slice(&[0, 0, 0, 0, 0]);
    if let Err(err) = encoder.encode(message, buf) {
        // Don't leave this frame's header, or part of the message, in front of
        // the next one.
        buf.clear();
        return Err(Status::from_std_error(err));
    }
    finish_data_frame(buf, max_size)
}

fn finish_data_frame(buf: &mut BytesMut, max_size: usize) -> Result<Bytes, Status> {
    let msg_len = buf.len() - 5;
    // The length prefix is 4 bytes, whatever the limit.
    let max_size = max_size.min(u32::MAX as usize);
    if msg_len > max_size {
        // Don't leave this message in front of the next one.
        buf.clear();
        return Err(Status::new(Code::ResourceExhausted).with_message(format!(
            "trying to send message larger than max ({msg_len} vs. {max_size})"
        )));
    }
    buf.as_mut()[1..5].copy_from_slice(&(msg_len as u32).to_be_bytes());
    Ok(buf.split().freeze())
}

struct DataFrameDecoder {
    buf: BytesMut,
    compression: Option<CompressionEncoding>,
    max_size: usize,
}

impl DataFrameDecoder {
    /// Creates a decoder of length-prefixed messages of at most `max_size`
    /// bytes, both as received and once decompressed.
    #[inline]
    fn new(compression: Option<CompressionEncoding>, max_size: usize) -> Self {
        Self {
            buf: BytesMut::new(),
            compression,
            max_size,
        }
    }

    #[inline]
    fn put_slice(&mut self, data: impl AsRef<[u8]>) {
        self.buf.extend_from_slice(data.as_ref());
    }

    #[inline]
    fn check_incomplete(&self) -> Result<(), Status> {
        if !self.buf.is_empty() {
            return Err(Status::new(Code::Internal).with_message("incomplete request"));
        }
        Ok(())
    }

    async fn next(&mut self) -> Result<Option<Bytes>, Status> {
        if self.buf.len() < 5 {
            return Ok(None);
        }

        let compressed = match self.buf[0] {
            1 => true,
            0 => false,
            compressed => Err(Status::new(Code::Unimplemented)
                .with_message(format!("unsupported compressed flag: {compressed}")))?,
        };

        // Checked as soon as the prefix is read, so no more than `max_size`
        // bytes, plus the frame they end in, are ever buffered for a message.
        let len = u32::from_be_bytes(self.buf[1..5].try_into().unwrap()) as usize;
        if len > self.max_size {
            return Err(Status::new(Code::ResourceExhausted).with_message(format!(
                "received message larger than max ({len} vs. {})",
                self.max_size
            )));
        }

        if self.buf.len() - 5 >= len {
            self.buf.advance(5);
            let data = self.buf.split_to(len).freeze();

            if compressed {
                let compression = self.compression.ok_or_else(|| {
                    Status::new(Code::Unimplemented)
                        .with_message(format!("unsupported compressed flag: {compressed}"))
                })?;
                let data = compression
                    .decode(&data, self.max_size)
                    .await
                    .map_err(|err| Status::new(Code::Internal).with_message(err.to_string()))?
                    .ok_or_else(|| {
                        Status::new(Code::ResourceExhausted).with_message(format!(
                            "received message after decompression larger than max ({})",
                            self.max_size
                        ))
                    })?;
                Ok(Some(data.into()))
            } else {
                Ok(Some(data))
            }
        } else {
            Ok(None)
        }
    }
}

pub(crate) fn create_decode_request_body<T: Decoder>(
    mut decoder: T,
    body: Body,
    compression: Option<CompressionEncoding>,
    max_message_size: usize,
) -> Streaming<T::Item> {
    let mut body: BoxBody = body.into();

    Streaming::new(async_stream::try_stream! {
        let mut frame_decoder = DataFrameDecoder::new(compression, max_message_size);

        loop {
            match body.frame().await.transpose().map_err(Status::from_std_error)? {
                Some(frame) => {
                    if let Ok(data) = frame.into_data() {
                        frame_decoder.put_slice(data);
                        while let Some(data) = frame_decoder.next().await? {
                            let message = decoder.decode(&data).map_err(Status::from_std_error)?;
                            yield message;
                        }
                    }
                }
                None => {
                    frame_decoder.check_incomplete()?;
                    break;
                }
            }
        }
    })
}

/// Encodes the messages of a response, followed by the trailers carrying its
/// status.
///
/// An error from `stream`, or a message that fails to encode, ends the
/// response with that error as its status: `stream` is not read any further,
/// and nothing follows the trailers.
pub(crate) fn create_encode_response_body<T: Encoder>(
    mut encoder: T,
    mut stream: Streaming<T::Item>,
    compression: Option<CompressionEncoding>,
    max_message_size: usize,
) -> Body {
    let stream = async_stream::stream! {
        let mut buf = BytesMut::new();
        let mut status = Status::new(Code::Ok);

        while let Some(item) = stream.next().await {
            let res = match item {
                Ok(message) => {
                    encode_data_frame(&mut encoder, &mut buf, message, compression, max_message_size)
                        .await
                }
                Err(err) => Err(err),
            };
            match res {
                Ok(data) => yield Ok::<_, IoError>(Frame::data(data)),
                Err(err) => {
                    status = err;
                    break;
                }
            }
        }

        yield Ok(Frame::trailers(status.to_headers()));
    };

    BodyExt::boxed(StreamBody::new(SyncStream::new(stream))).into()
}

/// The error that ended a streaming request before its end, shared between
/// the request body and the call so the call fails with it.
#[derive(Clone, Default)]
pub(crate) struct RequestError(Arc<Mutex<Option<Status>>>);

impl RequestError {
    fn set(&self, status: Status) {
        *self.0.lock().unwrap() = Some(status);
    }

    /// The error, if the request ended with one.
    pub(crate) fn get(&self) -> Option<Status> {
        self.0.lock().unwrap().clone()
    }
}

/// Encodes the messages of a streaming request.
///
/// An error from `stream`, or a message that fails to encode, ends the body
/// with an error, which resets the HTTP/2 stream: ending it cleanly would make
/// the server take the messages sent so far for the whole input. The error is
/// stored in `error`.
pub(crate) fn create_encode_request_body<T: Encoder>(
    mut encoder: T,
    mut stream: Streaming<T::Item>,
    compression: Option<CompressionEncoding>,
    max_message_size: usize,
    error: RequestError,
) -> Body {
    let stream = async_stream::stream! {
        let mut buf = BytesMut::new();

        while let Some(item) = stream.next().await {
            let status = match item {
                Ok(message) => {
                    match encode_data_frame(&mut encoder, &mut buf, message, compression, max_message_size).await {
                        Ok(data) => {
                            yield Ok(Frame::data(data));
                            continue;
                        }
                        Err(status) => status,
                    }
                }
                Err(status) => status,
            };
            error.set(status.clone());
            yield Err(IoError::other(status));
            break;
        }
    };

    BodyExt::boxed(StreamBody::new(SyncStream::new(stream))).into()
}

/// Encodes the only message of a non-client-streaming request into a complete
/// body, so the END_STREAM flag is sent on its DATA frame instead of in a
/// separate, empty one that may reach the server after it has responded.
pub(crate) async fn create_encode_unary_request_body<T: Encoder>(
    mut encoder: T,
    message: T::Item,
    compression: Option<CompressionEncoding>,
    max_message_size: usize,
) -> Result<Body, Status> {
    let mut buf = BytesMut::new();
    let data = match compression {
        // Boxed, so the compressor's state doesn't grow every call's future.
        Some(_) => {
            Box::pin(encode_data_frame(
                &mut encoder,
                &mut buf,
                message,
                compression,
                max_message_size,
            ))
            .await
        }
        None => encode_uncompressed_data_frame(&mut encoder, &mut buf, message, max_message_size),
    }?;
    Ok(Body::from_bytes(data))
}

/// How many bytes of a response body the client reads past an error to reach
/// its end. Dropping a body before its end resets the stream, and h2 closes
/// the connection after too many resets.
const MAX_DISCARDED_BYTES: usize = 64 * 1024;

/// Reads and discards the rest of `body`, up to [`MAX_DISCARDED_BYTES`].
///
/// It is not bounded in time: a peer that stalls before ending the body delays
/// the error until the call's own timeout, if any.
pub(crate) async fn discard_body(body: Body) {
    let mut body: BoxBody = body.into();
    let mut discarded = 0;
    while let Some(Ok(frame)) = body.frame().await {
        if let Ok(data) = frame.into_data() {
            discarded += data.len();
            if discarded > MAX_DISCARDED_BYTES {
                break;
            }
        }
    }
}

/// Decodes the only message of a non-server-streaming response.
///
/// The body is read up to the trailers even after an error, since the status
/// is only known from them and dropping the body early resets the stream. A
/// non-OK status in the trailers takes precedence over errors found locally.
pub(crate) async fn decode_single_response_message<T: Decoder>(
    mut decoder: T,
    headers: &HeaderMap,
    body: Body,
    compression: Option<CompressionEncoding>,
    max_message_size: usize,
) -> Result<T::Item, Status> {
    // check is trailers-only
    if let Some(status) = Status::from_headers(headers)? {
        return Err(if status.is_ok() {
            missing_response_message()
        } else {
            status
        });
    }

    let mut body: BoxBody = body.into();
    let mut frame_decoder = DataFrameDecoder::new(compression, max_message_size);
    let mut result: Option<Result<T::Item, Status>> = None;
    let mut discarded = 0;

    while let Some(frame) = body
        .frame()
        .await
        .transpose()
        .map_err(Status::from_std_error)?
    {
        let frame = match frame.into_data() {
            Ok(data) => {
                if result.is_some() {
                    if matches!(result, Some(Ok(_))) && !data.is_empty() {
                        result = Some(Err(extra_response_message()));
                    }
                    discarded += data.len();
                    if discarded > MAX_DISCARDED_BYTES {
                        break;
                    }
                    continue;
                }
                frame_decoder.put_slice(data);
                result = match frame_decoder.next().await {
                    // Bytes after the message: a second, maybe partial, one.
                    Ok(Some(_)) if !frame_decoder.buf.is_empty() => {
                        Some(Err(extra_response_message()))
                    }
                    Ok(Some(data)) => Some(decoder.decode(&data).map_err(Status::from_std_error)),
                    Ok(None) => None,
                    Err(status) => Some(Err(status)),
                };
                continue;
            }
            Err(frame) => frame,
        };

        if let Ok(trailers) = frame.into_trailers() {
            let status = Status::from_headers(&trailers)?.ok_or_else(missing_grpc_status)?;
            if !status.is_ok() {
                return Err(status);
            }
            return match result {
                Some(result) => result,
                None => {
                    frame_decoder.check_incomplete()?;
                    Err(missing_response_message())
                }
            };
        }
    }

    match result {
        Some(Err(status)) => Err(status),
        _ => Err(Status::new(Code::Internal).with_message("missing trailers")),
    }
}

// A wrong number of messages is `Internal`, as in grpc-go and grpc-java:
// `doc/statuscodes.md` says `Unimplemented`, which gRPC agreed to change
// (https://github.com/grpc/grpc-java/issues/11247), since the server may have
// handled the call already. A missing or invalid `grpc-status` is `Unknown`,
// as specified in <https://github.com/grpc/grpc/blob/master/doc/statuscodes.md>
// and <https://github.com/grpc/grpc/blob/master/doc/http-grpc-status-mapping.md>.

fn missing_response_message() -> Status {
    Status::new(Code::Internal).with_message("missing response message")
}

fn extra_response_message() -> Status {
    Status::new(Code::Internal).with_message("expected only one response message")
}

fn missing_grpc_status() -> Status {
    Status::new(Code::Unknown).with_message("missing grpc-status")
}

pub(crate) fn create_decode_response_body<T: Decoder>(
    mut decoder: T,
    headers: &HeaderMap,
    body: Body,
    compression: Option<CompressionEncoding>,
    max_message_size: usize,
) -> Result<Streaming<T::Item>, Status> {
    // check is trailers-only
    if let Some(status) = Status::from_headers(headers)? {
        return if status.is_ok() {
            Ok(Streaming::new(futures_util::stream::empty()))
        } else {
            Err(status)
        };
    }

    let mut body: BoxBody = body.into();

    Ok(Streaming::new(async_stream::try_stream! {
        let mut frame_decoder = DataFrameDecoder::new(compression, max_message_size);
        let mut status = None;

        while let Some(frame) = body.frame().await.transpose().map_err(Status::from_std_error)? {
            if frame.is_data() {
                let data = frame.into_data().unwrap();
                frame_decoder.put_slice(data);
                while let Some(data) = frame_decoder.next().await? {
                    let message = decoder.decode(&data).map_err(Status::from_std_error)?;
                    yield message;
                }
            } else if frame.is_trailers() {
                frame_decoder.check_incomplete()?;
                let headers = frame.into_trailers().unwrap();
                status = Some(Status::from_headers(&headers)?
                    .ok_or_else(missing_grpc_status)?);
                break;
            }
        }

        let status = status.ok_or_else(|| Status::new(Code::Internal).with_message("missing trailers"))?;
        if !status.is_ok() {
            Err(status)?;
        }
    }))
}

#[cfg(test)]
mod tests {
    use std::io::Result as IoResult;

    use futures_util::TryStreamExt;
    use http::HeaderMap;
    use prost::Message;

    use super::*;
    use crate::codec::{Codec, ProstCodec};

    #[derive(Clone, PartialEq, Message)]
    struct TestMsg {
        #[prost(string, tag = 1)]
        value: String,
    }

    #[tokio::test]
    async fn msg_data_spans_multiple_frames() {
        // Split and encoded message into multiple frames.
        let msg = TestMsg {
            value:
                "A program is like a poem, you cannot write a poem without writing it. --- Dijkstra"
                    .into(),
        };
        let encoded = msg.encode_to_vec();
        let len = encoded.len();

        // Compression flag + u32 big endian size
        let mut buffer = vec![0];
        buffer.extend((len as u32).to_be_bytes());
        buffer.extend(encoded);

        // Split the data into multiple frames.
        let (first_frame, second_frame) = buffer.split_at(len / 2);

        let bytes_stream = futures_util::stream::iter(vec![
            Ok::<_, std::io::Error>(Bytes::from(first_frame.to_vec())),
            Ok(Bytes::from(second_frame.to_vec())),
        ]);
        let body = Body::from_bytes_stream(bytes_stream);

        let mut codec = ProstCodec::<TestMsg, TestMsg>::default();
        let mut streaming = create_decode_response_body(
            codec.decoder(),
            &HeaderMap::default(),
            body,
            None,
            usize::MAX,
        )
        .expect("streaming");

        let stream_msg = streaming
            .try_next()
            .await
            .expect("msg")
            .expect("one message");

        assert_eq!(msg, stream_msg);
    }

    /// Encodes an `i32` as 4 big-endian bytes, and fails on a negative one
    /// after writing part of it.
    struct I32Encoder;

    impl Encoder for I32Encoder {
        type Item = i32;

        fn encode(&mut self, message: i32, buf: &mut BytesMut) -> IoResult<()> {
            if message < 0 {
                buf.put_slice(&[0xff, 0xff]);
                return Err(IoError::other("negative"));
            }
            buf.put_i32(message);
            Ok(())
        }
    }

    fn i32_frame(value: i32) -> Bytes {
        let mut buf = BytesMut::new();
        buf.put_slice(&[0, 0, 0, 0, 4]);
        buf.put_i32(value);
        buf.freeze()
    }

    async fn frames(body: Body) -> Vec<IoResult<Frame<Bytes>>> {
        let mut body: BoxBody = body.into();
        let mut frames = vec![];
        while let Some(frame) = body.frame().await {
            frames.push(frame);
        }
        frames
    }

    /// A message that fails to encode leaves nothing of itself in the buffer
    /// shared with the next one.
    #[tokio::test]
    async fn failed_encode_leaves_no_bytes() {
        let mut buf = BytesMut::new();
        assert!(
            encode_data_frame(&mut I32Encoder, &mut buf, -1, None, usize::MAX)
                .await
                .is_err()
        );
        assert!(buf.is_empty());
        let frame = encode_data_frame(&mut I32Encoder, &mut buf, 2, None, usize::MAX).await;
        assert_eq!(frame.unwrap(), i32_frame(2));
    }

    /// Same when the message was to be compressed.
    #[cfg(feature = "gzip")]
    #[tokio::test]
    async fn failed_compressed_encode_leaves_no_bytes() {
        let gzip = Some(CompressionEncoding::GZIP);
        let mut buf = BytesMut::new();
        assert!(
            encode_data_frame(&mut I32Encoder, &mut buf, -1, gzip, usize::MAX)
                .await
                .is_err()
        );
        assert!(buf.is_empty());
        let frame = encode_data_frame(&mut I32Encoder, &mut buf, 2, gzip, usize::MAX)
            .await
            .unwrap();
        assert_eq!(frame[0], 1);
        assert_eq!(
            u32::from_be_bytes(frame[1..5].try_into().unwrap()) as usize,
            frame.len() - 5
        );
        let data = CompressionEncoding::GZIP
            .decode(&frame[5..], usize::MAX)
            .await
            .unwrap();
        assert_eq!(data.unwrap(), 2i32.to_be_bytes());
    }

    fn grpc_status(frame: &IoResult<Frame<Bytes>>) -> (&str, Option<&str>) {
        let trailers = frame.as_ref().unwrap().trailers_ref().expect("trailers");
        (
            trailers["grpc-status"].to_str().unwrap(),
            trailers
                .get("grpc-message")
                .map(|value| value.to_str().unwrap()),
        )
    }

    /// A response ends with trailers carrying an OK status.
    #[tokio::test]
    async fn response_ends_with_ok_trailers() {
        let stream = Streaming::new(futures_util::stream::iter([Ok(1), Ok(2)]));
        let frames = frames(create_encode_response_body(
            I32Encoder,
            stream,
            None,
            usize::MAX,
        ))
        .await;
        assert_eq!(frames.len(), 3);
        assert_eq!(frames[0].as_ref().unwrap().data_ref(), Some(&i32_frame(1)));
        assert_eq!(frames[1].as_ref().unwrap().data_ref(), Some(&i32_frame(2)));
        assert_eq!(grpc_status(&frames[2]), ("0", None));
    }

    /// A message that fails to encode ends a response with that error in the
    /// trailers, and nothing after them.
    #[tokio::test]
    async fn failed_encode_ends_response_with_status() {
        let stream = Streaming::new(futures_util::stream::iter([Ok(1), Ok(-1), Ok(2)]));
        let frames = frames(create_encode_response_body(
            I32Encoder,
            stream,
            None,
            usize::MAX,
        ))
        .await;
        assert_eq!(frames.len(), 2);
        assert_eq!(frames[0].as_ref().unwrap().data_ref(), Some(&i32_frame(1)));
        assert_eq!(grpc_status(&frames[1]), ("13", Some("negative")));
    }

    /// An error from the response stream ends a response with it in the
    /// trailers: the stream isn't polled again, and no second trailers follow.
    #[tokio::test]
    async fn stream_error_ends_response_with_status() {
        let error = Status::new(Code::NotFound).with_message("gone");
        let polled_after_error = Arc::new(Mutex::new(false));
        let stream = Streaming::new(futures_util::stream::iter([Ok(1), Err(error)]).chain(
            futures_util::stream::poll_fn({
                let polled_after_error = polled_after_error.clone();
                move |_| {
                    *polled_after_error.lock().unwrap() = true;
                    std::task::Poll::Ready(None)
                }
            }),
        ));
        let frames = frames(create_encode_response_body(
            I32Encoder,
            stream,
            None,
            usize::MAX,
        ))
        .await;
        assert_eq!(frames.len(), 2);
        assert_eq!(frames[0].as_ref().unwrap().data_ref(), Some(&i32_frame(1)));
        assert_eq!(grpc_status(&frames[1]), ("5", Some("gone")));
        assert!(!*polled_after_error.lock().unwrap());
    }

    /// A message that fails to encode ends a streaming request with an error.
    #[tokio::test]
    async fn failed_encode_ends_request_with_error() {
        let stream = Streaming::new(futures_util::stream::iter([Ok(1), Ok(-1), Ok(2)]));
        let error = RequestError::default();
        let frames = frames(create_encode_request_body(
            I32Encoder,
            stream,
            None,
            usize::MAX,
            error.clone(),
        ))
        .await;
        assert_eq!(frames.len(), 2);
        assert_eq!(frames[0].as_ref().unwrap().data_ref(), Some(&i32_frame(1)));
        assert!(frames[1].is_err());
        assert_eq!(error.get().unwrap().code(), Code::Internal);
    }

    fn assert_resource_exhausted<T: std::fmt::Debug>(res: Result<T, Status>, message: &str) {
        let status = res.unwrap_err();
        assert_eq!(status.code(), Code::ResourceExhausted);
        assert_eq!(status.message(), Some(message));
    }

    /// A message of exactly the limit is sent, a larger one is not, and
    /// leaves nothing in the buffer shared with the next one.
    #[tokio::test]
    async fn encoded_message_size_limit() {
        let mut buf = BytesMut::new();
        let frame = encode_data_frame(&mut I32Encoder, &mut buf, 1, None, 4).await;
        assert_eq!(frame.unwrap(), i32_frame(1));

        let res = encode_data_frame(&mut I32Encoder, &mut buf, 1, None, 3).await;
        assert_resource_exhausted(res, "trying to send message larger than max (4 vs. 3)");
        assert!(buf.is_empty());

        let res = encode_uncompressed_data_frame(&mut I32Encoder, &mut buf, 1, 3);
        assert_resource_exhausted(res, "trying to send message larger than max (4 vs. 3)");
        assert!(buf.is_empty());
    }

    /// A message over the limit ends a response with `ResourceExhausted`.
    #[tokio::test]
    async fn oversized_message_ends_response_with_status() {
        let stream = Streaming::new(futures_util::stream::iter([Ok(1), Ok(2)]));
        let frames = frames(create_encode_response_body(I32Encoder, stream, None, 3)).await;
        assert_eq!(frames.len(), 1);
        let trailers = frames[0].as_ref().unwrap().trailers_ref().unwrap();
        assert_resource_exhausted::<()>(
            Err(Status::from_headers(trailers).unwrap().unwrap()),
            "trying to send message larger than max (4 vs. 3)",
        );
    }

    /// A message over the limit ends a streaming request with an error.
    #[tokio::test]
    async fn oversized_message_ends_request_with_error() {
        let stream = Streaming::new(futures_util::stream::iter([Ok(1), Ok(2)]));
        let error = RequestError::default();
        let frames = frames(create_encode_request_body(
            I32Encoder,
            stream,
            None,
            3,
            error.clone(),
        ))
        .await;
        assert_eq!(frames.len(), 1);
        assert!(frames[0].is_err());
        assert_eq!(error.get().unwrap().code(), Code::ResourceExhausted);
    }

    /// A message of exactly the limit is received.
    #[tokio::test]
    async fn decoded_message_at_size_limit() {
        let mut decoder = DataFrameDecoder::new(None, 4);
        decoder.put_slice(i32_frame(1));
        assert_eq!(
            decoder.next().await.unwrap().unwrap(),
            &1i32.to_be_bytes()[..]
        );
        assert!(decoder.check_incomplete().is_ok());
    }

    /// A message over the limit is rejected from its length prefix alone,
    /// before any of it is buffered.
    #[tokio::test]
    async fn decoded_message_over_size_limit() {
        let mut decoder = DataFrameDecoder::new(None, 3);
        decoder.put_slice([0, 0, 0, 0]);
        assert!(decoder.next().await.unwrap().is_none());
        decoder.put_slice([4]);
        assert_resource_exhausted(
            decoder.next().await,
            "received message larger than max (4 vs. 3)",
        );

        // The largest length a prefix can announce.
        let mut decoder = DataFrameDecoder::new(None, DEFAULT_MAX_DECODING_MESSAGE_SIZE);
        decoder.put_slice([0, 0xff, 0xff, 0xff, 0xff]);
        assert_resource_exhausted(
            decoder.next().await,
            "received message larger than max (4294967295 vs. 4194304)",
        );
    }

    /// Without a limit, the largest length a prefix can announce is waited
    /// for, also where `usize` is 32 bits.
    #[tokio::test]
    async fn decoded_message_without_size_limit() {
        let mut decoder = DataFrameDecoder::new(None, usize::MAX);
        decoder.put_slice([0, 0xff, 0xff, 0xff, 0xff, 1, 2, 3]);
        assert!(decoder.next().await.unwrap().is_none());
    }

    /// An oversized message ends the stream of decoded requests and of
    /// decoded responses with `ResourceExhausted`, after the messages before
    /// it.
    #[tokio::test]
    async fn oversized_message_ends_decoded_streams() {
        fn body() -> Body {
            let mut data = BytesMut::new();
            data.extend(i32_frame(1));
            data.put_slice(&[0, 0, 0, 0, 5, 1, 2, 3, 4, 5]);
            data.extend(i32_frame(2));
            Body::from_bytes(data.freeze())
        }

        struct I32Decoder;

        impl Decoder for I32Decoder {
            type Item = i32;

            fn decode(&mut self, mut buf: &[u8]) -> IoResult<i32> {
                Ok(buf.get_i32())
            }
        }

        let mut requests = create_decode_request_body(I32Decoder, body(), None, 4);
        assert_eq!(requests.try_next().await.unwrap(), Some(1));
        assert_resource_exhausted(
            requests.try_next().await,
            "received message larger than max (5 vs. 4)",
        );
        assert!(requests.next().await.is_none());

        let mut responses =
            create_decode_response_body(I32Decoder, &HeaderMap::default(), body(), None, 4)
                .unwrap();
        assert_eq!(responses.try_next().await.unwrap(), Some(1));
        assert_resource_exhausted(
            responses.try_next().await,
            "received message larger than max (5 vs. 4)",
        );
        assert!(responses.next().await.is_none());

        let res =
            decode_single_response_message(I32Decoder, &HeaderMap::default(), body(), None, 3)
                .await;
        assert_resource_exhausted(res, "received message larger than max (4 vs. 3)");
    }

    #[cfg(feature = "gzip")]
    mod compressed {
        use super::*;

        /// Encodes and decodes a message as its bytes.
        struct BytesCodec;

        impl Encoder for BytesCodec {
            type Item = Vec<u8>;

            fn encode(&mut self, message: Vec<u8>, buf: &mut BytesMut) -> IoResult<()> {
                buf.put_slice(&message);
                Ok(())
            }
        }

        const GZIP: Option<CompressionEncoding> = Some(CompressionEncoding::GZIP);

        /// 1 MiB of zeros, which gzip compresses to about 1 KiB.
        fn zeros() -> Vec<u8> {
            vec![0; 1024 * 1024]
        }

        async fn compressed_frame(message: Vec<u8>) -> Bytes {
            let mut buf = BytesMut::new();
            encode_data_frame(&mut BytesCodec, &mut buf, message, GZIP, usize::MAX)
                .await
                .unwrap()
        }

        /// The limit on a sent message applies to its compressed bytes.
        #[tokio::test]
        async fn encoded_size_limit_is_after_compression() {
            let frame = compressed_frame(zeros()).await;
            let compressed_len = frame.len() - 5;
            assert!(compressed_len < 4096, "{compressed_len}");

            let mut buf = BytesMut::new();
            let res =
                encode_data_frame(&mut BytesCodec, &mut buf, zeros(), GZIP, compressed_len).await;
            assert_eq!(res.unwrap(), frame);

            let res =
                encode_data_frame(&mut BytesCodec, &mut buf, zeros(), GZIP, compressed_len - 1)
                    .await;
            assert_eq!(res.unwrap_err().code(), Code::ResourceExhausted);
            assert!(buf.is_empty());
        }

        /// A small compressed message that decompresses to more than the
        /// limit is rejected; one that decompresses to exactly the limit is
        /// not.
        #[tokio::test]
        async fn decoded_size_limit_is_after_decompression() {
            let frame = compressed_frame(zeros()).await;

            let mut decoder = DataFrameDecoder::new(GZIP, 1024 * 1024);
            decoder.put_slice(&frame);
            assert_eq!(decoder.next().await.unwrap().unwrap(), zeros());

            let mut decoder = DataFrameDecoder::new(GZIP, 1024 * 1024 - 1);
            decoder.put_slice(&frame);
            assert_resource_exhausted(
                decoder.next().await,
                "received message after decompression larger than max (1048575)",
            );
        }

        /// Decompression stops one byte past the limit, instead of producing
        /// the whole message before measuring it.
        #[tokio::test]
        async fn decompression_stops_at_size_limit() {
            let frame = compressed_frame(zeros()).await;
            let data = &frame[5..];
            let gzip = CompressionEncoding::GZIP;
            assert_eq!(gzip.decode(data, usize::MAX).await.unwrap(), Some(zeros()));
            assert_eq!(gzip.decode(data, 1024 * 1024).await.unwrap(), Some(zeros()));
            assert_eq!(gzip.decode(data, 100).await.unwrap(), None);
            assert_eq!(gzip.decode(data, 0).await.unwrap(), None);
            // What rejecting it at a limit of 100 bytes produced.
            assert_eq!(gzip.decode_up_to(data, 101).await.unwrap(), [0; 101]);
        }
    }
}
