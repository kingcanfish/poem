use futures_util::StreamExt;
use http::HeaderValue;
use poem::{Body, Request, Response};
use tokio::time::Instant;

use crate::{
    Code, CompressionEncoding, Metadata, MethodType, Request as GrpcRequest,
    Response as GrpcResponse, Status, Streaming,
    codec::Codec,
    compression::{accept_encoding_header, get_request_encoding, select_response_encoding},
    encoding::{
        DEFAULT_MAX_DECODING_MESSAGE_SIZE, DEFAULT_MAX_ENCODING_MESSAGE_SIZE,
        create_decode_request_body, create_encode_response_body,
    },
    service::{
        BidirectionalStreamingService, ClientStreamingService, ServerStreamingService, UnaryService,
    },
    timeout::{deadline_from_headers, stream_with_deadline, with_deadline},
};

/// Tags the request with the [`MethodType`] of the method it is routed to.
/// `methods` holds paths relative to the service, i.e. `/{method}`.
#[doc(hidden)]
pub fn tag_method_type(req: &mut Request, methods: &[(&str, MethodType)]) {
    let path = req.uri().path();
    let method_type = methods
        .iter()
        .find(|(method_path, _)| *method_path == path)
        .map(|(_, method_type)| *method_type);
    crate::method_type::set_method_type(req, method_type);
}

#[doc(hidden)]
pub struct GrpcServer<'a, T> {
    codec: T,
    send_compressed: Option<CompressionEncoding>,
    accept_compressed: &'a [CompressionEncoding],
    max_decoding_message_size: usize,
    max_encoding_message_size: usize,
}

impl<'a, T: Codec> GrpcServer<'a, T> {
    #[inline]
    pub fn new(
        codec: T,
        send_compressed: Option<CompressionEncoding>,
        accept_compressed: &'a [CompressionEncoding],
    ) -> Self {
        Self {
            codec,
            send_compressed,
            accept_compressed,
            max_decoding_message_size: DEFAULT_MAX_DECODING_MESSAGE_SIZE,
            max_encoding_message_size: DEFAULT_MAX_ENCODING_MESSAGE_SIZE,
        }
    }

    /// Limits the size of a request message, `None` being the default of
    /// 4 MiB.
    #[inline]
    pub fn max_decoding_message_size(mut self, limit: Option<usize>) -> Self {
        self.max_decoding_message_size = limit.unwrap_or(DEFAULT_MAX_DECODING_MESSAGE_SIZE);
        self
    }

    /// Limits the size of a response message, `None` being the default of no
    /// limit.
    #[inline]
    pub fn max_encoding_message_size(mut self, limit: Option<usize>) -> Self {
        self.max_encoding_message_size = limit.unwrap_or(DEFAULT_MAX_ENCODING_MESSAGE_SIZE);
        self
    }

    /// Creates a response telling the client which encodings its requests
    /// can use, which the gRPC spec requires of one rejecting a request for
    /// its encoding.
    fn create_response(&self) -> Response {
        let mut resp = Response::default().set_content_type(T::CONTENT_TYPES[0]);
        resp.headers_mut().insert(
            "grpc-accept-encoding",
            accept_encoding_header(self.accept_compressed),
        );
        resp
    }

    /// Reads what the request's headers say about the call: the encoding of
    /// its messages, and the deadline its `grpc-timeout` gives it, counted
    /// from now. Keeps the configured encoding for the response's messages
    /// only if the client accepts it.
    fn read_call_headers(
        &mut self,
        headers: &http::HeaderMap,
    ) -> Result<(Option<CompressionEncoding>, Option<Instant>), Status> {
        let deadline = deadline_from_headers(headers)?;
        let incoming_encoding = get_request_encoding(headers, self.accept_compressed)?;
        self.send_compressed =
            select_response_encoding(headers, self.send_compressed, incoming_encoding);
        Ok((incoming_encoding, deadline))
    }

    pub async fn unary<S>(mut self, service: S, request: Request) -> Response
    where
        S: UnaryService<T::Decode, Response = T::Encode>,
    {
        let (parts, body) = request.into_parts();
        let mut resp = self.create_response();
        let (incoming_encoding, deadline) = match self.read_call_headers(&parts.headers) {
            Ok(call_headers) => call_headers,
            Err(status) => {
                resp.headers_mut().extend(status.to_headers());
                return resp;
            }
        };
        let mut stream = create_decode_request_body(
            self.codec.decoder(),
            body,
            incoming_encoding,
            self.max_decoding_message_size,
        );

        let res = with_deadline(deadline, async {
            let message = read_single_message(&mut stream).await?;
            service
                .call(GrpcRequest {
                    metadata: Metadata {
                        headers: parts.headers,
                    },
                    message,
                    extensions: parts.extensions,
                })
                .await
        })
        .await;

        match res {
            Ok(grpc_resp) => {
                let GrpcResponse { metadata, message } = grpc_resp;
                let body = create_encode_response_body(
                    self.codec.encoder(),
                    Streaming::new(futures_util::stream::once(async move { Ok(message) })),
                    self.send_compressed,
                    self.max_encoding_message_size,
                );
                update_http_response(
                    &mut resp,
                    metadata,
                    body,
                    self.send_compressed,
                    self.accept_compressed,
                );
            }
            Err(status) => resp.headers_mut().extend(status.to_headers()),
        }

        resp
    }

    pub async fn client_streaming<S>(mut self, service: S, request: Request) -> Response
    where
        S: ClientStreamingService<T::Decode, Response = T::Encode>,
    {
        let (parts, body) = request.into_parts();
        let mut resp = self.create_response();
        let (incoming_encoding, deadline) = match self.read_call_headers(&parts.headers) {
            Ok(call_headers) => call_headers,
            Err(status) => {
                resp.headers_mut().extend(status.to_headers());
                return resp;
            }
        };
        let stream = create_decode_request_body(
            self.codec.decoder(),
            body,
            incoming_encoding,
            self.max_decoding_message_size,
        );

        let res = with_deadline(
            deadline,
            service.call(GrpcRequest {
                metadata: Metadata {
                    headers: parts.headers,
                },
                extensions: parts.extensions,
                message: stream,
            }),
        )
        .await;

        match res {
            Ok(grpc_resp) => {
                let GrpcResponse { metadata, message } = grpc_resp;
                let body = create_encode_response_body(
                    self.codec.encoder(),
                    Streaming::new(futures_util::stream::once(async move { Ok(message) })),
                    self.send_compressed,
                    self.max_encoding_message_size,
                );
                update_http_response(
                    &mut resp,
                    metadata,
                    body,
                    self.send_compressed,
                    self.accept_compressed,
                );
            }
            Err(status) => {
                resp.headers_mut().extend(status.to_headers());
            }
        }

        resp
    }

    pub async fn server_streaming<S>(mut self, service: S, request: Request) -> Response
    where
        S: ServerStreamingService<T::Decode, Response = T::Encode>,
    {
        let (parts, body) = request.into_parts();
        let mut resp = self.create_response();
        let (incoming_encoding, deadline) = match self.read_call_headers(&parts.headers) {
            Ok(call_headers) => call_headers,
            Err(status) => {
                resp.headers_mut().extend(status.to_headers());
                return resp;
            }
        };
        let mut stream = create_decode_request_body(
            self.codec.decoder(),
            body,
            incoming_encoding,
            self.max_decoding_message_size,
        );

        let res = with_deadline(deadline, async {
            let message = read_single_message(&mut stream).await?;
            service
                .call(GrpcRequest {
                    metadata: Metadata {
                        headers: parts.headers,
                    },
                    message,
                    extensions: parts.extensions,
                })
                .await
        })
        .await;

        match res {
            Ok(grpc_resp) => {
                let GrpcResponse { metadata, message } = grpc_resp;
                let body = create_encode_response_body(
                    self.codec.encoder(),
                    stream_with_deadline(deadline, message),
                    self.send_compressed,
                    self.max_encoding_message_size,
                );
                update_http_response(
                    &mut resp,
                    metadata,
                    body,
                    self.send_compressed,
                    self.accept_compressed,
                );
            }
            Err(status) => {
                resp.headers_mut().extend(status.to_headers());
            }
        }

        resp
    }

    pub async fn bidirectional_streaming<S>(mut self, service: S, request: Request) -> Response
    where
        S: BidirectionalStreamingService<T::Decode, Response = T::Encode>,
    {
        let (parts, body) = request.into_parts();
        let mut resp = self.create_response();
        let (incoming_encoding, deadline) = match self.read_call_headers(&parts.headers) {
            Ok(call_headers) => call_headers,
            Err(status) => {
                resp.headers_mut().extend(status.to_headers());
                return resp;
            }
        };
        let stream = create_decode_request_body(
            self.codec.decoder(),
            body,
            incoming_encoding,
            self.max_decoding_message_size,
        );

        let res = with_deadline(
            deadline,
            service.call(GrpcRequest {
                metadata: Metadata {
                    headers: parts.headers,
                },
                message: stream,
                extensions: parts.extensions,
            }),
        )
        .await;

        match res {
            Ok(grpc_resp) => {
                let GrpcResponse { metadata, message } = grpc_resp;
                let body = create_encode_response_body(
                    self.codec.encoder(),
                    stream_with_deadline(deadline, message),
                    self.send_compressed,
                    self.max_encoding_message_size,
                );
                update_http_response(
                    &mut resp,
                    metadata,
                    body,
                    self.send_compressed,
                    self.accept_compressed,
                );
            }
            Err(status) => {
                resp.headers_mut().extend(status.to_headers());
            }
        }

        resp
    }
}

/// Reads the only message of a non-client-streaming request, up to the end of
/// the request stream: responding before it ends makes the client's final,
/// empty DATA frame arrive on a closed stream, which h2 counts as a reset and
/// closes the connection after too many of them.
async fn read_single_message<T>(stream: &mut Streaming<T>) -> Result<T, Status> {
    let message = stream
        .next()
        .await
        .ok_or_else(|| Status::new(Code::Internal).with_message("missing request message"))??;
    match stream.next().await {
        Some(Ok(_)) => {
            Err(Status::new(Code::Internal).with_message("expected only one request message"))
        }
        Some(Err(status)) => Err(status),
        None => Ok(message),
    }
}

fn update_http_response(
    resp: &mut Response,
    metadata: Metadata,
    body: Body,
    send_compressed: Option<CompressionEncoding>,
    accept_compressed: &[CompressionEncoding],
) {
    resp.headers_mut().extend(metadata.headers);
    // Not what the handler's metadata says, e.g. when it forwards another
    // server's response.
    resp.headers_mut().insert(
        "grpc-accept-encoding",
        accept_encoding_header(accept_compressed),
    );
    if let Some(send_compressed) = send_compressed {
        resp.headers_mut().insert(
            "grpc-encoding",
            HeaderValue::from_str(send_compressed.as_str()).expect("BUG: invalid encoding"),
        );
    }
    resp.set_body(body);
}

#[cfg(test)]
mod tests {
    use std::{
        io::{Error as IoError, Result as IoResult},
        sync::{
            Arc,
            atomic::{AtomicBool, Ordering},
        },
    };

    use bytes::{Buf, BufMut, BytesMut};
    use futures_util::TryStreamExt;
    use poem::{
        Server,
        listener::{Acceptor, Listener, TcpListener},
    };

    use super::*;
    use crate::{
        ClientConfig,
        client::GrpcClient,
        codec::{Decoder, Encoder},
    };

    /// Encodes an `i32` as 4 big-endian bytes, and fails on a negative one.
    #[derive(Default)]
    struct I32Codec;

    impl Codec for I32Codec {
        const CONTENT_TYPES: &'static [&'static str] = &["application/grpc"];

        type Encode = i32;
        type Decode = i32;
        type Encoder = I32Codec;
        type Decoder = I32Codec;

        fn encoder(&mut self) -> Self::Encoder {
            I32Codec
        }

        fn decoder(&mut self) -> Self::Decoder {
            I32Codec
        }
    }

    impl Encoder for I32Codec {
        type Item = i32;

        fn encode(&mut self, message: i32, buf: &mut BytesMut) -> IoResult<()> {
            if message < 0 {
                return Err(IoError::other("negative"));
            }
            buf.put_i32(message);
            Ok(())
        }
    }

    impl Decoder for I32Codec {
        type Item = i32;

        fn decode(&mut self, mut buf: &[u8]) -> IoResult<i32> {
            Ok(buf.get_i32())
        }
    }

    /// Responds to a unary call with `10 - request`, which fails to encode
    /// for a request above 10.
    struct Subtract;

    impl UnaryService<i32> for Subtract {
        type Response = i32;

        async fn call(&self, request: GrpcRequest<i32>) -> Result<GrpcResponse<i32>, Status> {
            Ok(GrpcResponse::new(10 - request.into_inner()))
        }
    }

    /// Responds to a server-streaming call with `items`, then sets `drained`
    /// once its response stream was polled to its end.
    #[derive(Clone)]
    struct Items {
        items: Vec<Result<i32, Status>>,
        drained: Arc<AtomicBool>,
    }

    impl ServerStreamingService<i32> for Items {
        type Response = i32;

        async fn call(
            &self,
            _request: GrpcRequest<i32>,
        ) -> Result<GrpcResponse<Streaming<i32>>, Status> {
            let drained = self.drained.clone();
            let end = futures_util::stream::poll_fn(move |_| {
                drained.store(true, Ordering::SeqCst);
                std::task::Poll::Ready(None)
            });
            Ok(GrpcResponse::new_streaming(
                futures_util::stream::iter(self.items.clone()).chain(end),
            ))
        }
    }

    /// Serves `Subtract` at `/unary` and `items` at `/stream` over HTTP/2, and
    /// returns a client for it and the flag set when `items` was read to its
    /// end.
    async fn serve(items: Vec<Result<i32, Status>>) -> (GrpcClient, Arc<AtomicBool>) {
        let drained = Arc::new(AtomicBool::new(false));
        let items = Items {
            items,
            drained: drained.clone(),
        };
        let app = poem::Route::new()
            .at(
                "/unary",
                poem::endpoint::make(|req| async move {
                    GrpcServer::new(I32Codec, None, &[])
                        .unary(Subtract, req)
                        .await
                }),
            )
            .at(
                "/stream",
                poem::endpoint::make(move |req| {
                    let items = items.clone();
                    async move {
                        GrpcServer::new(I32Codec, None, &[])
                            .server_streaming(items, req)
                            .await
                    }
                }),
            );
        let acceptor = TcpListener::bind("127.0.0.1:0")
            .into_acceptor()
            .await
            .unwrap();
        let addr = *acceptor.local_addr()[0].as_socket_addr().unwrap();
        tokio::spawn(Server::new_with_acceptor(acceptor).run(app));
        let cli = GrpcClient::new(
            ClientConfig::builder()
                .uri(format!("http://{addr}"))
                .build()
                .unwrap(),
        );
        (cli, drained)
    }

    /// Calls `/stream` and returns the messages received before the stream's
    /// end, and the status it ended with, if not OK.
    async fn stream(cli: &GrpcClient) -> (Vec<i32>, Option<Status>) {
        let mut stream = cli
            .server_streaming("/stream", I32Codec, GrpcRequest::new(0))
            .await
            .unwrap()
            .into_inner();
        let mut messages = vec![];
        loop {
            match stream.try_next().await {
                Ok(Some(message)) => messages.push(message),
                Ok(None) => return (messages, None),
                Err(status) => return (messages, Some(status)),
            }
        }
    }

    #[tokio::test]
    async fn responses_without_errors() {
        let (cli, drained) = serve(vec![Ok(1), Ok(2)]).await;
        let resp = cli.unary("/unary", I32Codec, GrpcRequest::new(3)).await;
        assert_eq!(resp.unwrap().into_inner(), 7);
        let (messages, status) = stream(&cli).await;
        assert_eq!(messages, [1, 2]);
        assert!(status.is_none());
        assert!(drained.load(Ordering::SeqCst));
    }

    /// A unary response that fails to encode fails the call with that error,
    /// instead of ending without a message and with an OK status.
    #[tokio::test]
    async fn unary_response_fails_to_encode() {
        let (cli, _) = serve(vec![]).await;
        let status = cli
            .unary("/unary", I32Codec, GrpcRequest::new(11))
            .await
            .unwrap_err();
        assert_eq!(status.code(), Code::Internal);
        assert_eq!(status.message(), Some("negative"));
    }

    /// A streamed message that fails to encode ends the response with that
    /// error, instead of being left out of an otherwise successful response.
    #[tokio::test]
    async fn streamed_response_fails_to_encode() {
        let (cli, drained) = serve(vec![Ok(1), Ok(-1), Ok(2)]).await;
        let (messages, status) = stream(&cli).await;
        assert_eq!(messages, [1]);
        let status = status.expect("the response ended with an OK status");
        assert_eq!(status.code(), Code::Internal);
        assert_eq!(status.message(), Some("negative"));
        assert!(!drained.load(Ordering::SeqCst));
    }

    /// An error from the handler's response stream reaches the client as the
    /// status of the response, after the messages before it. (This held
    /// before the encoder stopped at the error itself, since hyper sends
    /// nothing after the first trailers.)
    #[tokio::test]
    async fn streamed_response_error() {
        let error = Status::new(Code::NotFound).with_message("gone");
        let (cli, _) = serve(vec![Ok(1), Err(error), Ok(2)]).await;
        let (messages, status) = stream(&cli).await;
        assert_eq!(messages, [1]);
        let status = status.unwrap();
        assert_eq!(status.code(), Code::NotFound);
        assert_eq!(status.message(), Some("gone"));
    }

    /// Encodes and decodes a message as its bytes.
    #[derive(Default)]
    struct BytesCodec;

    impl Codec for BytesCodec {
        const CONTENT_TYPES: &'static [&'static str] = &["application/grpc"];

        type Encode = Vec<u8>;
        type Decode = Vec<u8>;
        type Encoder = BytesCodec;
        type Decoder = BytesCodec;

        fn encoder(&mut self) -> Self::Encoder {
            BytesCodec
        }

        fn decoder(&mut self) -> Self::Decoder {
            BytesCodec
        }
    }

    impl Encoder for BytesCodec {
        type Item = Vec<u8>;

        fn encode(&mut self, message: Vec<u8>, buf: &mut BytesMut) -> IoResult<()> {
            buf.put_slice(&message);
            Ok(())
        }
    }

    impl Decoder for BytesCodec {
        type Item = Vec<u8>;

        fn decode(&mut self, buf: &[u8]) -> IoResult<Vec<u8>> {
            Ok(buf.to_vec())
        }
    }

    fn len_message(len: usize) -> Vec<u8> {
        (len as u32).to_be_bytes().to_vec()
    }

    fn message_len(mut message: &[u8]) -> usize {
        message.get_u32() as usize
    }

    /// Responds with the total length of the request messages.
    struct Len;

    impl UnaryService<Vec<u8>> for Len {
        type Response = Vec<u8>;

        async fn call(
            &self,
            request: GrpcRequest<Vec<u8>>,
        ) -> Result<GrpcResponse<Vec<u8>>, Status> {
            Ok(GrpcResponse::new(len_message(request.into_inner().len())))
        }
    }

    impl ClientStreamingService<Vec<u8>> for Len {
        type Response = Vec<u8>;

        async fn call(
            &self,
            request: GrpcRequest<Streaming<Vec<u8>>>,
        ) -> Result<GrpcResponse<Vec<u8>>, Status> {
            let len = request
                .into_inner()
                .try_fold(0, |len, message| async move { Ok(len + message.len()) })
                .await?;
            Ok(GrpcResponse::new(len_message(len)))
        }
    }

    /// Responds with a message of as many bytes as the request asks for; when
    /// streaming, between two messages of one byte.
    struct Blob;

    impl UnaryService<Vec<u8>> for Blob {
        type Response = Vec<u8>;

        async fn call(
            &self,
            request: GrpcRequest<Vec<u8>>,
        ) -> Result<GrpcResponse<Vec<u8>>, Status> {
            Ok(GrpcResponse::new(vec![
                0;
                message_len(&request.into_inner())
            ]))
        }
    }

    impl ServerStreamingService<Vec<u8>> for Blob {
        type Response = Vec<u8>;

        async fn call(
            &self,
            request: GrpcRequest<Vec<u8>>,
        ) -> Result<GrpcResponse<Streaming<Vec<u8>>>, Status> {
            let len = message_len(&request.into_inner());
            Ok(GrpcResponse::new_streaming(futures_util::stream::iter([
                Ok(vec![0]),
                Ok(vec![0; len]),
                Ok(vec![0]),
            ])))
        }
    }

    const MIB: usize = 1024 * 1024;

    /// Serves `Len` and `Blob` over HTTP/2 with the given message size limits,
    /// and returns a client for them.
    async fn serve_bytes(decoding: Option<usize>, encoding: Option<usize>) -> GrpcClient {
        serve_compressed_bytes(decoding, encoding, None).await
    }

    /// Same, with a server that sends and accepts `compression`.
    async fn serve_compressed_bytes(
        decoding: Option<usize>,
        encoding: Option<usize>,
        compression: Option<CompressionEncoding>,
    ) -> GrpcClient {
        let server = move || {
            let accept: &'static [CompressionEncoding] = match compression {
                Some(compression) => vec![compression].leak(),
                None => &[],
            };
            GrpcServer::new(BytesCodec, compression, accept)
                .max_decoding_message_size(decoding)
                .max_encoding_message_size(encoding)
        };

        let app = poem::Route::new()
            .at(
                "/len",
                poem::endpoint::make(move |req| async move { server().unary(Len, req).await }),
            )
            .at(
                "/total_len",
                poem::endpoint::make(move |req| async move {
                    server().client_streaming(Len, req).await
                }),
            )
            .at(
                "/blob",
                poem::endpoint::make(move |req| async move { server().unary(Blob, req).await }),
            )
            .at(
                "/blobs",
                poem::endpoint::make(move |req| async move {
                    server().server_streaming(Blob, req).await
                }),
            );
        let acceptor = TcpListener::bind("127.0.0.1:0")
            .into_acceptor()
            .await
            .unwrap();
        let addr = *acceptor.local_addr()[0].as_socket_addr().unwrap();
        tokio::spawn(Server::new_with_acceptor(acceptor).run(app));
        GrpcClient::new(
            ClientConfig::builder()
                .uri(format!("http://{addr}"))
                .build()
                .unwrap(),
        )
    }

    /// Sends a message of `len` bytes, and returns the length the server
    /// received.
    async fn len(cli: &GrpcClient, len: usize) -> Result<usize, Status> {
        let resp = cli
            .unary("/len", BytesCodec, GrpcRequest::new(vec![0; len]))
            .await?;
        Ok(message_len(&resp.into_inner()))
    }

    /// Sends messages of `lens` bytes, and returns the total length the
    /// server received.
    async fn total_len(cli: &GrpcClient, lens: &[usize]) -> Result<usize, Status> {
        let messages: Vec<_> = lens.iter().map(|len| Ok(vec![0; *len])).collect();
        let request = GrpcRequest::new_streaming(futures_util::stream::iter(messages));
        let resp = cli
            .client_streaming("/total_len", BytesCodec, request)
            .await?;
        Ok(message_len(&resp.into_inner()))
    }

    /// Asks for a message of `len` bytes, and returns the length received.
    async fn blob(cli: &GrpcClient, len: usize) -> Result<usize, Status> {
        let resp = cli
            .unary("/blob", BytesCodec, GrpcRequest::new(len_message(len)))
            .await?;
        Ok(resp.into_inner().len())
    }

    /// Asks for a message of `len` bytes between two of one byte, and returns
    /// the lengths received before the stream's end, and the status it ended
    /// with, if not OK.
    async fn blobs(cli: &GrpcClient, len: usize) -> (Vec<usize>, Option<Status>) {
        let mut stream = cli
            .server_streaming("/blobs", BytesCodec, GrpcRequest::new(len_message(len)))
            .await
            .unwrap()
            .into_inner();
        let mut lens = vec![];
        loop {
            match stream.try_next().await {
                Ok(Some(message)) => lens.push(message.len()),
                Ok(None) => return (lens, None),
                Err(status) => return (lens, Some(status)),
            }
        }
    }

    fn assert_resource_exhausted(status: Status, message: &str) {
        assert_eq!(status.code(), Code::ResourceExhausted);
        assert_eq!(status.message(), Some(message));
    }

    /// By default the server accepts a request message of up to 4 MiB, and
    /// rejects a larger one without breaking the connection.
    #[tokio::test]
    async fn default_request_size_limit() {
        let cli = serve_bytes(None, None).await;
        assert_eq!(len(&cli, 4 * MIB).await.unwrap(), 4 * MIB);
        assert_resource_exhausted(
            len(&cli, 4 * MIB + 1).await.unwrap_err(),
            "received message larger than max (4194305 vs. 4194304)",
        );
        assert_resource_exhausted(
            total_len(&cli, &[1, 4 * MIB + 1, 1]).await.unwrap_err(),
            "received message larger than max (4194305 vs. 4194304)",
        );
        assert_eq!(len(&cli, 4 * MIB).await.unwrap(), 4 * MIB);
        assert_eq!(total_len(&cli, &[4 * MIB, 4 * MIB]).await.unwrap(), 8 * MIB);
    }

    /// The server's limit on request messages can be lowered and raised.
    #[tokio::test]
    async fn request_size_limit() {
        let cli = serve_bytes(Some(16), None).await;
        assert_eq!(len(&cli, 16).await.unwrap(), 16);
        assert_resource_exhausted(
            len(&cli, 17).await.unwrap_err(),
            "received message larger than max (17 vs. 16)",
        );
        assert_eq!(total_len(&cli, &[16, 16, 16]).await.unwrap(), 48);
        assert_resource_exhausted(
            total_len(&cli, &[16, 17, 16]).await.unwrap_err(),
            "received message larger than max (17 vs. 16)",
        );

        let cli = serve_bytes(Some(6 * MIB), None).await;
        assert_eq!(len(&cli, 6 * MIB).await.unwrap(), 6 * MIB);
        assert_resource_exhausted(
            len(&cli, 6 * MIB + 1).await.unwrap_err(),
            "received message larger than max (6291457 vs. 6291456)",
        );

        // Only an empty message fits a limit of 0.
        let cli = serve_bytes(Some(0), None).await;
        assert_eq!(len(&cli, 0).await.unwrap(), 0);
        assert_resource_exhausted(
            len(&cli, 1).await.unwrap_err(),
            "received message larger than max (1 vs. 0)",
        );

        let cli = serve_bytes(Some(usize::MAX), None).await;
        assert_eq!(len(&cli, 6 * MIB).await.unwrap(), 6 * MIB);
    }

    /// By default the server's response messages are not limited, and a
    /// limit ends a response with `ResourceExhausted`.
    #[tokio::test]
    async fn response_size_limit() {
        let mut cli = serve_bytes(None, None).await;
        cli.set_max_decoding_message_size(usize::MAX);
        assert_eq!(blob(&cli, 6 * MIB).await.unwrap(), 6 * MIB);

        let cli = serve_bytes(None, Some(16)).await;
        assert_eq!(blob(&cli, 16).await.unwrap(), 16);
        assert_resource_exhausted(
            blob(&cli, 17).await.unwrap_err(),
            "trying to send message larger than max (17 vs. 16)",
        );
        let (lens, status) = blobs(&cli, 16).await;
        assert_eq!(lens, [1, 16, 1]);
        assert!(status.is_none());
        let (lens, status) = blobs(&cli, 17).await;
        assert_eq!(lens, [1]);
        assert_resource_exhausted(
            status.unwrap(),
            "trying to send message larger than max (17 vs. 16)",
        );
    }

    /// By default the client accepts a response message of up to 4 MiB, and
    /// rejects a larger one without breaking the connection.
    #[tokio::test]
    async fn default_client_response_size_limit() {
        let cli = serve_bytes(None, None).await;
        assert_eq!(blob(&cli, 4 * MIB).await.unwrap(), 4 * MIB);
        assert_resource_exhausted(
            blob(&cli, 4 * MIB + 1).await.unwrap_err(),
            "received message larger than max (4194305 vs. 4194304)",
        );
        let (lens, status) = blobs(&cli, 4 * MIB + 1).await;
        assert_eq!(lens, [1]);
        assert_resource_exhausted(
            status.unwrap(),
            "received message larger than max (4194305 vs. 4194304)",
        );
        assert_eq!(blob(&cli, 4 * MIB).await.unwrap(), 4 * MIB);
        let (lens, status) = blobs(&cli, 4 * MIB).await;
        assert_eq!(lens, [1, 4 * MIB, 1]);
        assert!(status.is_none());
    }

    /// The client's limits on response and request messages can be set.
    #[tokio::test]
    async fn client_size_limits() {
        let mut cli = serve_bytes(None, None).await;
        cli.set_max_decoding_message_size(16);
        assert_eq!(blob(&cli, 16).await.unwrap(), 16);
        assert_resource_exhausted(
            blob(&cli, 17).await.unwrap_err(),
            "received message larger than max (17 vs. 16)",
        );
        let (lens, status) = blobs(&cli, 17).await;
        assert_eq!(lens, [1]);
        assert_resource_exhausted(
            status.unwrap(),
            "received message larger than max (17 vs. 16)",
        );

        cli.set_max_encoding_message_size(16);
        assert_eq!(len(&cli, 16).await.unwrap(), 16);
        assert_resource_exhausted(
            len(&cli, 17).await.unwrap_err(),
            "trying to send message larger than max (17 vs. 16)",
        );
        assert_eq!(total_len(&cli, &[16, 16]).await.unwrap(), 32);
        assert_resource_exhausted(
            total_len(&cli, &[16, 17, 16]).await.unwrap_err(),
            "trying to send message larger than max (17 vs. 16)",
        );
    }

    /// Over a connection too, compressed messages are limited by their size
    /// once decompressed, in both directions, and by their compressed size
    /// when sent.
    #[cfg(feature = "gzip")]
    #[tokio::test]
    async fn compressed_message_size_limits() {
        let gzip = CompressionEncoding::GZIP;
        let mut cli = serve_compressed_bytes(None, None, Some(gzip)).await;
        cli.set_send_compressed(gzip);
        cli.set_accept_compressed([gzip]);

        // 8 MiB of zeros are a few KiB once compressed.
        assert_eq!(len(&cli, 4 * MIB).await.unwrap(), 4 * MIB);
        assert_resource_exhausted(
            len(&cli, 8 * MIB).await.unwrap_err(),
            "received message after decompression larger than max (4194304)",
        );
        assert_eq!(blob(&cli, 4 * MIB).await.unwrap(), 4 * MIB);
        assert_resource_exhausted(
            blob(&cli, 8 * MIB).await.unwrap_err(),
            "received message after decompression larger than max (4194304)",
        );
        let (lens, status) = blobs(&cli, 8 * MIB).await;
        assert_eq!(lens, [1]);
        assert_resource_exhausted(
            status.unwrap(),
            "received message after decompression larger than max (4194304)",
        );
        assert_eq!(len(&cli, 4 * MIB).await.unwrap(), 4 * MIB);

        // 1 MiB of zeros is about 1 KiB once compressed.
        cli.set_max_encoding_message_size(4096);
        assert_eq!(len(&cli, MIB).await.unwrap(), MIB);
        cli.set_max_encoding_message_size(64);
        assert_eq!(
            len(&cli, MIB).await.unwrap_err().code(),
            Code::ResourceExhausted
        );
    }

    #[cfg(feature = "gzip")]
    mod compression {
        use bytes::Bytes;
        use http::HeaderMap;
        use http_body_util::BodyExt;

        use super::*;
        use crate::client::BoxBody;

        const GZIP: CompressionEncoding = CompressionEncoding::GZIP;

        /// A response's headers, data and trailers.
        struct RawResponse {
            headers: HeaderMap,
            data: Vec<u8>,
            trailers: HeaderMap,
        }

        impl RawResponse {
            /// The `grpc-status`, from the trailers or a Trailers-Only
            /// response's headers.
            fn status(&self) -> &str {
                self.trailers
                    .get("grpc-status")
                    .or_else(|| self.headers.get("grpc-status"))
                    .expect("grpc-status")
                    .to_str()
                    .unwrap()
            }

            fn header(&self, name: &str) -> Option<&str> {
                self.headers.get(name).map(|value| value.to_str().unwrap())
            }
        }

        /// Calls `Subtract` on a server sending `send` and accepting `accept`
        /// with a request of these `headers` and `body`.
        async fn call(
            send: Option<CompressionEncoding>,
            accept: &[CompressionEncoding],
            headers: &[(&str, &str)],
            body: impl Into<Bytes>,
        ) -> RawResponse {
            let mut req = Request::builder().content_type("application/grpc");
            for (name, value) in headers {
                req = req.header(*name, *value);
            }
            let resp = GrpcServer::new(I32Codec, send, accept)
                .unary(Subtract, req.body(body.into()))
                .await;
            let (parts, body) = resp.into_parts();
            let mut body: BoxBody = body.into();
            let mut data = vec![];
            let mut trailers = HeaderMap::new();
            while let Some(frame) = body.frame().await {
                match frame.unwrap().into_data() {
                    Ok(bytes) => data.extend(bytes),
                    Err(frame) => trailers = frame.into_trailers().unwrap(),
                }
            }
            RawResponse {
                headers: parts.headers,
                data,
                trailers,
            }
        }

        /// The request message `3`, uncompressed.
        const THREE: [u8; 9] = [0, 0, 0, 0, 4, 0, 0, 0, 3];

        /// The request message `3`, compressed with `encoding`.
        async fn compressed_three(encoding: CompressionEncoding) -> Vec<u8> {
            let data = encoding.encode(&3i32.to_be_bytes()).await.unwrap();
            let mut message = vec![1];
            message.extend((data.len() as u32).to_be_bytes());
            message.extend(data);
            message
        }

        /// Checks that `resp` carries the message `7`, compressed or not.
        async fn assert_seven(resp: &RawResponse, compressed: bool) {
            assert_eq!(resp.status(), "0");
            if compressed {
                assert_eq!(resp.header("grpc-encoding"), Some("gzip"));
                assert_eq!(resp.data[0], 1);
                let data = GZIP.decode(&resp.data[5..], usize::MAX).await.unwrap();
                assert_eq!(data.unwrap(), 7i32.to_be_bytes());
            } else {
                assert_eq!(resp.header("grpc-encoding"), None);
                assert_eq!(resp.data, [0, 0, 0, 0, 4, 0, 0, 0, 7]);
            }
        }

        /// The server compresses a response only for a client that accepts
        /// the encoding: one listing it in `grpc-accept-encoding`, or using
        /// it for the request.
        #[tokio::test]
        async fn response_encoding_follows_client() {
            let resp = call(Some(GZIP), &[], &[], &THREE[..]).await;
            assert_seven(&resp, false).await;

            for accepted in ["gzip", "identity,gzip", "identity, deflate , gzip", "GZIP"] {
                let headers = [("grpc-accept-encoding", accepted)];
                let resp = call(Some(GZIP), &[], &headers, &THREE[..]).await;
                assert_seven(&resp, accepted != "GZIP").await;
            }

            for accepted in ["", "identity", "deflate, br", "gzipped", "x-gzip"] {
                let headers = [("grpc-accept-encoding", accepted)];
                let resp = call(Some(GZIP), &[], &headers, &THREE[..]).await;
                assert_seven(&resp, false).await;
            }

            // Listed in a second header.
            let headers = [
                ("grpc-accept-encoding", "identity"),
                ("grpc-accept-encoding", "gzip"),
            ];
            let resp = call(Some(GZIP), &[], &headers, &THREE[..]).await;
            assert_seven(&resp, true).await;

            // Used for the request, by a client that doesn't say what it
            // accepts.
            let headers = [("grpc-encoding", "gzip")];
            let body = compressed_three(GZIP).await;
            let resp = call(Some(GZIP), &[GZIP], &headers, body.clone()).await;
            assert_seven(&resp, true).await;

            // Used for the request, but not accepted.
            let headers = [
                ("grpc-encoding", "gzip"),
                ("grpc-accept-encoding", "identity"),
            ];
            let resp = call(Some(GZIP), &[GZIP], &headers, body).await;
            assert_seven(&resp, false).await;

            // A server not configured to compress doesn't.
            let headers = [("grpc-accept-encoding", "gzip")];
            let resp = call(None, &[GZIP], &headers, &THREE[..]).await;
            assert_seven(&resp, false).await;
        }

        /// A request in an encoding the server doesn't accept fails with
        /// `Unimplemented` and a `grpc-accept-encoding` header listing the
        /// ones it does.
        #[tokio::test]
        async fn unsupported_request_encoding() {
            let body = compressed_three(GZIP).await;

            let resp = call(None, &[], &[("grpc-encoding", "gzip")], body.clone()).await;
            assert_eq!(resp.status(), "12");
            assert_eq!(resp.header("grpc-accept-encoding"), Some("identity"));
            assert!(resp.data.is_empty());

            let resp = call(None, &[GZIP], &[("grpc-encoding", "snappy")], body).await;
            assert_eq!(resp.status(), "12");
            assert_eq!(resp.header("grpc-accept-encoding"), Some("identity, gzip"));
        }

        /// `identity` is the name of no compression.
        #[tokio::test]
        async fn identity_request_encoding() {
            let resp = call(None, &[], &[("grpc-encoding", "identity")], &THREE[..]).await;
            assert_seven(&resp, false).await;
        }

        /// A server tells every client the encodings it accepts.
        #[tokio::test]
        async fn accepted_encodings_are_advertised() {
            let resp = call(None, &[GZIP], &[], &THREE[..]).await;
            assert_seven(&resp, false).await;
            assert_eq!(resp.header("grpc-accept-encoding"), Some("identity, gzip"));

            let resp = call(None, &[], &[], &THREE[..]).await;
            assert_eq!(resp.header("grpc-accept-encoding"), Some("identity"));
        }

        /// Forwards the metadata of another server's response.
        struct Forward;

        impl UnaryService<i32> for Forward {
            type Response = i32;

            async fn call(&self, request: GrpcRequest<i32>) -> Result<GrpcResponse<i32>, Status> {
                let mut resp = GrpcResponse::new(request.into_inner());
                resp.metadata_mut().insert("grpc-accept-encoding", "snappy");
                resp.metadata_mut().append("grpc-accept-encoding", "lz4");
                resp.metadata_mut().insert("x-upstream", "1");
                Ok(resp)
            }
        }

        /// The encodings a server advertises are its own, whatever the
        /// handler's response metadata says.
        #[tokio::test]
        async fn advertised_encodings_are_the_servers() {
            let req = Request::builder()
                .content_type("application/grpc")
                .body(&THREE[..]);
            let resp = GrpcServer::new(I32Codec, None, &[GZIP])
                .unary(Forward, req)
                .await;
            let values: Vec<_> = resp
                .headers()
                .get_all("grpc-accept-encoding")
                .iter()
                .collect();
            assert_eq!(values, ["identity, gzip"]);
            assert_eq!(resp.headers()["x-upstream"], "1");
        }

        /// A message with the Compressed-Flag set in a request without a
        /// `grpc-encoding` fails with `Internal`.
        #[tokio::test]
        async fn compressed_flag_without_encoding() {
            let body = compressed_three(GZIP).await;
            for headers in [&[][..], &[("grpc-encoding", "identity")][..]] {
                let resp = call(None, &[GZIP], headers, body.clone()).await;
                assert_eq!(resp.status(), "13");
            }
        }

        /// An uncompressed message in a request with a `grpc-encoding` is
        /// read as it is.
        #[tokio::test]
        async fn uncompressed_message_with_encoding() {
            let resp = call(None, &[GZIP], &[("grpc-encoding", "gzip")], &THREE[..]).await;
            assert_seven(&resp, false).await;
        }
    }

    mod deadline {
        use std::time::Duration;

        use bytes::Bytes;
        use http::HeaderMap;
        use http_body_util::BodyExt;
        use tokio::time::Instant;

        use super::*;
        use crate::client::BoxBody;

        /// What became of a handler: whether it ran, and whether it, or its
        /// response stream, was dropped before its end.
        #[derive(Default)]
        struct Fate {
            called: AtomicBool,
            cancelled: AtomicBool,
        }

        /// Marks the handler as cancelled unless disarmed.
        struct CancelGuard(Arc<Fate>, bool);

        impl Drop for CancelGuard {
            fn drop(&mut self) {
                if !self.1 {
                    self.0.cancelled.store(true, Ordering::SeqCst);
                }
            }
        }

        /// Sleeps for as many milliseconds as the request says, or as the
        /// first request of a stream says, before responding with it. When
        /// streaming the response, does so before every message, endlessly.
        #[derive(Clone, Default)]
        struct Slow(Arc<Fate>);

        impl Slow {
            async fn sleep(&self, millis: i32) -> i32 {
                self.0.called.store(true, Ordering::SeqCst);
                let mut guard = CancelGuard(self.0.clone(), false);
                tokio::time::sleep(Duration::from_millis(millis as u64)).await;
                guard.1 = true;
                millis
            }

            fn stream(&self, millis: i32) -> Streaming<i32> {
                self.0.called.store(true, Ordering::SeqCst);
                let guard = CancelGuard(self.0.clone(), false);
                Streaming::new(futures_util::stream::unfold(
                    guard,
                    move |guard| async move {
                        tokio::time::sleep(Duration::from_millis(millis as u64)).await;
                        Some((Ok(millis), guard))
                    },
                ))
            }
        }

        impl UnaryService<i32> for Slow {
            type Response = i32;

            async fn call(&self, request: GrpcRequest<i32>) -> Result<GrpcResponse<i32>, Status> {
                Ok(GrpcResponse::new(self.sleep(request.into_inner()).await))
            }
        }

        impl ClientStreamingService<i32> for Slow {
            type Response = i32;

            async fn call(
                &self,
                request: GrpcRequest<Streaming<i32>>,
            ) -> Result<GrpcResponse<i32>, Status> {
                let millis = request.into_inner().try_next().await?.unwrap();
                Ok(GrpcResponse::new(self.sleep(millis).await))
            }
        }

        impl ServerStreamingService<i32> for Slow {
            type Response = i32;

            async fn call(
                &self,
                request: GrpcRequest<i32>,
            ) -> Result<GrpcResponse<Streaming<i32>>, Status> {
                Ok(GrpcResponse::new_streaming(
                    self.stream(request.into_inner()),
                ))
            }
        }

        impl BidirectionalStreamingService<i32> for Slow {
            type Response = i32;

            async fn call(
                &self,
                request: GrpcRequest<Streaming<i32>>,
            ) -> Result<GrpcResponse<Streaming<i32>>, Status> {
                let millis = request.into_inner().try_next().await?.unwrap();
                Ok(GrpcResponse::new_streaming(self.stream(millis)))
            }
        }

        #[derive(Clone, Copy, Debug, PartialEq)]
        enum Method {
            Unary,
            ClientStreaming,
            ServerStreaming,
            BidirectionalStreaming,
        }

        use Method::*;

        fn message(value: i32) -> Bytes {
            let mut buf = BytesMut::new();
            buf.put_slice(&[0, 0, 0, 0, 4]);
            buf.put_i32(value);
            buf.freeze()
        }

        /// The outcome of a call: the response messages, the `grpc-status`,
        /// whether it came in a Trailers-Only response, the time the call
        /// took, and what became of the handler.
        struct Outcome {
            messages: usize,
            status: String,
            trailers_only: bool,
            elapsed: Duration,
            called: bool,
            cancelled: bool,
        }

        /// Calls `Slow` with `timeout` as `grpc-timeout` and a request made
        /// of `body`.
        async fn call_with_body(method: Method, timeout: Option<&str>, body: Body) -> Outcome {
            let mut req = Request::builder().content_type("application/grpc");
            if let Some(timeout) = timeout {
                req = req.header("grpc-timeout", timeout);
            }
            let req = req.body(body);
            let slow = Slow::default();
            let fate = slow.0.clone();
            let server = GrpcServer::new(I32Codec, None, &[]);
            let start = Instant::now();
            let resp = match method {
                Unary => server.unary(slow, req).await,
                ClientStreaming => server.client_streaming(slow, req).await,
                ServerStreaming => server.server_streaming(slow, req).await,
                BidirectionalStreaming => server.bidirectional_streaming(slow, req).await,
            };
            let (parts, body) = resp.into_parts();
            let mut body: BoxBody = body.into();
            let mut messages = 0;
            let mut trailers: Option<HeaderMap> = None;
            while let Some(frame) = body.frame().await {
                match frame.unwrap().into_data() {
                    Ok(_) => messages += 1,
                    Err(frame) => trailers = Some(frame.into_trailers().unwrap()),
                }
            }
            let elapsed = start.elapsed();
            drop(body);
            let trailers_only = trailers.is_none();
            let headers = trailers.unwrap_or(parts.headers);
            Outcome {
                messages,
                status: headers["grpc-status"].to_str().unwrap().to_string(),
                trailers_only,
                elapsed,
                called: fate.called.load(Ordering::SeqCst),
                cancelled: fate.cancelled.load(Ordering::SeqCst),
            }
        }

        /// Calls `Slow` with `timeout` as `grpc-timeout` and `millis` as the
        /// request.
        async fn call(method: Method, timeout: Option<&str>, millis: i32) -> Outcome {
            call_with_body(method, timeout, Body::from_bytes(message(millis))).await
        }

        const MILLIS: fn(u64) -> Duration = Duration::from_millis;

        /// A handler that takes longer than the request's `grpc-timeout` is
        /// dropped when it expires, and the call fails with
        /// `DeadlineExceeded`.
        #[tokio::test(start_paused = true)]
        async fn handler_is_cancelled_at_deadline() {
            for method in [Unary, ClientStreaming] {
                let outcome = call(method, Some("100m"), 5000).await;
                assert_eq!(outcome.status, "4", "{method:?}");
                assert!(outcome.trailers_only);
                assert_eq!(outcome.elapsed, MILLIS(100));
                assert!(outcome.called && outcome.cancelled);
            }
        }

        /// A handler that responds before the deadline is not affected, nor
        /// one called without a `grpc-timeout`.
        #[tokio::test(start_paused = true)]
        async fn handler_within_deadline() {
            for method in [Unary, ClientStreaming] {
                for timeout in [Some("100m"), Some("1H"), Some("99999999H"), None] {
                    let outcome = call(method, timeout, 99).await;
                    assert_eq!(outcome.status, "0", "{method:?} {timeout:?}");
                    assert_eq!(outcome.messages, 1);
                    assert_eq!(outcome.elapsed, MILLIS(99));
                    assert!(outcome.called && !outcome.cancelled);
                }
                // Without a deadline, a handler may take as long as it likes.
                let outcome = call(method, None, 3_600_000).await;
                assert_eq!(outcome.status, "0", "{method:?}");
            }
        }

        /// A response stream is cut at the deadline: the messages sent by
        /// then are followed by `DeadlineExceeded`, and the handler's stream
        /// is dropped.
        #[tokio::test(start_paused = true)]
        async fn response_stream_ends_at_deadline() {
            for method in [ServerStreaming, BidirectionalStreaming] {
                let outcome = call(method, Some("100m"), 30).await;
                assert_eq!(outcome.status, "4", "{method:?}");
                assert_eq!(outcome.messages, 3);
                assert!(!outcome.trailers_only);
                assert_eq!(outcome.elapsed, MILLIS(100));
                assert!(outcome.called && outcome.cancelled);
            }
        }

        /// The time a client takes to send its request counts: a request
        /// that doesn't end fails at the deadline.
        #[tokio::test(start_paused = true)]
        async fn request_is_read_within_deadline() {
            fn stalled() -> Body {
                Body::from_bytes_stream(
                    futures_util::stream::iter([Ok::<_, IoError>(message(1))])
                        .chain(futures_util::stream::pending()),
                )
            }

            // These read the whole request before calling the handler.
            for method in [Unary, ServerStreaming] {
                let outcome = call_with_body(method, Some("100m"), stalled()).await;
                assert_eq!(outcome.status, "4", "{method:?}");
                assert!(outcome.trailers_only);
                assert_eq!(outcome.elapsed, MILLIS(100));
                assert!(!outcome.called);
            }
        }

        /// A zero timeout has expired on arrival: the handler isn't called,
        /// even one that would respond at once.
        #[tokio::test(start_paused = true)]
        async fn zero_timeout() {
            for method in [
                Unary,
                ClientStreaming,
                ServerStreaming,
                BidirectionalStreaming,
            ] {
                for millis in [0, 10] {
                    let outcome = call(method, Some("0n"), millis).await;
                    assert_eq!(outcome.status, "4", "{method:?}");
                    assert_eq!(outcome.messages, 0);
                    assert!(outcome.trailers_only);
                    assert_eq!(outcome.elapsed, Duration::ZERO);
                    assert!(!outcome.called);
                }
            }
        }

        /// Responds with an endless stream of messages that are always ready.
        struct Flood;

        impl ServerStreamingService<i32> for Flood {
            type Response = i32;

            async fn call(
                &self,
                _request: GrpcRequest<i32>,
            ) -> Result<GrpcResponse<Streaming<i32>>, Status> {
                Ok(GrpcResponse::new_streaming(
                    futures_util::stream::repeat_with(|| Ok(1)),
                ))
            }
        }

        /// A response stream that never makes the server wait is cut at the
        /// deadline too.
        #[tokio::test(start_paused = true)]
        async fn ready_response_stream_ends_at_deadline() {
            let req = Request::builder()
                .content_type("application/grpc")
                .header("grpc-timeout", "100m")
                .body(message(1));
            let resp = GrpcServer::new(I32Codec, None, &[])
                .server_streaming(Flood, req)
                .await;
            let mut body: BoxBody = resp.into_body().into();
            let mut messages = 0;
            let status = loop {
                let frame = body.frame().await.expect("the response ended").unwrap();
                match frame.into_data() {
                    Ok(_) => messages += 1,
                    Err(frame) => break frame.into_trailers().unwrap()["grpc-status"].clone(),
                }
                // A client reading a message every 30 ms.
                tokio::time::sleep(MILLIS(30)).await;
            };
            assert_eq!(status, "4");
            assert_eq!(messages, 4);
        }

        /// A malformed `grpc-timeout` fails the call with `Internal`, without
        /// calling the handler.
        #[tokio::test(start_paused = true)]
        async fn malformed_timeout() {
            for method in [
                Unary,
                ClientStreaming,
                ServerStreaming,
                BidirectionalStreaming,
            ] {
                for timeout in ["", "100", "1s", "100000000S", "-1S"] {
                    let outcome = call(method, Some(timeout), 10).await;
                    assert_eq!(outcome.status, "13", "{method:?} {timeout:?}");
                    assert!(outcome.trailers_only);
                    assert!(!outcome.called);
                }
            }
        }

        /// Over a connection: a client's timeout fails its call on time, the
        /// server drops the handler, and the connection serves other calls.
        #[tokio::test]
        async fn timeout_over_http2() {
            let slow = Slow::default();
            let fate = slow.0.clone();
            let app =
                poem::Route::new()
                    .at(
                        "/unary",
                        poem::endpoint::make({
                            let slow = slow.clone();
                            move |req| {
                                let slow = slow.clone();
                                async move {
                                    GrpcServer::new(I32Codec, None, &[]).unary(slow, req).await
                                }
                            }
                        }),
                    )
                    .at(
                        "/stream",
                        poem::endpoint::make(move |req| {
                            let slow = slow.clone();
                            async move {
                                GrpcServer::new(I32Codec, None, &[])
                                    .server_streaming(slow, req)
                                    .await
                            }
                        }),
                    );
            let acceptor = TcpListener::bind("127.0.0.1:0")
                .into_acceptor()
                .await
                .unwrap();
            let addr = *acceptor.local_addr()[0].as_socket_addr().unwrap();
            tokio::spawn(Server::new_with_acceptor(acceptor).run(app));
            let cli = GrpcClient::new(
                ClientConfig::builder()
                    .uri(format!("http://{addr}"))
                    .build()
                    .unwrap(),
            );
            let request = |millis: i32, timeout: Option<Duration>| {
                let mut request = GrpcRequest::new(millis);
                if let Some(timeout) = timeout {
                    request.set_timeout(timeout);
                }
                request
            };
            let wait_for_cancellation = || async {
                for _ in 0..500 {
                    if fate.cancelled.swap(false, Ordering::SeqCst) {
                        return;
                    }
                    tokio::time::sleep(MILLIS(10)).await;
                }
                panic!("the handler wasn't dropped");
            };

            // Within the timeout.
            let resp = cli
                .unary("/unary", I32Codec, request(10, Some(MILLIS(10_000))))
                .await;
            assert_eq!(resp.unwrap().into_inner(), 10);
            assert!(!fate.cancelled.load(Ordering::SeqCst));

            // A unary call that takes 60 s, with a timeout of 200 ms.
            let start = Instant::now();
            let status = cli
                .unary("/unary", I32Codec, request(60_000, Some(MILLIS(200))))
                .await
                .unwrap_err();
            assert_eq!(status.code(), Code::DeadlineExceeded);
            assert!(start.elapsed() >= MILLIS(200));
            assert!(start.elapsed() < MILLIS(5000), "{:?}", start.elapsed());
            wait_for_cancellation().await;

            // An endless stream of a message every 50 ms, with a timeout of
            // 300 ms.
            let start = Instant::now();
            let mut stream = cli
                .server_streaming("/stream", I32Codec, request(50, Some(MILLIS(300))))
                .await
                .unwrap()
                .into_inner();
            let mut messages = 0;
            let status = loop {
                match stream.try_next().await {
                    Ok(Some(_)) => messages += 1,
                    Ok(None) => panic!("the stream ended"),
                    Err(status) => break status,
                }
            };
            assert_eq!(status.code(), Code::DeadlineExceeded);
            assert!((1..=6).contains(&messages), "{messages}");
            assert!(start.elapsed() >= MILLIS(300));
            assert!(start.elapsed() < MILLIS(5000), "{:?}", start.elapsed());
            wait_for_cancellation().await;

            // The connection is still good.
            let resp = cli.unary("/unary", I32Codec, request(10, None)).await;
            assert_eq!(resp.unwrap().into_inner(), 10);
        }
    }
}
