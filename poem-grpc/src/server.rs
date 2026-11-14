use futures_util::StreamExt;
use http::HeaderValue;
use poem::{Body, Request, Response};

use crate::{
    Code, CompressionEncoding, Metadata, MethodType, Request as GrpcRequest,
    Response as GrpcResponse, Status, Streaming,
    codec::Codec,
    compression::get_incoming_encodings,
    encoding::{
        DEFAULT_MAX_DECODING_MESSAGE_SIZE, DEFAULT_MAX_ENCODING_MESSAGE_SIZE,
        create_decode_request_body, create_encode_response_body,
    },
    service::{
        BidirectionalStreamingService, ClientStreamingService, ServerStreamingService, UnaryService,
    },
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

    pub async fn unary<S>(mut self, service: S, request: Request) -> Response
    where
        S: UnaryService<T::Decode, Response = T::Encode>,
    {
        let (parts, body) = request.into_parts();
        let mut resp = Response::default().set_content_type(T::CONTENT_TYPES[0]);
        let incoming_encoding = match get_incoming_encodings(&parts.headers, self.accept_compressed)
        {
            Ok(incoming_encoding) => incoming_encoding,
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

        let res = match read_single_message(&mut stream).await {
            Ok(message) => {
                service
                    .call(GrpcRequest {
                        metadata: Metadata {
                            headers: parts.headers,
                        },
                        message,
                        extensions: parts.extensions,
                    })
                    .await
            }
            Err(status) => Err(status),
        };

        match res {
            Ok(grpc_resp) => {
                let GrpcResponse { metadata, message } = grpc_resp;
                let body = create_encode_response_body(
                    self.codec.encoder(),
                    Streaming::new(futures_util::stream::once(async move { Ok(message) })),
                    self.send_compressed,
                    self.max_encoding_message_size,
                );
                update_http_response(&mut resp, metadata, body, self.send_compressed);
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
        let mut resp = Response::default().set_content_type(T::CONTENT_TYPES[0]);
        let incoming_encoding = match get_incoming_encodings(&parts.headers, self.accept_compressed)
        {
            Ok(incoming_encoding) => incoming_encoding,
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

        let res = service
            .call(GrpcRequest {
                metadata: Metadata {
                    headers: parts.headers,
                },
                extensions: parts.extensions,
                message: stream,
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
                update_http_response(&mut resp, metadata, body, self.send_compressed);
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
        let mut resp = Response::default().set_content_type(T::CONTENT_TYPES[0]);
        let incoming_encoding = match get_incoming_encodings(&parts.headers, self.accept_compressed)
        {
            Ok(incoming_encoding) => incoming_encoding,
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

        let res = match read_single_message(&mut stream).await {
            Ok(message) => {
                service
                    .call(GrpcRequest {
                        metadata: Metadata {
                            headers: parts.headers,
                        },
                        message,
                        extensions: parts.extensions,
                    })
                    .await
            }
            Err(status) => Err(status),
        };

        match res {
            Ok(grpc_resp) => {
                let GrpcResponse { metadata, message } = grpc_resp;
                let body = create_encode_response_body(
                    self.codec.encoder(),
                    message,
                    self.send_compressed,
                    self.max_encoding_message_size,
                );
                update_http_response(&mut resp, metadata, body, self.send_compressed);
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
        let mut resp = Response::default().set_content_type(T::CONTENT_TYPES[0]);
        let incoming_encoding = match get_incoming_encodings(&parts.headers, self.accept_compressed)
        {
            Ok(incoming_encoding) => incoming_encoding,
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

        let res = service
            .call(GrpcRequest {
                metadata: Metadata {
                    headers: parts.headers,
                },
                message: stream,
                extensions: parts.extensions,
            })
            .await;

        match res {
            Ok(grpc_resp) => {
                let GrpcResponse { metadata, message } = grpc_resp;
                let body = create_encode_response_body(
                    self.codec.encoder(),
                    message,
                    self.send_compressed,
                    self.max_encoding_message_size,
                );
                update_http_response(&mut resp, metadata, body, self.send_compressed);
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
) {
    resp.headers_mut().extend(metadata.headers);
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
}
