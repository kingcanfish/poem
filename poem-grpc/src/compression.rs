use std::{io::Result as IoResult, str::FromStr};

use http::{HeaderMap, HeaderValue};

use crate::{Code, Status};

/// The compression encodings.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum CompressionEncoding {
    /// gzip
    #[cfg(feature = "gzip")]
    #[cfg_attr(docsrs, doc(cfg(feature = "gzip")))]
    GZIP,
    /// deflate, in the zlib format ([RFC 1950]), as gRPC and HTTP mean by
    /// that name
    ///
    /// [RFC 1950]: https://datatracker.ietf.org/doc/html/rfc1950
    #[cfg(feature = "deflate")]
    #[cfg_attr(docsrs, doc(cfg(feature = "deflate")))]
    DEFLATE,
    /// brotli
    #[cfg(feature = "brotli")]
    #[cfg_attr(docsrs, doc(cfg(feature = "brotli")))]
    BROTLI,
    /// zstd
    #[cfg(feature = "zstd")]
    #[cfg_attr(docsrs, doc(cfg(feature = "zstd")))]
    ZSTD,
}

impl FromStr for CompressionEncoding {
    type Err = ();

    #[inline]
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            #[cfg(feature = "gzip")]
            "gzip" => Ok(CompressionEncoding::GZIP),
            #[cfg(feature = "deflate")]
            "deflate" => Ok(CompressionEncoding::DEFLATE),
            #[cfg(feature = "brotli")]
            "br" => Ok(CompressionEncoding::BROTLI),
            #[cfg(feature = "zstd")]
            "zstd" => Ok(CompressionEncoding::ZSTD),
            _ => Err(()),
        }
    }
}

impl CompressionEncoding {
    /// Returns the encoding name.
    #[allow(unreachable_patterns)]
    pub fn as_str(&self) -> &'static str {
        match self {
            #[cfg(feature = "gzip")]
            CompressionEncoding::GZIP => "gzip",
            #[cfg(feature = "deflate")]
            CompressionEncoding::DEFLATE => "deflate",
            #[cfg(feature = "brotli")]
            CompressionEncoding::BROTLI => "br",
            #[cfg(feature = "zstd")]
            CompressionEncoding::ZSTD => "zstd",
            _ => unreachable!(),
        }
    }

    #[allow(
        unreachable_code,
        unused_imports,
        unused_mut,
        unused_variables,
        unreachable_patterns
    )]
    pub(crate) async fn encode(&self, data: &[u8]) -> IoResult<Vec<u8>> {
        use tokio::io::AsyncReadExt;

        let mut buf = Vec::new();

        match self {
            #[cfg(feature = "gzip")]
            CompressionEncoding::GZIP => {
                async_compression::tokio::bufread::GzipEncoder::new(data)
                    .read_to_end(&mut buf)
                    .await?;
            }
            #[cfg(feature = "deflate")]
            CompressionEncoding::DEFLATE => {
                async_compression::tokio::bufread::ZlibEncoder::new(data)
                    .read_to_end(&mut buf)
                    .await?;
            }
            #[cfg(feature = "brotli")]
            CompressionEncoding::BROTLI => {
                async_compression::tokio::bufread::BrotliEncoder::new(data)
                    .read_to_end(&mut buf)
                    .await?;
            }
            #[cfg(feature = "zstd")]
            CompressionEncoding::ZSTD => {
                async_compression::tokio::bufread::ZstdEncoder::new(data)
                    .read_to_end(&mut buf)
                    .await?;
            }
            _ => unreachable!(),
        }

        Ok(buf)
    }

    /// Decompresses `data`, or returns `None` if that takes more than
    /// `max_size` bytes, of which no more than one extra is produced.
    pub(crate) async fn decode(&self, data: &[u8], max_size: usize) -> IoResult<Option<Vec<u8>>> {
        // One byte more than allowed tells a message that is too large from
        // one of exactly `max_size` bytes.
        let limit = (max_size as u64).saturating_add(1);
        let buf = self.decode_up_to(data, limit).await?;
        Ok((buf.len() <= max_size).then_some(buf))
    }

    /// Decompresses the first `limit` bytes of `data`'s contents.
    #[allow(
        unreachable_code,
        unused_imports,
        unused_mut,
        unused_variables,
        unreachable_patterns
    )]
    pub(crate) async fn decode_up_to(&self, data: &[u8], limit: u64) -> IoResult<Vec<u8>> {
        use tokio::io::AsyncReadExt;

        let mut buf = Vec::new();

        match self {
            #[cfg(feature = "gzip")]
            CompressionEncoding::GZIP => {
                async_compression::tokio::bufread::GzipDecoder::new(data)
                    .take(limit)
                    .read_to_end(&mut buf)
                    .await?;
            }
            #[cfg(feature = "deflate")]
            CompressionEncoding::DEFLATE => {
                async_compression::tokio::bufread::ZlibDecoder::new(data)
                    .take(limit)
                    .read_to_end(&mut buf)
                    .await?;
            }
            #[cfg(feature = "brotli")]
            CompressionEncoding::BROTLI => {
                async_compression::tokio::bufread::BrotliDecoder::new(data)
                    .take(limit)
                    .read_to_end(&mut buf)
                    .await?;
            }
            #[cfg(feature = "zstd")]
            CompressionEncoding::ZSTD => {
                async_compression::tokio::bufread::ZstdDecoder::new(data)
                    .take(limit)
                    .read_to_end(&mut buf)
                    .await?;
            }
            _ => unreachable!(),
        }

        Ok(buf)
    }
}

const GRPC_ENCODING: &str = "grpc-encoding";
const GRPC_ACCEPT_ENCODING: &str = "grpc-accept-encoding";

/// The name of no compression.
const IDENTITY: &str = "identity";

/// The names of no compression and of `encodings`, as listed in a
/// `grpc-accept-encoding` header.
fn accepted_encodings(encodings: &[CompressionEncoding]) -> String {
    let mut names = String::from(IDENTITY);
    for encoding in encodings {
        names.push_str(", ");
        names.push_str(encoding.as_str());
    }
    names
}

/// The `grpc-accept-encoding` header of a peer accepting `encodings`.
pub(crate) fn accept_encoding_header(encodings: &[CompressionEncoding]) -> HeaderValue {
    HeaderValue::from_str(&accepted_encodings(encodings)).expect("BUG: invalid encoding")
}

/// The encoding of the messages received with `headers`, or the description
/// of the error if it isn't one of `accept_compressed`.
fn incoming_encoding(
    headers: &HeaderMap,
    accept_compressed: &[CompressionEncoding],
) -> Result<Option<CompressionEncoding>, String> {
    let Some(value) = headers.get(GRPC_ENCODING) else {
        return Ok(None);
    };
    if value == IDENTITY {
        return Ok(None);
    }
    value
        .to_str()
        .ok()
        .and_then(|value| value.parse::<CompressionEncoding>().ok())
        .filter(|encoding| accept_compressed.contains(encoding))
        .map(Some)
        .ok_or_else(|| {
            format!(
                "unsupported encoding `{}`, supported: {}",
                String::from_utf8_lossy(value.as_bytes()),
                accepted_encodings(accept_compressed)
            )
        })
}

/// The encoding of a request's messages, as a server reads it.
///
/// One it doesn't accept is `Unimplemented`. The response must then carry the
/// `grpc-accept-encoding` header listing those it does, as every response of
/// a server does.
pub(crate) fn get_request_encoding(
    headers: &HeaderMap,
    accept_compressed: &[CompressionEncoding],
) -> Result<Option<CompressionEncoding>, Status> {
    incoming_encoding(headers, accept_compressed)
        .map_err(|message| Status::new(Code::Unimplemented).with_message(message))
}

/// The encoding of a response's messages, as a client reads it.
///
/// One it doesn't accept is `Internal`.
pub(crate) fn get_response_encoding(
    headers: &HeaderMap,
    accept_compressed: &[CompressionEncoding],
) -> Result<Option<CompressionEncoding>, Status> {
    incoming_encoding(headers, accept_compressed)
        .map_err(|message| Status::new(Code::Internal).with_message(message))
}

/// The encoding a server configured to send `send_compressed` uses for the
/// response to the request with `headers`, whose messages are in
/// `request_encoding`.
///
/// It only compresses for a client known to read the encoding: one that lists
/// it in `grpc-accept-encoding`. A client that doesn't send that header, as
/// those of older versions of this crate, is taken to read the encoding it
/// uses itself.
pub(crate) fn select_response_encoding(
    headers: &HeaderMap,
    send_compressed: Option<CompressionEncoding>,
    request_encoding: Option<CompressionEncoding>,
) -> Option<CompressionEncoding> {
    let encoding = send_compressed?;
    let accepted = if headers.contains_key(GRPC_ACCEPT_ENCODING) {
        headers
            .get_all(GRPC_ACCEPT_ENCODING)
            .iter()
            .filter_map(|value| value.to_str().ok())
            .flat_map(|value| value.split(','))
            .any(|name| name.trim() == encoding.as_str())
    } else {
        request_encoding == Some(encoding)
    };
    accepted.then_some(encoding)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn accept_encoding_lists_identity_first() {
        assert_eq!(accept_encoding_header(&[]), "identity");
    }

    /// Every encoding compresses to something else than its input, and
    /// decompresses back to it.
    #[tokio::test]
    #[allow(clippy::useless_vec)]
    async fn round_trip() {
        let encodings: Vec<CompressionEncoding> = vec![
            #[cfg(feature = "gzip")]
            CompressionEncoding::GZIP,
            #[cfg(feature = "deflate")]
            CompressionEncoding::DEFLATE,
            #[cfg(feature = "brotli")]
            CompressionEncoding::BROTLI,
            #[cfg(feature = "zstd")]
            CompressionEncoding::ZSTD,
        ];
        let data = b"hello hello hello hello hello hello".to_vec();
        for encoding in encodings {
            let compressed = encoding.encode(&data).await.unwrap();
            assert_ne!(compressed, data, "{encoding:?}");
            let decompressed = encoding.decode(&compressed, usize::MAX).await.unwrap();
            assert_eq!(decompressed, Some(data.clone()), "{encoding:?}");
            assert_eq!(encoding.as_str().parse(), Ok(encoding));
        }
    }

    /// `deflate` is the zlib format of RFC 1950, as the gRPC spec requires,
    /// not a raw deflate stream.
    #[cfg(feature = "deflate")]
    #[tokio::test]
    async fn deflate_is_zlib() {
        let deflate = CompressionEncoding::DEFLATE;

        // `zlib.compress(b"hello")` of Python.
        let zlib = [
            0x78, 0x9c, 0xcb, 0x48, 0xcd, 0xc9, 0xc9, 0x07, 0x00, 0x06, 0x2c, 0x02, 0x15,
        ];
        let data = deflate.decode(&zlib, usize::MAX).await.unwrap();
        assert_eq!(data.unwrap(), b"hello");

        // The same without the 2-byte header and the checksum.
        let raw = &zlib[2..zlib.len() - 4];
        assert!(deflate.decode(raw, usize::MAX).await.is_err());

        // A zlib header: the deflate method with a window of up to 32 KiB,
        // and a check value making the two bytes a multiple of 31.
        let compressed = deflate.encode(b"hello").await.unwrap();
        assert_eq!(compressed[0] & 0x0f, 8);
        assert!(compressed[0] >> 4 <= 7);
        assert_eq!(u16::from_be_bytes([compressed[0], compressed[1]]) % 31, 0);
    }

    #[cfg(feature = "gzip")]
    mod gzip {
        use super::*;

        const GZIP: CompressionEncoding = CompressionEncoding::GZIP;

        fn headers(headers: &[(&'static str, &'static str)]) -> HeaderMap {
            let mut map = HeaderMap::new();
            for (name, value) in headers {
                map.append(*name, HeaderValue::from_static(value));
            }
            map
        }

        #[test]
        fn incoming_encodings() {
            for encoding in [&[][..], &[("grpc-encoding", "identity")][..]] {
                let headers = headers(encoding);
                assert_eq!(get_request_encoding(&headers, &[]).unwrap(), None);
                assert_eq!(get_response_encoding(&headers, &[GZIP]).unwrap(), None);
            }

            let gzip = headers(&[("grpc-encoding", "gzip")]);
            assert_eq!(get_request_encoding(&gzip, &[GZIP]).unwrap(), Some(GZIP));
            assert_eq!(get_response_encoding(&gzip, &[GZIP]).unwrap(), Some(GZIP));

            // Known, but not accepted.
            let status = get_request_encoding(&gzip, &[]).unwrap_err();
            assert_eq!(status.code(), Code::Unimplemented);
            assert_eq!(
                status.message(),
                Some("unsupported encoding `gzip`, supported: identity")
            );
            let status = get_response_encoding(&gzip, &[]).unwrap_err();
            assert_eq!(status.code(), Code::Internal);
            assert_eq!(
                status.message(),
                Some("unsupported encoding `gzip`, supported: identity")
            );

            // Unknown.
            for name in ["snappy", "", "Gzip", "gzip, identity"] {
                let headers = headers(&[("grpc-encoding", name)]);
                let status = get_request_encoding(&headers, &[GZIP]).unwrap_err();
                assert_eq!(status.code(), Code::Unimplemented);
                let status = get_response_encoding(&headers, &[GZIP]).unwrap_err();
                assert_eq!(status.code(), Code::Internal);
            }
        }

        #[test]
        fn response_encoding() {
            let select = |accepted: &[(&'static str, &'static str)], request| {
                select_response_encoding(&headers(accepted), Some(GZIP), request)
            };
            assert_eq!(select(&[], None), None);
            assert_eq!(select(&[], Some(GZIP)), Some(GZIP));
            assert_eq!(
                select(&[("grpc-accept-encoding", "gzip")], None),
                Some(GZIP)
            );
            assert_eq!(
                select(&[("grpc-accept-encoding", "identity,deflate,gzip")], None),
                Some(GZIP)
            );
            assert_eq!(select(&[("grpc-accept-encoding", "identity")], None), None);
            // What the client says it accepts counts, not what it sends.
            assert_eq!(
                select(&[("grpc-accept-encoding", "identity")], Some(GZIP)),
                None
            );
            assert_eq!(select(&[("grpc-accept-encoding", "")], Some(GZIP)), None);
            assert_eq!(
                select_response_encoding(
                    &headers(&[("grpc-accept-encoding", "gzip")]),
                    None,
                    Some(GZIP)
                ),
                None
            );
        }
    }
}
