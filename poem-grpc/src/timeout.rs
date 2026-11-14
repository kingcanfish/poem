//! The `grpc-timeout` header and the deadline of a call.
//!
//! <https://github.com/grpc/grpc/blob/master/doc/PROTOCOL-HTTP2.md#requests>

use std::{future::Future, task::Poll, time::Duration};

use futures_util::StreamExt;
use http::{HeaderMap, HeaderValue};
use tokio::time::Instant;

use crate::{Code, Status, Streaming};

pub(crate) const GRPC_TIMEOUT: &str = "grpc-timeout";

/// The largest value a `grpc-timeout` can hold: 8 digits.
const MAX_TIMEOUT_VALUE: u128 = 99_999_999;

/// The status of a call that didn't complete before its deadline.
pub(crate) fn deadline_exceeded() -> Status {
    Status::new(Code::DeadlineExceeded).with_message("deadline exceeded")
}

/// Parses a `grpc-timeout` value: at most 8 digits and a unit.
fn parse_timeout(value: &[u8]) -> Option<Duration> {
    let (unit, digits) = value.split_last()?;
    if digits.is_empty() || digits.len() > 8 || !digits.iter().all(u8::is_ascii_digit) {
        return None;
    }
    let value: u64 = std::str::from_utf8(digits).ok()?.parse().ok()?;
    Some(match unit {
        b'H' => Duration::from_secs(value * 60 * 60),
        b'M' => Duration::from_secs(value * 60),
        b'S' => Duration::from_secs(value),
        b'm' => Duration::from_millis(value),
        b'u' => Duration::from_micros(value),
        b'n' => Duration::from_nanos(value),
        _ => return None,
    })
}

/// The timeout in the `grpc-timeout` header of `headers`, if any. A malformed
/// one is `Internal`.
pub(crate) fn timeout_from_headers(headers: &HeaderMap) -> Result<Option<Duration>, Status> {
    let Some(value) = headers.get(GRPC_TIMEOUT) else {
        return Ok(None);
    };
    match parse_timeout(value.as_bytes()) {
        Some(timeout) => Ok(Some(timeout)),
        None => Err(Status::new(Code::Internal).with_message(format!(
            "malformed grpc-timeout: {}",
            String::from_utf8_lossy(value.as_bytes())
        ))),
    }
}

/// The `grpc-timeout` value for `timeout`, in the finest unit whose value
/// fits in 8 digits, rounded up so the deadline is never sooner than asked.
pub(crate) fn timeout_header(timeout: Duration) -> HeaderValue {
    const UNITS: [(u128, char); 6] = [
        (1, 'n'),
        (1_000, 'u'),
        (1_000_000, 'm'),
        (1_000_000_000, 'S'),
        (60 * 1_000_000_000, 'M'),
        (60 * 60 * 1_000_000_000, 'H'),
    ];
    let nanos = timeout.as_nanos();
    let (value, unit) = UNITS
        .iter()
        .map(|(unit_nanos, unit)| (nanos.div_ceil(*unit_nanos), *unit))
        .find(|(value, _)| *value <= MAX_TIMEOUT_VALUE)
        // More than 11000 years: as long as can be said.
        .unwrap_or((MAX_TIMEOUT_VALUE, 'H'));
    HeaderValue::from_str(&format!("{value}{unit}")).expect("BUG: invalid timeout")
}

/// The deadline of a call starting now with the `grpc-timeout` of `headers`.
///
/// There is none without the header, or for a timeout beyond what the clock
/// can tell.
pub(crate) fn deadline_from_headers(headers: &HeaderMap) -> Result<Option<Instant>, Status> {
    Ok(timeout_from_headers(headers)?.and_then(|timeout| Instant::now().checked_add(timeout)))
}

/// Runs `fut` until `deadline`, after which it is dropped and the result is
/// `DeadlineExceeded`. It isn't run at all past the deadline.
pub(crate) async fn with_deadline<T>(
    deadline: Option<Instant>,
    fut: impl Future<Output = Result<T, Status>>,
) -> Result<T, Status> {
    match deadline {
        // `timeout_at` polls the future before looking at the time.
        Some(deadline) if deadline <= Instant::now() => Err(deadline_exceeded()),
        Some(deadline) => tokio::time::timeout_at(deadline, fut)
            .await
            .unwrap_or_else(|_| Err(deadline_exceeded())),
        None => fut.await,
    }
}

/// Ends `stream` with `DeadlineExceeded` at `deadline`, after which it is
/// dropped.
pub(crate) fn stream_with_deadline<T: Send + 'static>(
    deadline: Option<Instant>,
    mut stream: Streaming<T>,
) -> Streaming<T> {
    let Some(deadline) = deadline else {
        return stream;
    };
    Streaming::new(async_stream::stream! {
        let sleep = tokio::time::sleep_until(deadline);
        futures_util::pin_mut!(sleep);
        loop {
            // The deadline is looked at first, so a stream whose items are
            // always ready is cut too.
            let item = std::future::poll_fn(|cx| {
                if sleep.as_mut().poll(cx).is_ready() {
                    return Poll::Ready(None);
                }
                stream.poll_next_unpin(cx).map(Some)
            })
            .await;
            match item {
                Some(Some(item)) => yield item,
                Some(None) => break,
                None => {
                    drop(stream);
                    yield Err(deadline_exceeded());
                    break;
                }
            }
        }
    })
}

#[cfg(test)]
mod tests {
    use futures_util::TryStreamExt;

    use super::*;

    fn parse(value: &'static str) -> Result<Option<Duration>, Status> {
        let mut headers = HeaderMap::new();
        headers.insert(GRPC_TIMEOUT, HeaderValue::from_static(value));
        timeout_from_headers(&headers)
    }

    #[test]
    fn parse_units() {
        assert_eq!(timeout_from_headers(&HeaderMap::new()).unwrap(), None);
        for (value, timeout) in [
            ("1H", Duration::from_secs(3600)),
            ("2M", Duration::from_secs(120)),
            ("3S", Duration::from_secs(3)),
            ("4m", Duration::from_millis(4)),
            ("5u", Duration::from_micros(5)),
            ("6n", Duration::from_nanos(6)),
            ("0S", Duration::ZERO),
            ("00000001S", Duration::from_secs(1)),
            ("99999999H", Duration::from_secs(99_999_999 * 3600)),
            ("99999999n", Duration::from_nanos(99_999_999)),
        ] {
            assert_eq!(parse(value).unwrap(), Some(timeout), "{value}");
        }
    }

    #[test]
    fn parse_malformed() {
        for value in [
            "",
            "S",
            "1",
            "1s",
            "1 S",
            " 1S",
            "1S ",
            "-1S",
            "+1S",
            "1.5S",
            "100000000S",
            "1h",
            "1SS",
            "1ms",
            "S1",
        ] {
            let status = parse(value).unwrap_err();
            assert_eq!(status.code(), Code::Internal, "{value}");
            assert_eq!(
                status.message(),
                Some(format!("malformed grpc-timeout: {value}").as_str())
            );
        }
    }

    #[test]
    fn encode() {
        for (timeout, value) in [
            (Duration::ZERO, "0n"),
            (Duration::from_nanos(1), "1n"),
            (Duration::from_nanos(99_999_999), "99999999n"),
            // Rounded up to the next microsecond, millisecond, ...
            (Duration::from_nanos(100_000_000), "100000u"),
            (Duration::from_nanos(100_000_001), "100001u"),
            (Duration::from_millis(1500), "1500000u"),
            (Duration::from_secs(1), "1000000u"),
            (Duration::from_secs(30), "30000000u"),
            (Duration::from_secs(100), "100000m"),
            (Duration::from_secs(3600), "3600000m"),
            (Duration::from_secs(100_000), "100000S"),
            (Duration::from_secs(99_999_999), "99999999S"),
            (Duration::from_secs(100_000_000), "1666667M"),
            (Duration::from_secs(99_999_999 * 60 + 1), "1666667H"),
            (Duration::from_secs(99_999_999 * 3600), "99999999H"),
            (Duration::from_secs(99_999_999 * 3600 + 1), "99999999H"),
            (Duration::MAX, "99999999H"),
        ] {
            assert_eq!(timeout_header(timeout), value, "{timeout:?}");
        }
    }

    /// What is sent is never sooner than the timeout, and later by less than
    /// a hundred-thousandth of it.
    #[test]
    fn encode_round_trip() {
        for nanos in [0, 1, 999, 123_456_789, 99_999_999_999, 987_654_321_012_345] {
            let timeout = Duration::from_nanos(nanos);
            let mut headers = HeaderMap::new();
            headers.insert(GRPC_TIMEOUT, timeout_header(timeout));
            let parsed = timeout_from_headers(&headers).unwrap().unwrap();
            assert!(parsed >= timeout, "{timeout:?}");
            // Less than one unit, of which there are at least 100000.
            assert!(parsed.as_nanos() - timeout.as_nanos() <= timeout.as_nanos() / 100_000);
        }
    }

    #[test]
    fn deadline() {
        assert!(deadline_from_headers(&HeaderMap::new()).unwrap().is_none());
        let mut headers = HeaderMap::new();
        headers.insert(GRPC_TIMEOUT, HeaderValue::from_static("1S"));
        let deadline = deadline_from_headers(&headers).unwrap().unwrap();
        let left = deadline - Instant::now();
        assert!(left <= Duration::from_secs(1) && left > Duration::from_millis(900));
        // Beyond what an `Instant` can tell.
        headers.insert(GRPC_TIMEOUT, HeaderValue::from_static("99999999H"));
        let _ = deadline_from_headers(&headers).unwrap();
        headers.insert(GRPC_TIMEOUT, HeaderValue::from_static("x"));
        assert!(deadline_from_headers(&headers).is_err());
    }

    #[tokio::test(start_paused = true)]
    async fn future_with_deadline() {
        let sleep = |millis| async move {
            tokio::time::sleep(Duration::from_millis(millis)).await;
            Ok::<_, Status>(millis)
        };
        assert_eq!(with_deadline(None, sleep(1000)).await.unwrap(), 1000);
        let deadline = Some(Instant::now() + Duration::from_millis(100));
        assert_eq!(with_deadline(deadline, sleep(99)).await.unwrap(), 99);
        let deadline = Some(Instant::now() + Duration::from_millis(100));
        let status = with_deadline(deadline, sleep(101)).await.unwrap_err();
        assert_eq!(status.code(), Code::DeadlineExceeded);
        // Past the deadline, the future isn't even polled.
        let polled = std::cell::Cell::new(false);
        let status = with_deadline(Some(Instant::now()), async {
            polled.set(true);
            Ok(())
        })
        .await
        .unwrap_err();
        assert_eq!(status.code(), Code::DeadlineExceeded);
        assert!(!polled.get());
        // An error before the deadline is not replaced.
        let deadline = Some(Instant::now() + Duration::from_millis(100));
        let status = with_deadline(deadline, async {
            Err::<(), _>(Status::new(Code::NotFound))
        })
        .await
        .unwrap_err();
        assert_eq!(status.code(), Code::NotFound);
    }

    #[tokio::test(start_paused = true)]
    async fn stream_ends_at_deadline() {
        let items = || {
            Streaming::new(futures_util::stream::unfold(0, |n| async move {
                tokio::time::sleep(Duration::from_millis(40)).await;
                (n < 4).then_some((Ok(n), n + 1))
            }))
        };
        let all: Vec<_> = stream_with_deadline(None, items())
            .try_collect()
            .await
            .unwrap();
        assert_eq!(all, [0, 1, 2, 3]);

        let deadline = Some(Instant::now() + Duration::from_secs(1));
        let all: Vec<_> = stream_with_deadline(deadline, items())
            .try_collect()
            .await
            .unwrap();
        assert_eq!(all, [0, 1, 2, 3]);

        let deadline = Some(Instant::now() + Duration::from_millis(100));
        let mut stream = stream_with_deadline(deadline, items());
        assert_eq!(stream.try_next().await.unwrap(), Some(0));
        assert_eq!(stream.try_next().await.unwrap(), Some(1));
        let status = stream.try_next().await.unwrap_err();
        assert_eq!(status.code(), Code::DeadlineExceeded);
        assert!(stream.next().await.is_none());
    }

    /// A stream whose items are always ready is cut at the deadline too,
    /// however slowly it is read.
    #[tokio::test(start_paused = true)]
    async fn ready_stream_ends_at_deadline() {
        let deadline = Some(Instant::now() + Duration::from_millis(100));
        let endless = Streaming::new(futures_util::stream::repeat_with(|| Ok(1)));
        let mut stream = stream_with_deadline(deadline, endless);
        let mut items = 0;
        let status = loop {
            match stream.try_next().await {
                Ok(Some(_)) => items += 1,
                Ok(None) => panic!("the stream ended"),
                Err(status) => break status,
            }
            tokio::time::sleep(Duration::from_millis(30)).await;
        };
        assert_eq!(status.code(), Code::DeadlineExceeded);
        assert_eq!(items, 4);
        assert!(stream.next().await.is_none());

        // Already expired: not polled at all.
        let endless = Streaming::new(futures_util::stream::repeat_with(|| Ok(1)));
        let mut stream = stream_with_deadline(Some(Instant::now()), endless);
        assert_eq!(
            stream.try_next().await.unwrap_err().code(),
            Code::DeadlineExceeded
        );
    }
}
