//! Length-prefixed `rmp-serde` frame codec.

use serde::{Serialize, de::DeserializeOwned};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

use crate::frame::BusError;

/// Maximum single-frame payload (32 MiB). Protects against malformed senders.
pub const MAX_FRAME_BYTES: u32 = 32 * 1024 * 1024;

/// Serialize `value` with `rmp-serde` and write as `[u32 BE len][payload]`.
pub async fn write_frame<W, T>(w: &mut W, value: &T) -> Result<(), BusError>
where
    W: AsyncWrite + Unpin,
    T: Serialize,
{
    let bytes = rmp_serde::to_vec_named(value).map_err(|e| BusError::Codec(format!("encode: {e}")))?;
    if bytes.len() > MAX_FRAME_BYTES as usize {
        return Err(BusError::FrameTooLarge {
            size: bytes.len() as u64,
            max: MAX_FRAME_BYTES,
        });
    }
    let len = bytes.len() as u32;
    w.write_all(&len.to_be_bytes()).await?;
    w.write_all(&bytes).await?;
    w.flush().await?;
    Ok(())
}

/// Read exactly one frame. Fails (not `Ok(None)`) on any I/O error including
/// EOF mid-frame — use [`read_frame_opt`] if a clean EOF at the start is
/// acceptable.
pub async fn read_frame<R, T>(r: &mut R) -> Result<T, BusError>
where
    R: AsyncRead + Unpin,
    T: DeserializeOwned,
{
    match read_frame_opt(r).await? {
        Some(v) => Ok(v),
        None => Err(BusError::ConnectionClosed),
    }
}

/// Try to read a frame; returns `Ok(None)` on clean EOF before any bytes.
pub async fn read_frame_opt<R, T>(r: &mut R) -> Result<Option<T>, BusError>
where
    R: AsyncRead + Unpin,
    T: DeserializeOwned,
{
    let mut len_buf = [0u8; 4];
    match r.read_exact(&mut len_buf).await {
        Ok(_) => {}
        Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => return Ok(None),
        Err(e) => return Err(e.into()),
    }
    let len = u32::from_be_bytes(len_buf);
    if len > MAX_FRAME_BYTES {
        return Err(BusError::FrameTooLarge {
            size: u64::from(len),
            max: MAX_FRAME_BYTES,
        });
    }
    let mut buf = vec![0u8; len as usize];
    r.read_exact(&mut buf).await?;
    let value = rmp_serde::from_slice::<T>(&buf).map_err(|e| BusError::Codec(format!("decode: {e}")))?;
    Ok(Some(value))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::frame::{BusFrame, HelloRequest, HttpMethod, MethodDecl, ProtocolVersion};
    use tokio::io::duplex;

    #[tokio::test]
    async fn roundtrip_hello() {
        let (mut a, mut b) = duplex(64 * 1024);
        let hello = BusFrame::Hello(HelloRequest {
            protocol: ProtocolVersion::CURRENT,
            service: "helloworld".into(),
            version: "0.1.0".into(),
            pid: 1234,
            auth_token: "tok".into(),
            methods: vec![MethodDecl {
                name: "echo".into(),
                requires_auth: false,
                streaming: false,
                http_method: HttpMethod::Post,
                path: None,
                description: None,
            }],
            events: vec![],
            data_plane: None,
        });

        tokio::spawn(async move {
            write_frame(&mut a, &hello).await.unwrap();
        });

        let back: BusFrame = read_frame(&mut b).await.unwrap();
        match back {
            BusFrame::Hello(h) => assert_eq!(h.service, "helloworld"),
            other => panic!("unexpected frame: {other:?}"),
        }
    }

    #[tokio::test]
    async fn clean_eof_returns_none() {
        let (a, mut b) = duplex(8);
        drop(a);
        let out: Option<BusFrame> = read_frame_opt(&mut b).await.unwrap();
        assert!(out.is_none());
    }
}
