//! Cross-platform transport abstraction for Unix sockets and Windows Named Pipes.
//!
//! Provides [`BusListener`] and [`BusStream`] that work on both Linux/macOS
//! (using Unix domain sockets) and Windows (using Named Pipes).

use std::io;
use std::pin::Pin;
use std::task::{Context, Poll};

use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};

use crate::DataPlaneSocket;

/// Cross-platform listener that accepts incoming connections.
///
/// On Unix platforms, wraps `tokio::net::UnixListener`.
/// On Windows, manages a Named Pipe server instance with accept semantics.
pub struct BusListener {
    #[cfg(unix)]
    inner: tokio::net::UnixListener,
    #[cfg(windows)]
    inner: NamedPipeListener,
}

#[cfg(windows)]
struct NamedPipeListener {
    current: tokio::net::windows::named_pipe::NamedPipeServer,
    name: String,
}

impl BusListener {
    /// Bind to the specified socket.
    ///
    /// For Unix sockets, binds to the filesystem path.
    /// For Named Pipes, creates the pipe with the given name.
    ///
    /// Returns `io::ErrorKind::Unsupported` if attempting to bind a socket type
    /// not supported on the current platform.
    pub fn bind(socket: &DataPlaneSocket) -> io::Result<Self> {
        match socket {
            #[cfg(unix)]
            DataPlaneSocket::Unix { path } => {
                let listener = tokio::net::UnixListener::bind(path)?;
                Ok(Self { inner: listener })
            }
            #[cfg(not(unix))]
            DataPlaneSocket::Unix { .. } => Err(io::Error::new(
                io::ErrorKind::Unsupported,
                "Unix sockets not supported on this platform",
            )),
            #[cfg(windows)]
            DataPlaneSocket::NamedPipe { name } => {
                use tokio::net::windows::named_pipe::ServerOptions;
                let full_name = if name.starts_with(r"\\.\pipe\") {
                    name.clone()
                } else {
                    format!(r"\\.\pipe\{}", name)
                };
                let server = ServerOptions::new().first_pipe_instance(true).create(&full_name)?;
                Ok(Self {
                    inner: NamedPipeListener {
                        current: server,
                        name: full_name,
                    },
                })
            }
            #[cfg(not(windows))]
            DataPlaneSocket::NamedPipe { .. } => Err(io::Error::new(
                io::ErrorKind::Unsupported,
                "Named Pipes not supported on this platform",
            )),
        }
    }

    /// Accept an incoming connection.
    ///
    /// For Unix sockets, this is a straightforward accept.
    /// For Named Pipes, waits for a client to connect to the current pipe instance,
    /// then creates a new instance for the next connection.
    pub async fn accept(&mut self) -> io::Result<BusStream> {
        #[cfg(unix)]
        {
            let (stream, _) = self.inner.accept().await?;
            Ok(BusStream::Unix(stream))
        }
        #[cfg(windows)]
        {
            self.inner.current.connect().await?;
            // Swap out the connected pipe and create a new one for the next connection
            let connected = std::mem::replace(
                &mut self.inner.current,
                tokio::net::windows::named_pipe::ServerOptions::new().create(&self.inner.name)?,
            );
            Ok(BusStream::NamedPipe(NamedPipeStream::Server(connected)))
        }
        #[cfg(not(any(unix, windows)))]
        {
            Err(io::Error::new(
                io::ErrorKind::Unsupported,
                "No transport available on this platform",
            ))
        }
    }
}

/// Cross-platform bidirectional stream.
///
/// Implements `AsyncRead` and `AsyncWrite` by delegating to the underlying
/// platform-specific stream type.
pub enum BusStream {
    /// Unix domain socket stream (Linux, macOS, modern Windows).
    #[cfg(unix)]
    Unix(tokio::net::UnixStream),
    /// Windows Named Pipe stream (server or client).
    #[cfg(windows)]
    NamedPipe(NamedPipeStream),
}

/// Windows Named Pipe stream variant.
#[cfg(windows)]
pub enum NamedPipeStream {
    /// Server-side pipe that accepted a connection.
    Server(tokio::net::windows::named_pipe::NamedPipeServer),
    /// Client-side pipe that connected to a server.
    Client(tokio::net::windows::named_pipe::NamedPipeClient),
}

impl BusStream {
    /// Connect to the specified socket.
    ///
    /// For Unix sockets, connects to the filesystem path.
    /// For Named Pipes, opens a client connection with retry on PIPE_BUSY.
    pub async fn connect(socket: &DataPlaneSocket) -> io::Result<Self> {
        match socket {
            #[cfg(unix)]
            DataPlaneSocket::Unix { path } => {
                // Retry briefly on ConnectionRefused / NotFound — broker may still be
                // initializing the listener (mirror Windows NamedPipe PIPE_BUSY behavior).
                let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
                loop {
                    match tokio::net::UnixStream::connect(path).await {
                        Ok(stream) => return Ok(Self::Unix(stream)),
                        Err(e) if matches!(e.kind(), io::ErrorKind::ConnectionRefused | io::ErrorKind::NotFound) => {
                            if std::time::Instant::now() < deadline {
                                tokio::time::sleep(std::time::Duration::from_millis(50)).await;
                                continue;
                            }
                            return Err(e);
                        }
                        Err(e) => return Err(e),
                    }
                }
            }
            #[cfg(not(unix))]
            DataPlaneSocket::Unix { .. } => Err(io::Error::new(
                io::ErrorKind::Unsupported,
                "Unix sockets not supported on this platform",
            )),
            #[cfg(windows)]
            DataPlaneSocket::NamedPipe { name } => {
                use tokio::net::windows::named_pipe::ClientOptions;
                let full_name = if name.starts_with(r"\\.\pipe\") {
                    name.clone()
                } else {
                    format!(r"\\.\pipe\{}", name)
                };
                // Retry briefly on PIPE_BUSY (error 231)
                let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
                loop {
                    match ClientOptions::new().open(&full_name) {
                        Ok(client) => return Ok(Self::NamedPipe(NamedPipeStream::Client(client))),
                        Err(e) if e.raw_os_error() == Some(231) => {
                            // ERROR_PIPE_BUSY
                            if std::time::Instant::now() < deadline {
                                tokio::time::sleep(std::time::Duration::from_millis(50)).await;
                                continue;
                            }
                            return Err(e);
                        }
                        Err(e) => return Err(e),
                    }
                }
            }
            #[cfg(not(windows))]
            DataPlaneSocket::NamedPipe { .. } => Err(io::Error::new(
                io::ErrorKind::Unsupported,
                "Named Pipes not supported on this platform",
            )),
        }
    }
}

impl AsyncRead for BusStream {
    fn poll_read(self: Pin<&mut Self>, cx: &mut Context<'_>, buf: &mut ReadBuf<'_>) -> Poll<io::Result<()>> {
        match self.get_mut() {
            #[cfg(unix)]
            Self::Unix(stream) => Pin::new(stream).poll_read(cx, buf),
            #[cfg(windows)]
            Self::NamedPipe(NamedPipeStream::Server(pipe)) => Pin::new(pipe).poll_read(cx, buf),
            #[cfg(windows)]
            Self::NamedPipe(NamedPipeStream::Client(pipe)) => Pin::new(pipe).poll_read(cx, buf),
        }
    }
}

impl AsyncWrite for BusStream {
    fn poll_write(self: Pin<&mut Self>, cx: &mut Context<'_>, buf: &[u8]) -> Poll<io::Result<usize>> {
        match self.get_mut() {
            #[cfg(unix)]
            Self::Unix(stream) => Pin::new(stream).poll_write(cx, buf),
            #[cfg(windows)]
            Self::NamedPipe(NamedPipeStream::Server(pipe)) => Pin::new(pipe).poll_write(cx, buf),
            #[cfg(windows)]
            Self::NamedPipe(NamedPipeStream::Client(pipe)) => Pin::new(pipe).poll_write(cx, buf),
        }
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        match self.get_mut() {
            #[cfg(unix)]
            Self::Unix(stream) => Pin::new(stream).poll_flush(cx),
            #[cfg(windows)]
            Self::NamedPipe(NamedPipeStream::Server(pipe)) => Pin::new(pipe).poll_flush(cx),
            #[cfg(windows)]
            Self::NamedPipe(NamedPipeStream::Client(pipe)) => Pin::new(pipe).poll_flush(cx),
        }
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        match self.get_mut() {
            #[cfg(unix)]
            Self::Unix(stream) => Pin::new(stream).poll_shutdown(cx),
            #[cfg(windows)]
            Self::NamedPipe(NamedPipeStream::Server(pipe)) => Pin::new(pipe).poll_shutdown(cx),
            #[cfg(windows)]
            Self::NamedPipe(NamedPipeStream::Client(pipe)) => Pin::new(pipe).poll_shutdown(cx),
        }
    }
}

/// Clean up socket resources.
///
/// For Unix sockets, removes the filesystem path (ignoring NotFound errors).
/// For Named Pipes, this is a no-op (Windows cleans up automatically).
pub fn cleanup(socket: &DataPlaneSocket) {
    match socket {
        DataPlaneSocket::Unix { path } => {
            let _ = std::fs::remove_file(path);
        }
        DataPlaneSocket::NamedPipe { .. } => {
            // Named Pipes are automatically cleaned up by Windows
        }
    }
}

/// Compute the conventional [`DataPlaneSocket`] for an app server.
///
/// - **Unix**: derives `<parent of $TOKIMO_BUS_SOCKET>/apps/<service>.sock` and
///   removes any stale socket file at that path. Requires `TOKIMO_BUS_SOCKET`
///   to be set (the broker socket the supervisor handed us).
/// - **Windows**: returns a [`DataPlaneSocket::NamedPipe`] with name
///   `tokimo-app-<service>-<pid>` to avoid collisions across instances.
///
/// Apps that follow the standard layout should not implement this themselves
/// — call [`BusListener::bind_for_app`] which composes this with a bind.
pub fn app_socket(service: &str) -> io::Result<DataPlaneSocket> {
    #[cfg(unix)]
    {
        let bus = std::env::var("TOKIMO_BUS_SOCKET")
            .map_err(|_| io::Error::new(io::ErrorKind::NotFound, "TOKIMO_BUS_SOCKET not set"))?;
        let parent = std::path::PathBuf::from(&bus)
            .parent()
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "TOKIMO_BUS_SOCKET has no parent"))?
            .to_path_buf();
        let apps_dir = parent.join("apps");
        std::fs::create_dir_all(&apps_dir)?;
        let path = apps_dir.join(format!("{service}.sock"));
        // Remove any leftover socket file from a previous run.
        let _ = std::fs::remove_file(&path);
        Ok(DataPlaneSocket::Unix {
            path: path.to_string_lossy().into_owned(),
        })
    }
    #[cfg(windows)]
    {
        Ok(DataPlaneSocket::NamedPipe {
            name: format!("tokimo-app-{service}-{}", std::process::id()),
        })
    }
    #[cfg(not(any(unix, windows)))]
    {
        let _ = service;
        Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "no transport available on this platform",
        ))
    }
}

impl BusListener {
    /// Bind the conventional app-server listener for `service`.
    ///
    /// Combines [`app_socket`] with [`BusListener::bind`] so apps don't need
    /// any `#[cfg(unix)] / #[cfg(windows)]` boilerplate. Returns both the
    /// listener and the socket descriptor (the latter is what the app reports
    /// back to the broker via the supervisor protocol).
    pub fn bind_for_app(service: &str) -> io::Result<(Self, DataPlaneSocket)> {
        let socket = app_socket(service)?;
        let listener = Self::bind(&socket)?;
        Ok((listener, socket))
    }
}
