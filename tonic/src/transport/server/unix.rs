use super::Connected;
use std::sync::Arc;
use std::{
    os::unix::net::UnixListener as StdUnixListener,
    path::Path,
    pin::Pin,
    task::{Context, Poll},
};

use tokio::net::{UnixListener, UnixStream};
use tokio_stream::{Stream, wrappers::UnixListenerStream};

/// Binds a Unix domain socket for a [Router](super::super::Router).
///
/// An incoming stream, usable with
/// [Router::serve_with_incoming](super::super::Router::serve_with_incoming), of
/// `AsyncRead + AsyncWrite` that communicate with clients that connect to a
/// Unix domain socket path.
#[derive(Debug)]
pub struct UnixIncoming {
    inner: UnixListenerStream,
}

impl UnixIncoming {
    /// Creates an instance by binding (opening) the specified socket path.
    ///
    /// Returns a `UnixIncoming` if the socket path was successfully bound.
    ///
    /// If the process was launched under a socket-activation manager
    /// that passed a listening Unix socket matching `path` via the
    /// `LISTEN_FDS` / `LISTEN_PID` environment variables, that inherited
    /// descriptor is adopted instead of opening a new socket.
    ///
    /// # Examples
    /// ```no_run
    /// # use tower_service::Service;
    /// # use http::{request::Request, response::Response};
    /// # use tonic::{body::Body, server::NamedService, transport::{Server, server::UnixIncoming}};
    /// # use core::convert::Infallible;
    /// # use std::error::Error;
    /// # fn main() { }
    /// # fn run<S>(some_service: S) -> Result<(), Box<dyn Error + Send + Sync>>
    /// # where
    /// #   S: Service<Request<Body>, Response = Response<Body>, Error = Infallible> + NamedService + Clone + Send + Sync + 'static,
    /// #   S::Future: Send + 'static,
    /// # {
    /// let uinc = UnixIncoming::bind("/tmp/tonic/helloworld")?;
    /// Server::builder()
    ///    .add_service(some_service)
    ///    .serve_with_incoming(uinc);
    /// # Ok(())
    /// # }
    /// ```
    pub fn bind(path: impl AsRef<Path>) -> std::io::Result<Self> {
        let path = path.as_ref();
        let std_listener = match find_preallocated_fd(path) {
            Some(listener) => listener,
            None => StdUnixListener::bind(path)?,
        };

        std_listener.set_nonblocking(true)?;

        Ok(UnixListener::from_std(std_listener)?.into())
    }

    /// Returns the local address that this Unix incoming is bound to.
    pub fn local_addr(&self) -> std::io::Result<tokio::net::unix::SocketAddr> {
        self.inner.as_ref().local_addr()
    }
}

impl From<UnixListener> for UnixIncoming {
    fn from(listener: UnixListener) -> Self {
        Self {
            inner: UnixListenerStream::new(listener),
        }
    }
}

impl Stream for UnixIncoming {
    type Item = std::io::Result<UnixStream>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        Pin::new(&mut self.inner).poll_next(cx)
    }
}

fn find_preallocated_fd(path: &Path) -> Option<StdUnixListener> {
    use std::os::unix::io::FromRawFd;

    let fd = super::socket_activation::find_preallocated_fd(|fd| unix_fd_matches(fd, path))?;

    Some(unsafe { StdUnixListener::from_raw_fd(fd) })
}

fn unix_fd_matches(fd: std::os::unix::io::RawFd, requested: &Path) -> bool {
    use std::mem::ManuallyDrop;
    use std::os::unix::io::FromRawFd;

    let listener = ManuallyDrop::new(unsafe { StdUnixListener::from_raw_fd(fd) });
    matches!(listener.local_addr(), Ok(addr) if addr.as_pathname() == Some(requested))
}

/// Connection info for Unix domain socket streams.
///
/// This type will be accessible through [request extensions][ext] if you're using
/// a unix stream.
///
/// See [Connected] for more details.
///
/// [ext]: crate::Request::extensions
#[derive(Clone, Debug)]
pub struct UdsConnectInfo {
    /// Peer address. This will be "unnamed" for client unix sockets.
    pub peer_addr: Option<Arc<tokio::net::unix::SocketAddr>>,
    /// Process credentials for the unix socket.
    pub peer_cred: Option<tokio::net::unix::UCred>,
}

impl Connected for tokio::net::UnixStream {
    type ConnectInfo = UdsConnectInfo;

    fn connect_info(&self) -> Self::ConnectInfo {
        UdsConnectInfo {
            peer_addr: self.peer_addr().ok().map(Arc::new),
            peer_cred: self.peer_cred().ok(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::UnixIncoming;
    use serial_test::serial;
    use std::os::unix::net::UnixListener as StdUnixListener;
    use std::path::PathBuf;

    fn temp_socket_path(tag: &str) -> PathBuf {
        let mut path = std::env::temp_dir();
        path.push(format!(
            "tonic-uds-test-{}-{}.sock",
            tag,
            std::process::id()
        ));
        let _ = std::fs::remove_file(&path);
        path
    }

    #[test]
    #[serial]
    fn unix_fd_matches_cases() {
        use super::unix_fd_matches;
        use std::os::unix::io::AsRawFd;

        let path = temp_socket_path("matches");
        let other = temp_socket_path("matches-other");

        let listener = StdUnixListener::bind(&path).unwrap();
        let fd = listener.as_raw_fd();

        assert!(unix_fd_matches(fd, &path));
        assert!(!unix_fd_matches(fd, &other));

        drop(listener);
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    #[serial]
    fn is_listening_stream_socket_cases() {
        use crate::transport::server::socket_activation::is_listening_stream_socket;
        use std::os::unix::io::AsRawFd;

        let path = temp_socket_path("listening");

        let listener = StdUnixListener::bind(&path).unwrap();
        assert!(is_listening_stream_socket(listener.as_raw_fd()));

        let dgram = std::os::unix::net::UnixDatagram::unbound().unwrap();
        assert!(!is_listening_stream_socket(dgram.as_raw_fd()));

        let raw = socket2::Socket::new(socket2::Domain::UNIX, socket2::Type::STREAM, None).unwrap();
        assert!(!is_listening_stream_socket(raw.as_raw_fd()));

        drop(listener);
        let _ = std::fs::remove_file(&path);
    }

    #[tokio::test]
    #[serial]
    async fn socket_activation_uses_preallocated_fd() {
        use std::os::unix::io::IntoRawFd;

        const SD_FD: libc::c_int = 3;

        let path = temp_socket_path("activation");

        let pre_listener = StdUnixListener::bind(&path).unwrap();
        let pre_fd = pre_listener.into_raw_fd();

        let saved_fd = unsafe { libc::dup(SD_FD) };
        unsafe {
            libc::dup2(pre_fd, SD_FD);
            libc::close(pre_fd);
        }

        unsafe {
            std::env::set_var("LISTEN_PID", std::process::id().to_string());
            std::env::set_var("LISTEN_FDS", "1");
        }

        let incoming = UnixIncoming::bind(&path).unwrap();
        assert_eq!(
            incoming.local_addr().unwrap().as_pathname(),
            Some(path.as_path())
        );
        drop(incoming);

        unsafe {
            std::env::remove_var("LISTEN_PID");
            std::env::remove_var("LISTEN_FDS");
        }

        unsafe {
            if saved_fd >= 0 {
                libc::dup2(saved_fd, SD_FD);
                libc::close(saved_fd);
            } else {
                libc::close(SD_FD);
            }
        }

        let _ = std::fs::remove_file(&path);
    }
}
