//! Shared helpers for adopting sockets passed in by a socket-activation manager
//! (e.g. systemd) via the `LISTEN_FDS` / `LISTEN_PID` protocol.

use std::os::unix::io::{BorrowedFd, RawFd};

/// First descriptor handed to the process by the socket-activation manager.
const SD_LISTEN_FDS_START: RawFd = 3;

/// Searches the descriptors passed by the socket-activation manager and returns
/// the first one that is a listening stream socket and is accepted by `matches`.
///
/// `matches` inspects a borrowed descriptor and must not take ownership; the
/// caller is responsible for adopting the returned descriptor.
pub(super) fn find_preallocated_fd<F>(matches: F) -> Option<RawFd>
where
    F: Fn(RawFd) -> bool,
{
    let listen_pid: u32 = std::env::var("LISTEN_PID").ok()?.parse().ok()?;
    if listen_pid != std::process::id() {
        return None;
    }

    let n_fds: i32 = std::env::var("LISTEN_FDS").ok()?.parse().ok()?;
    let end = SD_LISTEN_FDS_START.checked_add(n_fds)?;

    (SD_LISTEN_FDS_START..end).find(|&fd| is_listening_stream_socket(fd) && matches(fd))
}

/// Returns `true` if `fd` refers to a stream socket in the listening state.
pub(super) fn is_listening_stream_socket(fd: RawFd) -> bool {
    // Borrow the fd without taking ownership so the socket is not closed here.
    let borrowed = unsafe { BorrowedFd::borrow_raw(fd) };
    let sock = socket2::SockRef::from(&borrowed);

    matches!(sock.r#type(), Ok(socket2::Type::STREAM)) && matches!(sock.is_listener(), Ok(true))
}
