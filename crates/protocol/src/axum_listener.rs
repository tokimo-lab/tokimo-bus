//! [`axum::serve::Listener`] impl for [`BusListener`].
//!
//! Enables `axum::serve(listener, router)` directly on a bus data-plane
//! listener, so apps don't need manual accept loops or hyper boilerplate.

use std::io;
use std::time::Duration;

use crate::{BusListener, BusStream};

impl axum::serve::Listener for BusListener {
    type Io = BusStream;
    type Addr = ();

    async fn accept(&mut self) -> (Self::Io, Self::Addr) {
        loop {
            match BusListener::accept(self).await {
                Ok(stream) => return (stream, ()),
                Err(e) => {
                    tracing::error!(error = %e, "bus-listener: accept error");
                    tokio::time::sleep(Duration::from_secs(1)).await;
                }
            }
        }
    }

    fn local_addr(&self) -> io::Result<Self::Addr> {
        Ok(())
    }
}
