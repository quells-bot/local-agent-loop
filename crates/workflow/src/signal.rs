use std::future::Future;
use std::marker::PhantomData;
use std::pin::Pin;
use std::rc::Rc;
use std::task::{Context as TaskContext, Poll};

use serde::de::DeserializeOwned;

use crate::context::ContextInner;

/// Idempotent-by-name signal channel (the `workflow.GetSignalChannel` analog,
/// spec §6.3). Holds no buffer of its own — it reads/writes the shared per-name
/// buffer in `ContextInner`, so two channels for the same name are the same logical
/// channel. Allocates no command and consumes no `seq`.
pub struct SignalChannel<T> {
    inner: Rc<ContextInner>,
    name: String,
    _marker: PhantomData<fn() -> T>,
}

impl<T> SignalChannel<T> {
    pub(crate) fn new(inner: Rc<ContextInner>, name: String) -> Self {
        Self {
            inner,
            name,
            _marker: PhantomData,
        }
    }

    /// Await one buffered signal of this name (`ReceiveChannel.Receive` analog).
    /// Resolves by popping the front of the per-name buffer; the Nth `recv()`
    /// deterministically pops the Nth buffered signal, so it is replay-stable
    /// without a `seq` (spec §6.3).
    pub fn recv(&self) -> SignalRecv<T> {
        SignalRecv {
            inner: self.inner.clone(),
            name: self.name.clone(),
            _marker: PhantomData,
        }
    }
}

/// Future returned by [`SignalChannel::recv`]. Pops one payload off the per-name
/// buffer and deserializes it to `T`; parks (Pending) while the buffer is empty.
pub struct SignalRecv<T> {
    inner: Rc<ContextInner>,
    name: String,
    _marker: PhantomData<fn() -> T>,
}

impl<T: DeserializeOwned> Future for SignalRecv<T> {
    type Output = Result<T, crate::Error>;

    fn poll(self: Pin<&mut Self>, _cx: &mut TaskContext<'_>) -> Poll<Self::Output> {
        // No waker registration: the driver re-polls after applying each event under
        // the one-event-per-turn rule (§4.1), so `recv` never needs to wake itself —
        // identical to the activity/timer futures.
        let me = self.get_mut();
        let popped = me
            .inner
            .signals
            .borrow_mut()
            .get_mut(&me.name)
            .and_then(|buf| buf.pop_front());
        match popped {
            Some(bytes) => {
                Poll::Ready(serde_json::from_slice::<T>(&bytes).map_err(|e| {
                    crate::Error::new(format!("signal '{}' deserialize: {e}", me.name))
                }))
            }
            None => Poll::Pending,
        }
    }
}
