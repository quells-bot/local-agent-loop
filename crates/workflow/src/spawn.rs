use std::cell::RefCell;
use std::future::Future;
use std::pin::Pin;
use std::rc::Rc;
use std::task::{Context as TaskContext, Poll};

/// Awaitable handle for a detached `ctx.spawn` branch (the `workflow.Go` analog,
/// spec §4.4). Resolves to the branch's output once it completes. The branch writes
/// its result into the shared slot; the handle takes it.
pub struct SpawnHandle<T> {
    pub(crate) slot: Rc<RefCell<Option<T>>>,
}

impl<T> Future for SpawnHandle<T> {
    type Output = T;

    fn poll(self: Pin<&mut Self>, _cx: &mut TaskContext<'_>) -> Poll<T> {
        match self.slot.borrow_mut().take() {
            Some(v) => Poll::Ready(v),
            None => Poll::Pending,
        }
    }
}
