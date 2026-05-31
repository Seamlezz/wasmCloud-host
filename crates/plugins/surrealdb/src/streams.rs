use std::pin::Pin;
use std::task::{Context, Poll};

use wasmtime::StoreContextMut;
use wasmtime::component::{Destination, StreamProducer, StreamResult};

use surrealdb_host_adapter::LiveAction;

type BindingLiveAction = super::bindings::seamlezz::surrealdb::call::LiveAction;
type BindingLiveEvent = super::bindings::seamlezz::surrealdb::call::LiveEvent;

pub fn map_live_action(action: LiveAction) -> BindingLiveAction {
    match action {
        LiveAction::Create => BindingLiveAction::Create,
        LiveAction::Update => BindingLiveAction::Update,
        LiveAction::Delete => BindingLiveAction::Delete,
        LiveAction::Killed => BindingLiveAction::Killed,
    }
}

pub fn to_binding_live_event(event: surrealdb_host_adapter::LiveEvent) -> BindingLiveEvent {
    BindingLiveEvent {
        subscription_id: event.subscription_id,
        query_id: event.query_id,
        action: map_live_action(event.action),
        data: event.data,
    }
}

pub struct LiveEventProducer {
    receiver: tokio::sync::mpsc::UnboundedReceiver<BindingLiveEvent>,
}

impl LiveEventProducer {
    pub fn new(receiver: tokio::sync::mpsc::UnboundedReceiver<BindingLiveEvent>) -> Self {
        Self { receiver }
    }
}

impl<T> StreamProducer<T> for LiveEventProducer {
    type Item = BindingLiveEvent;
    type Buffer = Option<Self::Item>;

    fn poll_produce<'a>(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        _store: StoreContextMut<'a, T>,
        mut destination: Destination<'a, Self::Item, Self::Buffer>,
        finish: bool,
    ) -> Poll<wasmtime::Result<StreamResult>> {
        if finish {
            return Poll::Ready(Ok(StreamResult::Cancelled));
        }

        let this = self.get_mut();
        match this.receiver.poll_recv(cx) {
            Poll::Ready(Some(event)) => {
                destination.set_buffer(Some(event));
                Poll::Ready(Ok(StreamResult::Completed))
            }
            Poll::Ready(None) => Poll::Ready(Ok(StreamResult::Dropped)),
            Poll::Pending => Poll::Pending,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn map_all_live_actions() {
        use surrealdb_host_adapter::LiveAction;
        assert!(matches!(
            map_live_action(LiveAction::Create),
            BindingLiveAction::Create
        ));
        assert!(matches!(
            map_live_action(LiveAction::Update),
            BindingLiveAction::Update
        ));
        assert!(matches!(
            map_live_action(LiveAction::Delete),
            BindingLiveAction::Delete
        ));
        assert!(matches!(
            map_live_action(LiveAction::Killed),
            BindingLiveAction::Killed
        ));
    }

    #[test]
    fn to_binding_live_event_preserves_fields() {
        let event = surrealdb_host_adapter::LiveEvent {
            subscription_id: 42,
            query_id: "q1".into(),
            action: surrealdb_host_adapter::LiveAction::Create,
            data: vec![1, 2, 3],
        };
        let binding = to_binding_live_event(event);
        assert_eq!(binding.subscription_id, 42);
        assert_eq!(binding.query_id, "q1");
        assert!(matches!(binding.action, BindingLiveAction::Create));
        assert_eq!(binding.data, vec![1, 2, 3]);
    }
}
