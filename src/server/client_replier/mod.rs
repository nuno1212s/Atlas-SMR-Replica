use std::ops::Deref;
use std::sync::Arc;

use atlas_common::channel;
use atlas_common::channel::sync::{ChannelSyncRx, ChannelSyncTx};
use atlas_common::node_id::NodeId;
use atlas_core::messages::ReplyMessage;
use atlas_smr_application::app::BatchReplies;
use atlas_smr_application::serialize::ApplicationData;
use atlas_smr_core::exec::{ReplyNode, RequestType};
use atlas_smr_core::SMRReply;

type RepliesType<S> = BatchReplies<S>;

///Dedicated thread to reply to clients
/// This is currently not being used (we are currently using the thread pool)
pub struct Replier<D, NT>
where
    D: ApplicationData + 'static,
{
    node_id: NodeId,
    channel: ChannelSyncRx<RepliesType<D::Reply>>,
    send_node: Arc<NT>,
}

pub struct ReplyHandle<D>
where
    D: ApplicationData,
{
    inner: ChannelSyncTx<RepliesType<D::Reply>>,
}

const REPLY_CHANNEL_SIZE: usize = 1024;

impl<D> ReplyHandle<D>
where
    D: ApplicationData,
{
    pub fn new(replier: ChannelSyncTx<RepliesType<D::Reply>>) -> Self {
        Self { inner: replier }
    }
}

impl<D> Deref for ReplyHandle<D>
where
    D: ApplicationData,
{
    type Target = ChannelSyncTx<RepliesType<D::Reply>>;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl<D> Clone for ReplyHandle<D>
where
    D: ApplicationData,
{
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
        }
    }
}

impl<D, NT: 'static> Replier<D, NT>
where
    D: ApplicationData + 'static,
{
    pub fn new(node_id: NodeId, send_node: Arc<NT>) -> ReplyHandle<D> {
        let (ch_tx, ch_rx) =
            channel::sync::new_bounded_sync(REPLY_CHANNEL_SIZE, Some("Reply channel work channel"));

        let reply_task = Self {
            node_id,
            channel: ch_rx,
            send_node,
        };

        let handle = ReplyHandle::new(ch_tx);

        //reply_task.start();

        handle
    }

    pub fn start(mut self)
    where
        NT: ReplyNode<SMRReply<D>>,
    {
        std::thread::Builder::new()
            .name(format!("{:?} // Reply thread", self.node_id))
            .spawn(move || {
                loop {
                    let reply_batch = self.channel.recv().unwrap();

                    let mut batch = reply_batch.into_inner();

                    batch.sort_unstable_by_key(|update_reply| update_reply.to());

                    // keep track of the last message and node id
                    // we iterated over
                    let mut curr_send = None;

                    for update_reply in batch {
                        let (peer_id, session_id, operation_id, payload) =
                            update_reply.into_inner();

                        // NOTE: the technique used here to peek the next reply is a
                        // hack... when we port this fix over to the production
                        // branch, perhaps we can come up with a better approach,
                        // but for now this will do
                        if let Some((message, last_peer_id)) = curr_send.take() {
                            let flush = peer_id != last_peer_id;

                            self.send_node
                                .send(RequestType::Ordered, message, last_peer_id, flush)
                                .expect("Failed to send");
                        }

                        // store previous reply message and peer id,
                        // for the next iteration
                        //TODO: Choose ordered or unordered reply
                        let message = ReplyMessage::new(session_id, operation_id, payload);

                        curr_send = Some((message, peer_id));
                    }

                    // deliver last reply
                    if let Some((message, last_peer_id)) = curr_send {
                        let _ =
                            self.send_node
                                .send(RequestType::Ordered, message, last_peer_id, true);
                    } else {
                        // slightly optimize code path;
                        // the previous if branch will always execute
                        // (there is always at least one request in the batch)
                        unreachable!();
                    }
                }
            })
            .expect("Failed to launch thread for client replier!");
    }
}
