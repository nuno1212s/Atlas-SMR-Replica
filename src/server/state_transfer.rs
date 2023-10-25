use atlas_common::channel::{ChannelSyncRx, ChannelSyncTx, OneShotTx};
use atlas_common::ordering::SeqNo;
use atlas_communication::message::StoredMessage;
use atlas_core::ordering_protocol::ExecutionResult;
use atlas_core::ordering_protocol::networking::serialize::NetworkView;
use atlas_core::state_transfer::{StateTransferProtocol, STMsg, STResult};
use atlas_core::timeouts::RqTimeout;

pub enum StateTransferWorkMessage<V, ST> where V: NetworkView {
    RequestLatestState(V),
    StateTransferMessage(V, StoredMessage<ST>),
    Timeout(V, Vec<RqTimeout>),
    ShouldRequestAppState(V, SeqNo, OneShotTx<ExecutionResult>),
}

pub enum StateTransferProgress {
    StateTransferProgress(STResult),
}

pub struct StateTransferHandle<V, S, NT, PL, ST>
    where ST: StateTransferProtocol<S, NT, PL>,
          V: NetworkView {
    work_rx: ChannelSyncRx<StateTransferWorkMessage<V, STMsg<ST::Serialization>>>,
    response_tx: ChannelSyncTx<StateTransferProgress>,
    currently_running: bool,
}

impl<V, S, NT, PL, ST> StateTransferHandle<V, S, NT, PL, ST>
    where ST: StateTransferProtocol<S, NT, PL>,
          V: NetworkView {
    pub fn iterate(&mut self, state_transfer: &mut ST) {
        while let Ok(work) = self.work_rx.try_recv() {
            match work {
                StateTransferWorkMessage::ShouldRequestAppState(view, seq, response) =>
                    Self::should_request_app_state(view, seq, state_transfer, response),
                StateTransferWorkMessage::RequestLatestState(view) => {
                    self.currently_running = true;

                    Self::request_latest_state(view, state_transfer);
                }
                StateTransferWorkMessage::StateTransferMessage(view, message) => {
                    if self.currently_running {
                        let result = state_transfer.process_message(view, message);

                        if let Ok(st_result) = result {
                            let _ = self.response_tx.send(StateTransferProgress::StateTransferProgress(st_result));
                        }
                    } else {
                        let _ = state_transfer.handle_off_ctx_message(view, message);
                    }
                }
                StateTransferWorkMessage::Timeout(view, timeout) => {}
            }
        }
    }

    fn request_latest_state(view: V, state_transfer: &mut ST) {
        let _ = state_transfer.request_latest_state(view);
    }

    fn should_request_app_state(view: V, seq: SeqNo, state_transfer: &mut ST, result: OneShotTx<ExecutionResult>) {
        let appstate_res = state_transfer.handle_app_state_requested(view, seq);

        if let Ok(appstate) = appstate_res {
            let _ = result.send(appstate);
        } else {
            let _ = result.send(ExecutionResult::Nil);
        }
    }
}