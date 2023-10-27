use std::sync::Arc;
use log::error;
use atlas_common::{channel, threadpool};
use atlas_common::channel::{ChannelSyncRx, ChannelSyncTx};
use atlas_common::error::*;
use atlas_common::globals::ReadOnly;
use atlas_common::ordering::{Orderable, SeqNo};
use atlas_core::ordering_protocol::networking::serialize::NetworkView;
use atlas_core::persistent_log::MonolithicStateLog;
use atlas_core::state_transfer::Checkpoint;
use atlas_core::state_transfer::monolithic_state::MonolithicStateTransfer;
use atlas_core::timeouts::Timeouts;
use atlas_smr_application::state::monolithic_state::{AppStateMessage, digest_state, InstallStateMessage, MonolithicState};
use crate::server::state_transfer::{StateTransferMngr, StateTransferThreadHandle, StateTransferThreadInnerHandle};

pub struct MonStateTransfer<V, S, NT, PL, ST>
    where V: NetworkView,
          S: MonolithicState + 'static,
          ST: MonolithicStateTransfer<S, NT, PL>,
          PL: MonolithicStateLog<S>
{
    inner_state: StateTransferMngr<V, S, NT, PL, ST>,

    state_tx_to_executor: ChannelSyncTx<InstallStateMessage<S>>,
    checkpoint_rx_from_app: ChannelSyncRx<AppStateMessage<S>>,
    digested_state: (ChannelSyncTx<Arc<ReadOnly<Checkpoint<S>>>>, ChannelSyncRx<Arc<ReadOnly<Checkpoint<S>>>>),

    state_transfer_protocol: ST,
}

impl<V, S, NT, PL, ST> MonStateTransfer<V, S, NT, PL, ST>
    where V: NetworkView,
          S: MonolithicState + 'static,
          ST: MonolithicStateTransfer<S, NT, PL>,
          PL: MonolithicStateLog<S>
{
    pub fn init_state_transfer_thread(state_tx: ChannelSyncTx<InstallStateMessage<S>>,
                                      checkpoint_rx: ChannelSyncRx<AppStateMessage<S>>,
                                      st_config: ST::Config,
                                      node: Arc<NT>,
                                      timeouts: Timeouts,
                                      persistent_log: PL,
                                      handle: StateTransferThreadInnerHandle<V, ST::Serialization>)  {

        let inner_mngr = StateTransferMngr::initialize_core_state_transfer(handle)
            .expect("Failed to initialize state transfer inner layer");

        let digest_app_state = channel::new_bounded_sync(5, Some("Digested App State Channel"));

        let state_transfer_protocol = ST::initialize(st_config, timeouts,
                                                     node, persistent_log,
                                                     state_tx.clone()).expect("Failed to init state transfer protocol");

        let state_transfer_manager = Self {
            inner_state: inner_mngr,
            state_tx_to_executor: state_tx,
            checkpoint_rx_from_app: checkpoint_rx,
            digested_state: digest_app_state,
            state_transfer_protocol,
        };

        std::thread::Builder::new()
            .name(String::from("State transfer thread"))
            .spawn(move || {
                loop {
                    if let Err(err) = state_transfer_manager.run() {
                        error!("Received state transfer error {:?}", err);
                    }
                }
            })
            .expect("Failed to allocate the state transfer thread");
    }

    pub fn run(self) -> Result<()> {

        Ok(())
    }

    fn receive_checkpoints(&mut self) -> Result<()> {
        while let Ok(checkpoint) = self.checkpoint_rx_from_app.try_recv() {
            self.execution_finished_with_appstate(checkpoint.seq(), checkpoint.into_state())?;
        }

        Ok(())
    }

    /// receive digested checkpoints from the threadpoll and pass them to the state transfer protocol
    fn receive_digested_checkpoints(&mut self) -> Result<()> {
        while let Ok(checkpoint) = self.digested_state.1.try_recv() {
            self.state_transfer_protocol.handle_state_received_from_app(checkpoint.clone())?;
            self.inner_state.notify_of_checkpoint(checkpoint.sequence_number());
        }

        Ok(())
    }

    /// handle the execution being finished with the app state
    fn execution_finished_with_appstate(&mut self, seq: SeqNo, appstate: S) -> Result<()> {
        let return_tx = self.digested_state.0.clone();

        // Digest the app state before passing it on to the ordering protocols
        threadpool::execute(move || {
            let result = digest_state(&appstate);

            match result {
                Ok(digest) => {
                    let checkpoint = Checkpoint::new(seq, appstate, digest);

                    return_tx.send(checkpoint).unwrap();
                }
                Err(error) => {
                    error!("Failed to serialize and digest application state: {:?}", error)
                }
            }
        });

        Ok(())
    }
}