use std::marker::PhantomData;
use std::time::Instant;

use atlas_common::channel::{ChannelSyncRx, ChannelSyncTx};
use atlas_common::error::*;
use atlas_common::ordering::Orderable;
use atlas_communication::FullNetworkNode;
use atlas_communication::NetworkNode;
use atlas_core::log_transfer::LogTransferProtocol;
use atlas_core::ordering_protocol::loggable::LoggableOrderProtocol;
use atlas_core::ordering_protocol::PermissionedOrderingProtocol;
use atlas_core::ordering_protocol::reconfigurable_order_protocol::ReconfigurableOrderProtocol;
use atlas_core::persistent_log::{DivisibleStateLog, PersistableStateTransferProtocol};
use atlas_core::reconfiguration_protocol::ReconfigurationProtocol;
use atlas_core::smr::networking::SMRNetworkNode;
use atlas_core::smr::smr_decision_log::DecisionLog;
use atlas_core::state_transfer::divisible_state::DivisibleStateTransfer;
use atlas_smr_application::app::Application;
use atlas_smr_application::state::divisible_state::{AppState, AppStateMessage, DivisibleState, InstallStateMessage};
use atlas_metrics::metrics::metric_duration;
use atlas_smr_execution::TDivisibleStateExecutor;

use crate::config::DivisibleStateReplicaConfig;
use crate::metric::RUN_LATENCY_TIME_ID;
use crate::persistent_log::SMRPersistentLog;
use crate::server::Replica;

pub struct DivStReplica<RP, SE, S, A, OP, DL, ST, LT, NT, PL>
    where RP: ReconfigurationProtocol + 'static,
          S: DivisibleState + 'static,
          A: Application<S> + Send + 'static,
          OP: LoggableOrderProtocol<A::AppData, NT> + ReconfigurableOrderProtocol<RP::Serialization> + 'static,
          DL: DecisionLog<A::AppData, OP, NT, PL> + 'static,
          ST: DivisibleStateTransfer<S, NT, PL> + PersistableStateTransferProtocol + 'static,
          LT: LogTransferProtocol<A::AppData, OP, DL, NT, PL> + 'static,
          PL: SMRPersistentLog<A::AppData, OP::Serialization, OP::StateSerialization, OP::PermissionedSerialization> + 'static + DivisibleStateLog<S>,
{
    p: PhantomData<(A, SE)>,
    /// The inner replica object, responsible for the general replica things
    inner_replica: Replica<RP, S, A::AppData, OP, DL, ST, LT, NT, PL>,

    state_tx: ChannelSyncTx<InstallStateMessage<S>>,
    checkpoint_rx: ChannelSyncRx<AppStateMessage<S>>,
    /// State transfer protocols
    state_transfer_protocol: ST,
}

impl<RP, SE, S, A, OP, DL, ST, LT, NT, PL> DivStReplica<RP, SE, S, A, OP, DL, ST, LT, NT, PL> where
    RP: ReconfigurationProtocol + 'static,
    SE: TDivisibleStateExecutor<A, S, NT> + 'static,
    S: DivisibleState + Send + 'static,
    A: Application<S> + Send + 'static,
    OP: LoggableOrderProtocol<A::AppData, NT> + PermissionedOrderingProtocol + ReconfigurableOrderProtocol<RP::Serialization> + Send + 'static,
    DL: DecisionLog<A::AppData, OP, NT, PL> + 'static,
    LT: LogTransferProtocol<A::AppData, OP, DL, NT, PL> + 'static,
    ST: DivisibleStateTransfer<S, NT, PL> + PersistableStateTransferProtocol + Send + 'static,
    PL: SMRPersistentLog<A::AppData, OP::Serialization, OP::StateSerialization, OP::PermissionedSerialization> + DivisibleStateLog<S> + 'static,
    NT: SMRNetworkNode<RP::InformationProvider, RP::Serialization, A::AppData, OP::Serialization, ST::Serialization, LT::Serialization> + 'static, {
    pub async fn bootstrap(cfg: DivisibleStateReplicaConfig<RP, S, A, OP, DL, ST, LT, NT, PL>) -> Result<Self> {
        let DivisibleStateReplicaConfig {
            service, replica_config, st_config
        } = cfg;

        let (executor_handle, executor_receiver) = SE::init_handle();

        let inner_replica = Replica::<RP, S, A::AppData, OP, DL, ST, LT, NT, PL>::bootstrap(replica_config, executor_handle.clone()).await?;

        let node = inner_replica.node.clone();

        let (state_tx, checkpoint_rx) =
            SE::init(executor_receiver, None, service, node.clone())?;

        let state_transfer_protocol = ST::initialize(st_config, inner_replica.timeouts.clone(),
                                                     node.clone(), inner_replica.persistent_log.clone(),
                                                     state_tx.clone())?;

        let view = inner_replica.ordering_protocol.view();

        let mut replica = Self {
            p: Default::default(),
            inner_replica,
            state_tx,
            checkpoint_rx,
            state_transfer_protocol,
        };

        replica.state_transfer_protocol.request_latest_state(view)?;

        Ok(replica)
    }

    pub fn run(&mut self) -> Result<()> {
        let mut last_loop = Instant::now();

        loop {
            self.receive_checkpoints()?;

            self.inner_replica.run(&mut self.state_transfer_protocol)?;

            metric_duration(RUN_LATENCY_TIME_ID, last_loop.elapsed());

            last_loop = Instant::now();
        }
    }

    fn receive_checkpoints(&mut self) -> Result<()> {
        while let Ok(checkpoint) = self.checkpoint_rx.try_recv() {
            let (seq_no, state) = checkpoint.into_state();

            let view = self.inner_replica.ordering_protocol.view();

            match state {
                AppState::StateDescriptor(descriptor) => {
                    self.state_transfer_protocol.handle_state_desc_received_from_app(view, descriptor)?;
                }
                AppState::StatePart(parts) => {
                    self.state_transfer_protocol.handle_state_part_received_from_app(view, parts.into_vec())?;
                }
                AppState::Done => {
                    self.state_transfer_protocol.handle_state_finished_reception(view)?;
                }
            }

            self.inner_replica.decision_log.state_checkpoint(seq_no)?;
        }

        Ok(())
    }
}