use std::marker::PhantomData;
use std::time::Instant;

use atlas_common::error::*;
use atlas_common::ordering::Orderable;
use atlas_core::ordering_protocol::loggable::LoggableOrderProtocol;
use atlas_core::ordering_protocol::permissioned::ViewTransferProtocol;
use atlas_core::ordering_protocol::PermissionedOrderingProtocol;
use atlas_core::ordering_protocol::reconfigurable_order_protocol::ReconfigurableOrderProtocol;
use atlas_core::persistent_log::PersistableStateTransferProtocol;
use atlas_core::reconfiguration_protocol::ReconfigurationProtocol;
use atlas_logging_core::decision_log::DecisionLog;
use atlas_logging_core::log_transfer::LogTransferProtocol;
use atlas_metrics::metrics::metric_duration;
use atlas_smr_application::app::{Application, Request};
use atlas_smr_application::state::monolithic_state::MonolithicState;
use atlas_smr_core::exec::WrappedExecHandle;
use atlas_smr_core::networking::SMRNetworkNode;
use atlas_smr_core::persistent_log::MonolithicStateLog;
use atlas_smr_core::SMRReq;
use atlas_smr_core::state_transfer::monolithic_state::MonolithicStateTransfer;
use atlas_smr_execution::TMonolithicStateExecutor;

use crate::config::MonolithicStateReplicaConfig;
use crate::metric::RUN_LATENCY_TIME_ID;
use crate::persistent_log::SMRPersistentLog;
use crate::server::{Exec, PermissionedProtocolHandling, Replica};
use crate::server::monolithic_server::state_transfer::MonStateTransfer;
use crate::server::state_transfer::init_state_transfer_handles;

mod state_transfer;

/// Replica type made to handle monolithic states and executors
pub struct MonReplica<RP, ME, S, A, OP, DL, ST, LT, VT, NT, PL>
    where RP: ReconfigurationProtocol + 'static,
          S: MonolithicState + 'static,
          A: Application<S> + Send,
          OP: LoggableOrderProtocol<SMRReq<A::AppData>, NT>,
          DL: DecisionLog<SMRReq<A::AppData>, OP, NT, PL, Exec<A::AppData>>,
          LT: LogTransferProtocol<SMRReq<A::AppData>, OP, DL, NT, PL, Exec<A::AppData>>,
          ST: MonolithicStateTransfer<S, NT, PL> + PersistableStateTransferProtocol,
          VT: ViewTransferProtocol<OP, NT>,
          PL: SMRPersistentLog<A::AppData, OP::Serialization, OP::PersistableTypes, DL::LogSerialization> + MonolithicStateLog<S> + 'static,
          NT: SMRNetworkNode<RP::InformationProvider, RP::Serialization, A::AppData, OP::Serialization, ST::Serialization, LT::Serialization, VT::Serialization> + 'static, {
    p: PhantomData<fn() -> (A, ME)>,
    /// The inner replica object, responsible for the general replica things
    inner_replica: Replica<RP, S, A::AppData, OP, DL, ST, LT, VT, NT, PL>,
}

impl<RP, ME, S, A, OP, DL, ST, LT, VT, NT, PL> MonReplica<RP, ME, S, A, OP, DL, ST, LT, VT, NT, PL>
    where
        RP: ReconfigurationProtocol + 'static,
        ME: TMonolithicStateExecutor<A, S, NT> + 'static,
        S: MonolithicState + 'static,
        A: Application<S> + Send + 'static,
        OP: LoggableOrderProtocol<SMRReq<A::AppData>, NT> + Send + 'static,
        DL: DecisionLog<SMRReq<A::AppData>, OP, NT, PL, Exec<A::AppData>> + 'static,
        LT: LogTransferProtocol<SMRReq<A::AppData>, OP, DL, NT, PL, Exec<A::AppData>> + 'static,
        VT: ViewTransferProtocol<OP, NT> + 'static,
        ST: MonolithicStateTransfer<S, NT, PL> + PersistableStateTransferProtocol + Send + 'static,
        PL: SMRPersistentLog<A::AppData, OP::Serialization, OP::PersistableTypes, DL::LogSerialization> + MonolithicStateLog<S> + 'static,
        NT: SMRNetworkNode<RP::InformationProvider, RP::Serialization, A::AppData, OP::Serialization, ST::Serialization, LT::Serialization, VT::Serialization> + 'static, {
    pub async fn bootstrap(cfg: MonolithicStateReplicaConfig<RP, S, A, OP, DL, ST, LT, VT, NT, PL>) -> Result<Self> {
        let MonolithicStateReplicaConfig {
            service,
            replica_config,
            st_config
        } = cfg;

        let (executor_handle, executor_receiver) = ME::init_handle();

        let executor_handle = WrappedExecHandle(executor_handle);

        let (handle, inner_handle) = init_state_transfer_handles();

        let inner_replica = Replica::<RP, S, A::AppData, OP, DL, ST, LT, VT, NT, PL>::bootstrap(replica_config, executor_handle.clone(), handle).await?;

        let node = inner_replica.node.clone();

        let (state_tx, checkpoint_rx) =
            ME::init(executor_receiver, None, service, node.clone())?;

        MonStateTransfer
            ::<<Replica::<RP, S, A::AppData, OP, DL, ST, LT, VT, NT, PL> as PermissionedProtocolHandling<A::AppData, S, VT, OP, NT, PL>>::View,
            S, NT, PL, ST>
        ::init_state_transfer_thread(state_tx, checkpoint_rx, st_config,
                                     node.clone(), inner_replica.timeouts.clone(),
                                     inner_replica.persistent_log.clone(), inner_handle);

        let mut replica = Self {
            p: Default::default(),
            inner_replica,
        };

        replica.bootstrap_protocols()?;

        Ok(replica)
    }

    fn bootstrap_protocols(&mut self) -> Result<()> {
        self.inner_replica.bootstrap_protocols()
    }

    pub fn run(&mut self) -> Result<()> {
        let mut last_loop = Instant::now();

        loop {
            self.inner_replica.run()?;

            metric_duration(RUN_LATENCY_TIME_ID, last_loop.elapsed());

            last_loop = Instant::now();
        }
    }
}