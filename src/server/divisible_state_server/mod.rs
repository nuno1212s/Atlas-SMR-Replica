use std::marker::PhantomData;
use std::time::Instant;

use atlas_common::error::*;
use atlas_core::ordering_protocol::loggable::LoggableOrderProtocol;
use atlas_core::ordering_protocol::permissioned::ViewTransferProtocol;
use atlas_core::persistent_log::PersistableStateTransferProtocol;
use atlas_core::reconfiguration_protocol::ReconfigurationProtocol;
use atlas_logging_core::decision_log::DecisionLog;
use atlas_logging_core::log_transfer::LogTransferProtocol;
use atlas_metrics::metrics::metric_duration;
use atlas_smr_application::app::{Application, Request};
use atlas_smr_application::state::divisible_state::DivisibleState;
use atlas_smr_core::networking::SMRNetworkNode;
use atlas_smr_core::persistent_log::DivisibleStateLog;
use atlas_smr_core::state_transfer::divisible_state::DivisibleStateTransfer;
use atlas_smr_execution::TDivisibleStateExecutor;

use crate::config::DivisibleStateReplicaConfig;
use crate::metric::RUN_LATENCY_TIME_ID;
use crate::persistent_log::SMRPersistentLog;
use crate::server::{PermissionedProtocolHandling, Replica};
use crate::server::divisible_state_server::state_transfer::DivStateTransfer;
use crate::server::state_transfer::init_state_transfer_handles;

mod state_transfer;

pub struct DivStReplica<RP, SE, S, A, OP, DL, ST, LT, VT, NT, PL>
    where RP: ReconfigurationProtocol + 'static,
          S: DivisibleState + 'static,
          A: Application<S> + Send,
          OP: LoggableOrderProtocol<Request<A, S>, NT> ,
          DL: DecisionLog<Request<A, S>, OP, NT, PL> ,
          ST: DivisibleStateTransfer<S, NT, PL> + PersistableStateTransferProtocol ,
          LT: LogTransferProtocol<Request<A, S>, OP, DL, NT, PL> ,
          VT: ViewTransferProtocol<OP, NT> +,
          PL: SMRPersistentLog<Request<A, S>, OP::Serialization, OP::PersistableTypes, DL::LogSerialization> + 'static + DivisibleStateLog<S>,
          NT: SMRNetworkNode<RP::InformationProvider, RP::Serialization, A::AppData, OP::Serialization, ST::Serialization, LT::Serialization, VT::Serialization> + 'static,
{
    p: PhantomData<fn() -> (A, SE)>,
    /// The inner replica object, responsible for the general replica things
    inner_replica: Replica<RP, S, A::AppData, OP, DL, ST, LT, VT, NT, PL>,
}

impl<RP, SE, S, A, OP, DL, ST, LT, VT, NT, PL> DivStReplica<RP, SE, S, A, OP, DL, ST, LT, VT, NT, PL> where
    RP: ReconfigurationProtocol + 'static,
    SE: TDivisibleStateExecutor<A, S, NT> + 'static,
    S: DivisibleState + Send + 'static,
    A: Application<S> + Send + 'static,
    OP: LoggableOrderProtocol<Request<A, S>, NT> + Send + 'static,
    DL: DecisionLog<Request<A, S>, OP, NT, PL> + 'static,
    LT: LogTransferProtocol<Request<A, S>, OP, DL, NT, PL> + 'static,
    ST: DivisibleStateTransfer<S, NT, PL> + PersistableStateTransferProtocol + Send + 'static,
    VT: ViewTransferProtocol<OP, NT> + 'static,
    PL: SMRPersistentLog<Request<A, S>, OP::Serialization, OP::PersistableTypes, DL::LogSerialization> + DivisibleStateLog<S> + 'static,
    NT: SMRNetworkNode<RP::InformationProvider, RP::Serialization, A::AppData, OP::Serialization, ST::Serialization, LT::Serialization, VT::Serialization> + 'static, {

    pub async fn bootstrap(cfg: DivisibleStateReplicaConfig<RP, S, A, OP, DL, ST, LT, VT, NT, PL>) -> Result<Self> {
        let DivisibleStateReplicaConfig {
            service, replica_config, st_config
        } = cfg;

        let (handle, inner_handle) = init_state_transfer_handles();

        let (executor_handle, executor_receiver) = SE::init_handle();

        let inner_replica = Replica::<RP, S, A::AppData, OP, DL, ST, LT, VT, NT, PL>::bootstrap(replica_config, executor_handle.clone(),
                                                                                                handle).await?;

        let node = inner_replica.node.clone();

        let (state_tx, checkpoint_rx) =
            SE::init(executor_receiver, None, service, node.clone())?;

        DivStateTransfer
            ::<<Replica::<RP, S, A::AppData, OP, DL, ST, LT, VT, NT, PL> as PermissionedProtocolHandling<A::AppData, S, VT, OP, NT, PL>>::View,
            S, NT, PL, ST>
        ::init_state_transfer_thread(state_tx, checkpoint_rx, st_config,
                                     node.clone(), inner_replica.timeouts.clone(),
                                     inner_replica.persistent_log.clone(), inner_handle);

        let view = inner_replica.view();

        let mut replica = Self {
            p: Default::default(),
            inner_replica,
        };

        replica.bootstrap_protocols()?;

        Ok(replica)
    }

    /// Bootstrap our SMR protocols in order to start
    fn bootstrap_protocols(&mut self) -> Result<()> {
        self.inner_replica.bootstrap_protocols()
    }

    /// Run the main replica thread
    pub fn run(&mut self) -> Result<()> {
        let mut last_loop = Instant::now();

        loop {
            self.inner_replica.run()?;

            metric_duration(RUN_LATENCY_TIME_ID, last_loop.elapsed());

            last_loop = Instant::now();
        }
    }
}