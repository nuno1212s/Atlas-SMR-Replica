use std::marker::PhantomData;

use atlas_common::ordering::SeqNo;
use atlas_communication::FullNetworkNode;
use atlas_core::ordering_protocol::loggable::LoggableOrderProtocol;
use atlas_core::ordering_protocol::OrderingProtocol;
use atlas_core::ordering_protocol::permissioned::ViewTransferProtocol;
use atlas_core::persistent_log::PersistableStateTransferProtocol;
use atlas_core::reconfiguration_protocol::ReconfigurationProtocol;
use atlas_logging_core::decision_log::DecisionLog;
use atlas_logging_core::log_transfer::LogTransferProtocol;
use atlas_smr_application::app::{Application, Request};
use atlas_smr_application::serialize::ApplicationData;
use atlas_smr_application::state::divisible_state::DivisibleState;
use atlas_smr_application::state::monolithic_state::MonolithicState;
use atlas_smr_core::persistent_log::{DivisibleStateLog, MonolithicStateLog};
use atlas_smr_core::serialize::Service;
use atlas_smr_core::SMRReq;
use atlas_smr_core::state_transfer::divisible_state::DivisibleStateTransfer;
use atlas_smr_core::state_transfer::monolithic_state::MonolithicStateTransfer;
use atlas_smr_core::state_transfer::StateTransferProtocol;

use crate::persistent_log::SMRPersistentLog;
use crate::server::Exec;

pub struct MonolithicStateReplicaConfig<RF, S, A, OP, DL, ST, LT, VT, NT, PL>
    where RF: ReconfigurationProtocol + 'static,
          S: MonolithicState + 'static,
          A: Application<S> + 'static,
          OP: LoggableOrderProtocol<SMRReq<A::AppData>, NT> + 'static,
          DL: DecisionLog<SMRReq<A::AppData>, OP, NT, PL, Exec<A::AppData>> + 'static,
          ST: MonolithicStateTransfer<S, NT, PL> + 'static + PersistableStateTransferProtocol,
          VT: ViewTransferProtocol<OP, NT> + 'static,
          LT: LogTransferProtocol<SMRReq<A::AppData>, OP, DL, NT, PL, Exec<A::AppData>> + 'static,
          NT: FullNetworkNode<RF::InformationProvider, RF::Serialization, Service<A::AppData, OP::Serialization, ST::Serialization, LT::Serialization, VT::Serialization>>,
          PL: SMRPersistentLog<A::AppData, OP::Serialization, OP::PersistableTypes, DL::LogSerialization> + MonolithicStateLog<S> {
    /// The application logic.
    pub service: A,

    // the configuration for the replica
    pub replica_config: ReplicaConfig<RF, S, A::AppData, OP, DL, ST, LT, VT, NT, PL>,

    /// The configuration for the State transfer protocol
    pub st_config: ST::Config,
}

pub struct DivisibleStateReplicaConfig<RF, S, A, OP, DL, ST, LT, VT, NT, PL>
    where
        RF: ReconfigurationProtocol + 'static,
        S: DivisibleState + 'static,
        A: Application<S> + 'static,
        OP: LoggableOrderProtocol<SMRReq<A::AppData>, NT> + 'static,
        DL: DecisionLog<SMRReq<A::AppData>, OP, NT, PL, Exec<A::AppData>> + 'static,
        ST: DivisibleStateTransfer<S, NT, PL> + 'static + PersistableStateTransferProtocol,
        VT: ViewTransferProtocol<OP, NT> + 'static,
        LT: LogTransferProtocol<SMRReq<A::AppData>, OP, DL, NT, PL, Exec<A::AppData>> + 'static,
        NT: FullNetworkNode<RF::InformationProvider, RF::Serialization, Service<A::AppData, OP::Serialization, ST::Serialization, LT::Serialization, VT::Serialization>>,
        PL: SMRPersistentLog<A::AppData, OP::Serialization, OP::PersistableTypes, DL::LogSerialization> + DivisibleStateLog<S> {
    /// The application logic.
    pub service: A,

    pub replica_config: ReplicaConfig<RF, S, A::AppData, OP, DL, ST, LT, VT, NT, PL>,

    /// The configuration for the State transfer protocol
    pub st_config: ST::Config,
}

/// Represents a configuration used to bootstrap a `Replica`.
pub struct ReplicaConfig<RF, S, D, OP, DL, ST, LT, VT, NT, PL> where
    RF: ReconfigurationProtocol + 'static,
    D: ApplicationData + 'static,
    OP: LoggableOrderProtocol<SMRReq<D>, NT> + 'static,
    ST: StateTransferProtocol<S, NT, PL> + 'static,
    DL: DecisionLog<SMRReq<D>, OP, NT, PL, Exec<D>> + 'static,
    VT: ViewTransferProtocol<OP, NT> + 'static,
    LT: LogTransferProtocol<SMRReq<D>, OP, DL, NT, PL, Exec<D>> + 'static,
    NT: FullNetworkNode<RF::InformationProvider, RF::Serialization, Service<D, OP::Serialization, ST::Serialization, LT::Serialization, VT::Serialization>>,
    PL: SMRPersistentLog<D, OP::Serialization, OP::PersistableTypes, DL::LogSerialization> {
    /// Next sequence number attributed to a request by
    /// the consensus layer.
    pub next_consensus_seq: SeqNo,

    /// The path to the database
    pub db_path: String,

    /// The configuration for the ordering protocol
    pub op_config: OP::Config,

    /// The configuration for the decision log protocol
    pub dl_config: DL::Config,

    /// The configuration for the log transfer protocol
    pub lt_config: LT::Config,

    /// The configuration for the persistent log module
    pub pl_config: PL::Config,

    /// The configuration for the view transfer module
    pub vt_config: VT::Config,

    /// Check out the docs on `NodeConfig`.
    pub node: NT::Config,

    /// The configuration for the reconfiguration protocol
    pub reconfig_node: RF::Config,

    pub p: PhantomData<S>,
}