use std::marker::PhantomData;

use atlas_common::node_id::NodeId;
use atlas_common::ordering::SeqNo;
use atlas_communication::FullNetworkNode;
use atlas_core::log_transfer::LogTransferProtocol;
use atlas_core::ordering_protocol::loggable::LoggableOrderProtocol;
use atlas_core::ordering_protocol::{OrderingProtocol, PermissionedOrderingProtocol};
use atlas_core::ordering_protocol::permissioned::ViewTransferProtocol;
use atlas_core::persistent_log::{DivisibleStateLog, MonolithicStateLog, PersistableStateTransferProtocol};
use atlas_core::reconfiguration_protocol::ReconfigurationProtocol;
use atlas_core::serialize::{Service};
use atlas_core::smr::smr_decision_log::DecisionLog;
use atlas_core::state_transfer::divisible_state::DivisibleStateTransfer;
use atlas_core::state_transfer::monolithic_state::MonolithicStateTransfer;
use atlas_core::state_transfer::StateTransferProtocol;
use atlas_smr_application::app::Application;
use atlas_smr_application::serialize::ApplicationData;
use atlas_smr_application::state::divisible_state::DivisibleState;
use atlas_smr_application::state::monolithic_state::MonolithicState;

use crate::persistent_log::SMRPersistentLog;

pub struct MonolithicStateReplicaConfig<RF, S, A, OP, DL, ST, LT, VT, NT, PL>
    where RF: ReconfigurationProtocol + 'static,
          S: MonolithicState + 'static,
          A: Application<S> + 'static,
          OP: LoggableOrderProtocol<A::AppData, NT> + 'static,
          DL: DecisionLog<A::AppData, OP, NT, PL> + 'static,
          ST: MonolithicStateTransfer<S, NT, PL> + 'static + PersistableStateTransferProtocol,
          VT: ViewTransferProtocol<OP, NT> + 'static,
          LT: LogTransferProtocol<A::AppData, OP, DL, NT, PL> + 'static,
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
        OP: LoggableOrderProtocol<A::AppData, NT> + 'static,
        DL: DecisionLog<A::AppData, OP, NT, PL> + 'static,
        ST: DivisibleStateTransfer<S, NT, PL> + 'static + PersistableStateTransferProtocol,
        VT: ViewTransferProtocol<OP, NT> + 'static,
        LT: LogTransferProtocol<A::AppData, OP, DL, NT, PL> + 'static,
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
    OP: LoggableOrderProtocol<D, NT> + 'static,
    ST: StateTransferProtocol<S, NT, PL> + 'static,
    DL: DecisionLog<D, OP, NT, PL> + 'static,
    VT: ViewTransferProtocol<OP, NT> + 'static,
    LT: LogTransferProtocol<D, OP, DL, NT, PL> + 'static,
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