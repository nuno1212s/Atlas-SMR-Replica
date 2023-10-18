use std::marker::PhantomData;

use atlas_common::node_id::NodeId;
use atlas_common::ordering::SeqNo;
use atlas_communication::FullNetworkNode;
use atlas_core::log_transfer::LogTransferProtocol;
use atlas_core::ordering_protocol::loggable::LoggableOrderProtocol;
use atlas_core::ordering_protocol::{OrderingProtocol, PermissionedOrderingProtocol};
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

pub struct MonolithicStateReplicaConfig<RF, S, A, OP, DL, ST, LT, NT, PL>
    where RF: ReconfigurationProtocol + 'static,
          S: MonolithicState + 'static,
          A: Application<S> + 'static,
          OP: LoggableOrderProtocol<A::AppData, NT> + PermissionedOrderingProtocol + 'static,
          DL: DecisionLog<A::AppData, OP, NT, PL> + 'static,
          ST: MonolithicStateTransfer<S, NT, PL> + 'static + PersistableStateTransferProtocol,
          LT: LogTransferProtocol<A::AppData, OP, DL, NT, PL> + 'static,
          NT: FullNetworkNode<RF::InformationProvider, RF::Serialization, Service<A::AppData, OP::Serialization, ST::Serialization, LT::Serialization>>,
          PL: SMRPersistentLog<A::AppData, OP::Serialization, OP::PersistableTypes, DL::LogSerialization, OP::PermissionedSerialization> + MonolithicStateLog<S> {
    /// The application logic.
    pub service: A,

    pub replica_config: ReplicaConfig<RF, S, A::AppData, OP, DL, ST, LT, NT, PL>,

    /// The configuration for the State transfer protocol
    pub st_config: ST::Config,
}

pub struct DivisibleStateReplicaConfig<RF, S, A, OP, DL, ST, LT, NT, PL>
    where
        RF: ReconfigurationProtocol + 'static,
        S: DivisibleState + 'static,
        A: Application<S> + 'static,
        OP: LoggableOrderProtocol<A::AppData, NT> + PermissionedOrderingProtocol + 'static,
        DL: DecisionLog<A::AppData, OP, NT, PL> + 'static,
        ST: DivisibleStateTransfer<S, NT, PL> + 'static + PersistableStateTransferProtocol,
        LT: LogTransferProtocol<A::AppData, OP, DL, NT, PL> + 'static,
        NT: FullNetworkNode<RF::InformationProvider, RF::Serialization, Service<A::AppData, OP::Serialization, ST::Serialization, LT::Serialization>>,
        PL: SMRPersistentLog<A::AppData, OP::Serialization, OP::PersistableTypes, DL::LogSerialization, OP::PermissionedSerialization> + DivisibleStateLog<S> {
    /// The application logic.
    pub service: A,

    pub replica_config: ReplicaConfig<RF, S, A::AppData, OP, DL, ST, LT, NT, PL>,

    /// The configuration for the State transfer protocol
    pub st_config: ST::Config,
}

/// Represents a configuration used to bootstrap a `Replica`.
pub struct ReplicaConfig<RF, S, D, OP, DL, ST, LT, NT, PL> where
    RF: ReconfigurationProtocol + 'static,
    D: ApplicationData + 'static,
    OP: LoggableOrderProtocol<D, NT> + PermissionedOrderingProtocol + 'static,
    ST: StateTransferProtocol<S, NT, PL> + 'static,
    DL: DecisionLog<D, OP, NT, PL> + 'static,
    LT: LogTransferProtocol<D, OP, DL, NT, PL> + 'static,
    NT: FullNetworkNode<RF::InformationProvider, RF::Serialization, Service<D, OP::Serialization, ST::Serialization, LT::Serialization>>,
    PL: SMRPersistentLog<D, OP::Serialization, OP::PersistableTypes, DL::LogSerialization, OP::PermissionedSerialization> {
    /// ID of the Node in question
    pub id: NodeId,

    /// The number of nodes in the network
    pub n: usize,
    /// The number of nodes that can fail in the network
    pub f: usize,

    ///TODO: These two values should be loaded from storage
    /// The sequence number for the current view.
    pub view: SeqNo,

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

    /// Check out the docs on `NodeConfig`.
    pub node: NT::Config,

    /// The configuration for the reconfiguration protocol
    pub reconfig_node: RF::Config,

    pub p: PhantomData<S>,
}