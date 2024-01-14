use std::marker::PhantomData;

use atlas_common::ordering::SeqNo;
use atlas_communication::FullNetworkNode;
use atlas_smr_core::log_transfer::LogTransferProtocol;
use atlas_core::ordering_protocol::loggable::LoggableOrderProtocol;
use atlas_core::ordering_protocol::OrderingProtocol;
use atlas_core::ordering_protocol::permissioned::ViewTransferProtocol;
use atlas_core::persistent_log::PersistableStateTransferProtocol;
use atlas_core::reconfiguration_protocol::ReconfigurationProtocol;
use atlas_smr_application::app::{Application, Request};
use atlas_smr_application::serialize::ApplicationData;
use atlas_smr_application::state::divisible_state::DivisibleState;
use atlas_smr_application::state::monolithic_state::MonolithicState;
use atlas_smr_core::persistent_log::{DivisibleStateLog, MonolithicStateLog};
use atlas_smr_core::serialize::Service;
use atlas_smr_core::smr_decision_log::DecisionLog;
use atlas_smr_core::state_transfer::divisible_state::DivisibleStateTransfer;
use atlas_smr_core::state_transfer::monolithic_state::MonolithicStateTransfer;
use atlas_smr_core::state_transfer::StateTransferProtocol;

use crate::persistent_log::SMRPersistentLog;

pub struct MonolithicStateReplicaConfig<RF, S, A, OP, DL, ST, LT, VT, NT, PL>
    where RF: ReconfigurationProtocol + 'static,
          S: MonolithicState + 'static,
          A: Application<S> + 'static,
          OP: LoggableOrderProtocol<Request<A, S>, NT> + 'static,
          DL: DecisionLog<Request<A, S>, OP, NT, PL> + 'static,
          ST: MonolithicStateTransfer<S, NT, PL> + 'static + PersistableStateTransferProtocol,
          VT: ViewTransferProtocol<OP, NT> + 'static,
          LT: LogTransferProtocol<Request<A, S>, OP, DL, NT, PL> + 'static,
          NT: FullNetworkNode<RF::InformationProvider, RF::Serialization, Service<A::AppData, OP::Serialization, ST::Serialization, LT::Serialization, VT::Serialization>>,
          PL: SMRPersistentLog<Request<A, S>, OP::Serialization, OP::PersistableTypes, DL::LogSerialization> + MonolithicStateLog<S> {
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
        OP: LoggableOrderProtocol<Request<A, S>, NT> + 'static,
        DL: DecisionLog<Request<A, S>, OP, NT, PL> + 'static,
        ST: DivisibleStateTransfer<S, NT, PL> + 'static + PersistableStateTransferProtocol,
        VT: ViewTransferProtocol<OP, NT> + 'static,
        LT: LogTransferProtocol<Request<A, S>, OP, DL, NT, PL> + 'static,
        NT: FullNetworkNode<RF::InformationProvider, RF::Serialization, Service<A::AppData, OP::Serialization, ST::Serialization, LT::Serialization, VT::Serialization>>,
        PL: SMRPersistentLog<Request<A, S>, OP::Serialization, OP::PersistableTypes, DL::LogSerialization> + DivisibleStateLog<S> {
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
    OP: LoggableOrderProtocol<D::Request, NT> + 'static,
    ST: StateTransferProtocol<S, NT, PL> + 'static,
    DL: DecisionLog<D::Request, OP, NT, PL> + 'static,
    VT: ViewTransferProtocol<OP, NT> + 'static,
    LT: LogTransferProtocol<D::Request, OP, DL, NT, PL> + 'static,
    NT: FullNetworkNode<RF::InformationProvider, RF::Serialization, Service<D, OP::Serialization, ST::Serialization, LT::Serialization, VT::Serialization>>,
    PL: SMRPersistentLog<D::Request, OP::Serialization, OP::PersistableTypes, DL::LogSerialization> {
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