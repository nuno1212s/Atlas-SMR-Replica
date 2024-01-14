use std::path::Path;

use atlas_common::error::*;
use atlas_common::serialization_helper::SerType;
use atlas_core::ordering_protocol::loggable::{OrderProtocolPersistenceHelper, PersistentOrderProtocolTypes};
use atlas_core::ordering_protocol::networking::serialize::{OrderingProtocolMessage};
use atlas_core::ordering_protocol::ProtocolConsensusDecision;
use atlas_core::persistent_log::{OrderingProtocolLog, PersistableStateTransferProtocol};
use atlas_logging_core::decision_log::DecisionLogPersistenceHelper;
use atlas_logging_core::decision_log::serialize::DecisionLogMessage;
use atlas_logging_core::persistent_log::PersistentDecisionLog;
use atlas_persistent_log::PersistentLogModeTrait;
use atlas_persistent_log::stateful_logs::monolithic_state::{initialize_mon_persistent_log, MonStatePersistentLog};
use atlas_smr_application::ExecutorHandle;
use atlas_smr_application::serialize::ApplicationData;
use atlas_smr_application::state::monolithic_state::MonolithicState;
use atlas_smr_core::state_transfer::networking::serialize::StateTransferMessage;

pub trait SMRPersistentLog<RQ, OPM, POPT, LS>: OrderingProtocolLog<RQ, OPM> + PersistentDecisionLog<RQ, OPM, POPT, LS>
    where RQ: SerType,
          OPM: OrderingProtocolMessage<RQ>,
          POPT: PersistentOrderProtocolTypes<RQ, OPM>,
          LS: DecisionLogMessage<RQ, OPM, POPT>  {
    type Config;

    fn init_log<K, T, POS, PSP, DLPH>(executor: ExecutorHandle<RQ>, db_path: K) -> Result<Self>
        where
            K: AsRef<Path>,
            T: PersistentLogModeTrait,
            POS: OrderProtocolPersistenceHelper<RQ, OPM, POPT> + Send + 'static,
            PSP: PersistableStateTransferProtocol + Send + 'static,
            DLPH: DecisionLogPersistenceHelper<RQ, OPM, POPT, LS> + 'static,
            Self: Sized;
}

impl<S, RQ, OPM, POPT, LS, STM> SMRPersistentLog<RQ, OPM, POPT, LS> for MonStatePersistentLog<S, RQ, OPM, POPT, LS, STM>
    where S: MonolithicState + 'static,
          RQ: SerType + 'static,
          OPM: OrderingProtocolMessage<RQ> + 'static,
          POPT: PersistentOrderProtocolTypes<RQ, OPM> + 'static,
          LS: DecisionLogMessage<RQ, OPM, POPT> + 'static,
          STM: StateTransferMessage + 'static {
    type Config = ();

    fn init_log<K, T, POS, PSP, DLPH>(executor: ExecutorHandle<RQ>, db_path: K) -> Result<Self>
        where K: AsRef<Path>, T: PersistentLogModeTrait,
              POS: OrderProtocolPersistenceHelper<RQ, OPM, POPT> + Send + 'static,
              PSP: PersistableStateTransferProtocol + Send + 'static,
              DLPH: DecisionLogPersistenceHelper<RQ, OPM, POPT, LS> + 'static,
              Self: Sized {
        initialize_mon_persistent_log::<S, RQ, K, T, OPM, POPT, LS, STM, POS, PSP, DLPH>(executor, db_path)
    }
}