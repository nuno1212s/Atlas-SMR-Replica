use std::path::Path;

use atlas_common::error::*;
use atlas_core::ordering_protocol::loggable::{OrderProtocolPersistenceHelper, PersistentOrderProtocolTypes};
use atlas_core::ordering_protocol::networking::serialize::{OrderingProtocolMessage, PermissionedOrderingProtocolMessage};
use atlas_core::ordering_protocol::ProtocolConsensusDecision;
use atlas_core::persistent_log::{OrderingProtocolLog, PersistableStateTransferProtocol, PersistentDecisionLog};
use atlas_core::smr::networking::serialize::DecisionLogMessage;
use atlas_core::smr::smr_decision_log::DecisionLogPersistenceHelper;
use atlas_core::state_transfer::networking::serialize::StateTransferMessage;
use atlas_persistent_log::PersistentLogModeTrait;
use atlas_persistent_log::stateful_logs::monolithic_state::{initialize_mon_persistent_log, MonStatePersistentLog};
use atlas_smr_application::ExecutorHandle;
use atlas_smr_application::serialize::ApplicationData;
use atlas_smr_application::state::monolithic_state::MonolithicState;

pub trait SMRPersistentLog<D, OPM, POPT, LS, POP, >: OrderingProtocolLog<D, OPM> + PersistentDecisionLog<D, OPM, POPT, LS>
    where D: ApplicationData + 'static,
          OPM: OrderingProtocolMessage<D> + 'static,
          POPT: PersistentOrderProtocolTypes<D, OPM> + 'static,
          LS: DecisionLogMessage<D, OPM, POPT> + 'static,
          POP: PermissionedOrderingProtocolMessage + 'static {
    type Config;

    fn init_log<K, T, POS, PSP, DLPH>(executor: ExecutorHandle<D>, db_path: K) -> Result<Self>
        where
            K: AsRef<Path>,
            T: PersistentLogModeTrait,
            POS: OrderProtocolPersistenceHelper<D, OPM, POPT> + Send + 'static,
            PSP: PersistableStateTransferProtocol + Send + 'static,
            DLPH: DecisionLogPersistenceHelper<D, OPM, POPT, LS> + 'static,
            Self: Sized;
}

impl<S, D, OPM, POPT, LS, POP, STM> SMRPersistentLog<D, OPM, POPT, LS, POP> for MonStatePersistentLog<S, D, OPM, POPT, LS, POP, STM>
    where S: MonolithicState + 'static,
          D: ApplicationData + 'static,
          OPM: OrderingProtocolMessage<D> + 'static,
          POP: PermissionedOrderingProtocolMessage + 'static,
          POPT: PersistentOrderProtocolTypes<D, OPM> + 'static,
          LS: DecisionLogMessage<D, OPM, POPT> + 'static,
          STM: StateTransferMessage + 'static {
    type Config = ();

    fn init_log<K, T, POS, PSP, DLPH>(executor: ExecutorHandle<D>, db_path: K) -> Result<Self>
        where K: AsRef<Path>, T: PersistentLogModeTrait,
              POS: OrderProtocolPersistenceHelper<D, OPM, POPT> + Send + 'static,
              PSP: PersistableStateTransferProtocol + Send + 'static,
              DLPH: DecisionLogPersistenceHelper<D, OPM, POPT, LS> + 'static,
              Self: Sized {
        initialize_mon_persistent_log::<S, D, K, T, OPM, POPT, LS, POP, STM, POS, PSP, DLPH>(executor, db_path)
    }
}