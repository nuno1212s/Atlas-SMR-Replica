use std::collections::VecDeque;
use std::marker::PhantomData;
use either::Either;
use log::error;
use atlas_common::channel;
use atlas_common::channel::{ChannelSyncRx, ChannelSyncTx};
use atlas_common::maybe_vec::MaybeVec;
use atlas_common::error::*;
use atlas_common::maybe_vec::ordered::MaybeOrderedVec;
use atlas_common::ordering::{InvalidSeqNo, Orderable, SeqNo};
use atlas_communication::message::StoredMessage;
use atlas_core::log_transfer::{LogTM, LogTransferProtocol, LTResult, LTTimeoutResult};
use atlas_core::log_transfer::networking::serialize::LogTransferMessage;
use atlas_core::ordering_protocol::{Decision, DecisionMetadata, ExecutionResult, OrderingProtocol, ProtocolMessage};
use atlas_core::ordering_protocol::loggable::{LoggableOrderProtocol, PersistentOrderProtocolTypes, PProof};
use atlas_core::ordering_protocol::networking::serialize::{NetworkView, OrderingProtocolMessage};
use atlas_core::persistent_log::PersistentDecisionLog;
use atlas_core::request_pre_processing::{PreProcessorMessage, RequestPreProcessor};
use atlas_core::smr::smr_decision_log::{DecisionLog, LoggedDecision, LoggedDecisionValue};
use atlas_core::state_transfer::networking::serialize::StateTransferMessage;
use atlas_core::timeouts::RqTimeout;
use atlas_smr_application::ExecutorHandle;
use atlas_smr_application::serialize::ApplicationData;
use crate::server::CHECKPOINT_PERIOD;
use crate::server::state_transfer::{StateTransferThreadHandle, StateTransferWorkMessage};

/// The handle to the decision log thread
#[derive(Clone)]
pub struct DecisionLogHandle<V, D, OPM, POT, LTM> {
    work_tx: ChannelSyncTx<DLWorkMessage<V, D, OPM, POT, LTM>>,
    status_rx: ChannelSyncRx<ReplicaWorkResponses>,
}

pub enum DecisionLogWorkMessage<D, OPM, POT>
    where D: ApplicationData,
          OPM: OrderingProtocolMessage<D>,
          POT: PersistentOrderProtocolTypes<D, OPM>
{
    ClearSequenceNumber(SeqNo),
    ClearDecisionsForward(SeqNo),
    DecisionInformation(MaybeVec<Decision<DecisionMetadata<D, OPM>, ProtocolMessage<D, OPM>, D::Request>>),
    Proof(PProof<D, OPM, POT>),
}

/// Messages that are destined to the replica so it can piece
/// together the current state of the decision log
pub enum ReplicaWorkResponses {
    InstallSeqNo(SeqNo),
    LogTransferFinalized(SeqNo, SeqNo),
}

pub enum LogTransferWorkMessage<D, OPM, LTM>
    where D: ApplicationData,
          OPM: OrderingProtocolMessage<D>,
          LTM: LogTransferMessage<D, OPM> {
    RequestLogTransfer,
    LogTransferMessage(StoredMessage<LogTM<D, OPM, LTM>>),
    ReceivedTimeout(Vec<RqTimeout>),
    TransferDone(SeqNo, SeqNo),
}

pub enum DLWorkMessageType<D, OPM, POT, LTM>
    where D: ApplicationData + 'static,
          OPM: OrderingProtocolMessage<D>,
          POT: PersistentOrderProtocolTypes<D, OPM>,
          LTM: LogTransferMessage<D, OPM> {
    DecisionLog(DecisionLogWorkMessage<D, OPM, POT>),
    LogTransfer(LogTransferWorkMessage<D, OPM, LTM>),
}

pub struct DLWorkMessage<V, D, OPM, POT, LTM>
    where V: NetworkView,
          D: ApplicationData + 'static,
          OPM: OrderingProtocolMessage<D>,
          POT: PersistentOrderProtocolTypes<D, OPM>,
          LTM: LogTransferMessage<D, OPM> {
    view: V,
    message: DLWorkMessageType<D, OPM, POT, LTM>,
}

pub enum DecisionLogResponseMessage<D> where D: ApplicationData {
    InstallSeqNo(SeqNo),
    TransferProtocolFinished(SeqNo, SeqNo, MaybeVec<LoggedDecision<D::Request>>),
}

pub enum ActivePhase {
    LogTransfer,
    DecisionLog,
}

/// The work queue for the decision log
pub struct DecisionLogWorkQueue<D, OPM, POT> {
    work_queue: VecDeque<DecisionLogWorkMessage<D, OPM, POT>>,
}

pub struct DecisionLogManager<V, D, OP, DL, LT, STM, NT, PL>
    where V: NetworkView,
          D: ApplicationData + 'static,
          OP: LoggableOrderProtocol<D, NT>,
          DL: DecisionLog<D, OP, NT, PL>,
          LT: LogTransferProtocol<D, OP, DL, NT, PL>,
          STM: StateTransferMessage
{
    decision_log: DL,
    log_transfer: LT,
    work_receiver: ChannelSyncRx<DLWorkMessage<V, D, OP::Serialization, OP::PersistableTypes, LT::Serialization>>,
    order_protocol_tx: ChannelSyncTx<ReplicaWorkResponses>,
    decision_log_pending_queue: DecisionLogWorkQueue<D, OP::Serialization, OP::PersistableTypes>,
    active_phase: ActivePhase,
    rq_pre_processor: RequestPreProcessor<D::Request>,
    state_transfer_handle: StateTransferThreadHandle<V, STM>,
    executor_handle: ExecutorHandle<D>,
    pending_decisions_to_execute: Option<MaybeVec<LoggedDecision<D::Request>>>,
    _ph: PhantomData<(V, D, OP, NT, PL)>,
}

impl<V, D, OP, DL, LT, STM, NT, PL> DecisionLogManager<V, D, OP, DL, LT, STM, NT, PL>
    where V: NetworkView,
          D: ApplicationData + 'static,
          OP: LoggableOrderProtocol<D, NT>,
          DL: DecisionLog<D, OP, NT, PL>,
          LT: LogTransferProtocol<D, OP, DL, NT, PL>,
          PL: PersistentDecisionLog<D, OP::Serialization, OP::PersistableTypes, DL::LogSerialization>,
          STM: StateTransferMessage, {
    fn run(mut self) -> Result<()> {
        loop {
            let work = self.work_receiver.recv();

            if let Ok(work_message) = work {
                let DLWorkMessage {
                    view,
                    message,
                } = work_message;

                match message {
                    DLWorkMessageType::DecisionLog(dl_message) => {
                        self.handle_decision_log_work(dl_message)?;
                    }
                    DLWorkMessageType::LogTransfer(lt_message) => {
                        self.handle_log_transfer_work(view, lt_message)?;
                    }
                }
            }
        }

        Ok(())
    }

    fn handle_decision_log_work(&mut self,
                                dl_work: DecisionLogWorkMessage<D, OP::Serialization, OP::PersistableTypes>) -> Result<()> {
        match self.active_phase {
            ActivePhase::DecisionLog => {
                self.run_decision_log_work_message(dl_work)?;
            }
            ActivePhase::LogTransfer => {
                self.decision_log_pending_queue.work_queue.push_back(dl_work);
            }
        }

        Ok(())
    }

    fn handle_log_transfer_work(&mut self, view: V, lt_work: LogTransferWorkMessage<D, OP::Serialization, OP::PersistableTypes>) -> Result<()> {
        match self.active_phase {
            ActivePhase::LogTransfer => {
                self.run_log_transfer_work_message(view, lt_work)?;
            }
            ActivePhase::DecisionLog => {
                match lt_work {
                    LogTransferWorkMessage::RequestLogTransfer => {
                        self.run_log_transfer_protocol(view)?;
                    }
                    LogTransferWorkMessage::LogTransferMessage(message) => {
                        self.log_transfer.handle_off_ctx_message(&mut self.decision_log, view, message)?;
                    }
                    LogTransferWorkMessage::ReceivedTimeout(timeouts) => {
                        match self.log_transfer.handle_timeout(view.clone(), timeouts)? {
                            LTTimeoutResult::RunLTP => self.run_log_transfer_protocol(view)?,
                            LTTimeoutResult::NotNeeded => {
                                // Keep executing the decision log...
                            }
                        }
                    }
                    LogTransferWorkMessage::TransferDone(_, _) => {
                        unreachable!("How can the transfer be done if we are running the decision log")
                    }
                }
            }
        }

        Ok(())
    }

    fn run_decision_log_work_message(&mut self, dl_work: DecisionLogWorkMessage<D, OP::Serialization, OP::PersistableTypes>) -> Result<()>
    {
        match dl_work {
            DecisionLogWorkMessage::ClearSequenceNumber(clear_seq_no) => {
                self.decision_log.clear_sequence_number(clear_seq_no)?;
            }
            DecisionLogWorkMessage::ClearDecisionsForward(seq_no) => {
                self.decision_log.clear_decisions_forward(seq_no)?;
            }
            DecisionLogWorkMessage::DecisionInformation(decision_info) => {
                for decision in decision_info.into_iter() {
                    let decisions_made = self.decision_log.decision_information_received(decision)?;

                    self.execute_logged_decisions(decisions_made)?;
                }
            }
            DecisionLogWorkMessage::Proof(proof) => {
                self.decision_log.install_proof(proof)?;
            }
        }

        Ok(())
    }

    fn run_log_transfer_work_message(&mut self, view: V, lt_work: LogTransferWorkMessage<D, OP::Serialization, OP::PersistableTypes>) -> Result<()> {
        match lt_work {
            LogTransferWorkMessage::RequestLogTransfer => {
                self.run_log_transfer_protocol(view)?;
            }
            LogTransferWorkMessage::LogTransferMessage(message) => {
                match self.log_transfer.process_message(&mut self.decision_log, view.clone(), message)? {
                    LTResult::RunLTP => {
                        self.log_transfer.request_latest_log(&mut self.decision_log, view)?;
                    }
                    LTResult::NotNeeded => {
                        self.run_decision_log_protocol()?;
                    }
                    LTResult::Running => {}
                    LTResult::InstallSeq(seq) => {
                        let _ = self.order_protocol_tx.send(ReplicaWorkResponses::InstallSeqNo(seq));
                    }
                    LTResult::LTPFinished(init_seq, last_se, decisions_to_execute) => {
                        self.pending_decisions_to_execute = Some(decisions_to_execute);

                        let _ = self.order_protocol_tx.send(ReplicaWorkResponses::LogTransferFinalized(init_seq, last_se));
                    }
                };
            }
            LogTransferWorkMessage::ReceivedTimeout(timeout) => {
                self.log_transfer.handle_timeout(view, timeout)?;
            }
            LogTransferWorkMessage::TransferDone(start, end) => {
                if let Some(decisions) = self.pending_decisions_to_execute.take() {
                    decisions.into_iter().for_each(|decision| {
                        match decision.sequence_number().index(start) {
                            Either::Left(_) => {}
                            Either::Right(_) => {
                                let (seq, client_rqs, decision) = decision.into_inner();

                                let _ = self.rq_pre_processor.send(PreProcessorMessage::DecidedBatch(client_rqs));

                                match decision {
                                    LoggedDecisionValue::Execute(batch) => {
                                        let _ = self.executor_handle.queue_update(batch);
                                    }
                                    LoggedDecisionValue::ExecutionNotNeeded => {
                                        unreachable!("When installing a log transfer, we require the update batch to deliver")
                                    }
                                }
                            }
                        }
                    });
                }
            }
        }

        Ok(())
    }

    /// Run the log transfer protocol
    fn run_log_transfer_protocol(&mut self, view: V) -> Result<()> {
        self.active_phase = ActivePhase::LogTransfer;

        self.log_transfer.request_latest_log(&mut self.decision_log, view)?;

        Ok(())
    }

    fn run_decision_log_protocol(&mut self) -> Result<()> {
        self.active_phase = ActivePhase::DecisionLog;

        while let Some(work) = self.decision_log_pending_queue.work_queue.pop_front() {
            self.handle_decision_log_work(work)?;
        }

        Ok(())
    }

    fn execute_logged_decisions(&mut self, decisions: MaybeVec<LoggedDecision<D::Request>>) -> Result<()> {
        for decision in decisions.into_iter() {
            let (seq, requests, to_batch) = decision.into_inner();

            if let Err(err) = self.rq_pre_processor.send(PreProcessorMessage::DecidedBatch(requests)) {
                error!("Error sending decided batch to pre processor: {:?}", err);
            }

            let last_seq_no_u32 = u32::from(seq);

            let checkpoint = if last_seq_no_u32 > 0 && last_seq_no_u32 % CHECKPOINT_PERIOD == 0 {
//We check that % == 0 so we don't start multiple checkpoints

                let (e_tx, e_rx) = channel::new_oneshot_channel();

                self.state_transfer_handle.send_work_message(StateTransferWorkMessage::ShouldRequestAppState(seq, e_tx));

                if let Ok(res) = e_rx.recv() {
                    res
                } else {
                    ExecutionResult::Nil
                }
            } else {
                ExecutionResult::Nil
            };

            match to_batch {
                LoggedDecisionValue::Execute(requests) => {
                    match checkpoint {
                        ExecutionResult::Nil => {
                            self.executor_handle.queue_update(requests)?
                        }
                        ExecutionResult::BeginCheckpoint => {
                            self.executor_handle.queue_update_and_get_appstate(requests)?
                        }
                    }
                }
                LoggedDecisionValue::ExecutionNotNeeded => {
                    // When the execution is handled
                }
            }
        }

        Ok(())
    }
}

impl<V, D, OPM, POT, LTM> DLWorkMessage<V, D, OPM, POT, LTM> {
    pub fn initialize_message(view: V, work_msg: DLWorkMessageType<D, OPM, POT, LTM>) -> Self {
        Self {
            view,
            message: work_msg,
        }
    }

    pub fn init_log_transfer_message(view: V, work_msg: LogTransferWorkMessage<D, OPM, LTM>) -> Self {
        Self::initialize_message(view, DLWorkMessageType::LogTransfer(work_msg))
    }

    pub fn init_dec_log_message(view: V, work_msg: DecisionLogWorkMessage<D, OPM, POT>) -> Self {
        Self::initialize_message(view, DLWorkMessageType::DecisionLog(work_msg))
    }
}

impl<V, D, OPM, POT, LTM> DecisionLogHandle<V, D, OPM, POT, LTM> {
    pub fn send_work(&self, work_message: DLWorkMessage<V, D, OPM, POT, LTM>) {
        let _ = self.work_tx.send(work_message);
    }

    pub fn try_to_recv_resp(&self) -> Option<ReplicaWorkResponses> {
        if let Ok(message) = self.status_rx.recv() {
            Some(message)
        } else {
            None
        }
    }
}