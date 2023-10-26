//! Contains the server side core protocol logic of `Atlas`.

use std::fmt::{Debug, Formatter};
use std::marker::PhantomData;
use std::sync::Arc;
use std::time::{Duration, Instant};

use log::{debug, error, info, trace, warn};

use atlas_common::channel;
use atlas_common::channel::{ChannelSyncRx, ChannelSyncTx};
use atlas_common::error::*;
use atlas_common::globals::ReadOnly;
use atlas_common::maybe_vec::MaybeVec;
use atlas_common::node_id::NodeId;
use atlas_common::ordering::{Orderable, SeqNo};
use atlas_communication::message::StoredMessage;
use atlas_communication::NetworkNode;
use atlas_communication::protocol_node::{NodeIncomingRqHandler, ProtocolNetworkNode};
use atlas_core::log_transfer::{LogTransferProtocol, LTPollResult, LTResult, LTTimeoutResult};
use atlas_core::messages::Message;
use atlas_core::messages::SystemMessage;
use atlas_core::ordering_protocol::{DecisionsAhead, ExecutionResult, OPExecResult, OPPollResult, OrderingProtocol, OrderingProtocolArgs, PermissionedOrderingProtocol, ProtocolMessage, View};
use atlas_core::ordering_protocol::loggable::LoggableOrderProtocol;
use atlas_core::ordering_protocol::networking::serialize::{NetworkView, OrderingProtocolMessage};
use atlas_core::ordering_protocol::networking::ViewTransferProtocolSendNode;
use atlas_core::ordering_protocol::permissioned::{ViewTransferProtocol, VTMsg, VTPollResult, VTResult};
use atlas_core::ordering_protocol::reconfigurable_order_protocol::{ReconfigurableOrderProtocol, ReconfigurationAttemptResult};
use atlas_core::persistent_log::OperationMode;
use atlas_core::persistent_log::PersistableStateTransferProtocol;
use atlas_core::reconfiguration_protocol::{AlterationFailReason, QuorumAlterationResponse, QuorumAttemptJoinResponse, QuorumReconfigurationMessage, QuorumReconfigurationResponse, ReconfigurableNodeTypes, ReconfigurationProtocol};
use atlas_core::request_pre_processing::{initialize_request_pre_processor, PreProcessorMessage, RequestPreProcessor};
use atlas_core::request_pre_processing::work_dividers::WDRoundRobin;
use atlas_core::smr::networking::serialize::OrderProtocolLog;
use atlas_core::smr::networking::SMRNetworkNode;
use atlas_core::smr::smr_decision_log::{DecisionLog, LoggedDecision, LoggedDecisionValue, ShareableMessage};
use atlas_core::state_transfer::{StateTransferProtocol, STPollResult, STResult, STTimeoutResult};
use atlas_core::timeouts::{RqTimeout, TimedOut, TimeoutKind, Timeouts};
use atlas_metrics::metrics::{metric_duration, metric_increment};
use atlas_persistent_log::NoPersistentLog;
use atlas_smr_application::ExecutorHandle;
use atlas_smr_application::serialize::ApplicationData;

use crate::config::ReplicaConfig;
use crate::metric::{LOG_TRANSFER_PROCESS_TIME_ID, ORDERING_PROTOCOL_PROCESS_TIME_ID, REPLICA_INTERNAL_PROCESS_TIME_ID, REPLICA_ORDERED_RQS_PROCESSED_ID, REPLICA_PROTOCOL_RESP_PROCESS_TIME_ID, REPLICA_TAKE_FROM_NETWORK_ID, STATE_TRANSFER_PROCESS_TIME_ID, TIMEOUT_PROCESS_TIME_ID};
use crate::persistent_log::SMRPersistentLog;

pub mod client_replier;
pub mod follower_handling;
pub mod monolithic_server;
pub mod divisible_state_server;
pub mod state_transfer;
// pub mod rq_finalizer;

const REPLICA_MESSAGE_CHANNEL: usize = 1024;
pub const REPLICA_WAIT_TIME: Duration = Duration::from_millis(1000);

pub type StateTransferDone = Option<SeqNo>;
pub type LogTransferDone<R> = Option<(SeqNo, SeqNo, MaybeVec<LoggedDecision<R>>)>;

#[derive(Clone)]
pub(crate) enum ReplicaPhase<R> {
    // The replica is currently executing the ordering protocol
    OrderingProtocol(OPPhase<R>),
    // The replica is currently executing the state transfer protocol
    StateTransferProtocol {
        state_transfer: StateTransferDone,
        log_transfer: LogTransferDone<R>,
    },
}

#[derive(Clone)]
pub(crate) enum OPPhase<R> {
    OrderProtocol,
    ViewTransferProtocol(Option<Box<ReplicaPhase<R>>>),
}

pub struct Replica<RP, S, D, OP, DL, ST, LT, VT, NT, PL>
    where D: ApplicationData + 'static,
          OP: LoggableOrderProtocol<D, NT> + 'static,
          DL: DecisionLog<D, OP, NT, PL> + 'static,
          LT: LogTransferProtocol<D, OP, DL, NT, PL> + 'static,
          ST: StateTransferProtocol<S, NT, PL> + PersistableStateTransferProtocol + 'static,
          VT: ViewTransferProtocol<OP, NT> + 'static,
          PL: SMRPersistentLog<D, OP::Serialization, OP::PersistableTypes, DL::LogSerialization> + 'static,
          RP: ReconfigurationProtocol + 'static {
    replica_phase: ReplicaPhase<D::Request>,

    quorum_reconfig_data: QuorumReconfig,

    // The ordering protocol, responsible for ordering requests
    ordering_protocol: OP,
    // The decision log protocol, responsible for keeping track of all of the
    // order protocol decisions that have been made
    decision_log: DL,
    log_transfer_protocol: LT,
    view_transfer_protocol: VT,
    rq_pre_processor: RequestPreProcessor<D::Request>,
    timeouts: Timeouts,
    executor_handle: ExecutorHandle<D>,
    // The networking layer for a Node in the network (either Client or Replica)
    node: Arc<NT>,
    // The handle to the execution and timeouts handler
    execution: (ChannelSyncRx<Message>, ChannelSyncTx<Message>),
    // THe handle for processed timeouts
    processed_timeout: (ChannelSyncTx<(Vec<RqTimeout>, Vec<RqTimeout>)>, ChannelSyncRx<(Vec<RqTimeout>, Vec<RqTimeout>)>),

    // Receive reconfiguration messages from the reconfiguration protocol
    reconf_receive: ChannelSyncRx<QuorumReconfigurationMessage>,
    // reconfiguration protocol send
    reconf_tx: ChannelSyncTx<QuorumReconfigurationResponse>,

    persistent_log: PL,
    // The reconfiguration protocol handle
    reconfig_protocol: RP,

    st: PhantomData<(S, ST)>,
}

/// This is used to keep track of the node that is currently
/// attempting to join the server
pub struct QuorumReconfig {
    node_pending_join: Option<NodeId>,
}

/// Result of polling the log transfer protocol
enum LTPollRes {
    None,
    Receive,
    Continue,
}

/// Result of polling the state transfer protocol
enum STPollRes {
    None,
    Receive,
    Continue,
}

pub trait ReconfigurableProtocolHandling {
    fn attempt_quorum_join(&mut self, node: NodeId) -> Result<()>;

    fn attempt_to_join_quorum(&mut self) -> Result<()>;
}

pub trait PermissionedProtocolHandling<S, VT, OP, ST, NT, PL>
    where VT: ViewTransferProtocol<OP, NT> {
    type View: NetworkView;

    fn view(&self) -> Self::View;

    fn run_view_transfer(&mut self) -> Result<()>
        where NT: ViewTransferProtocolSendNode<VT::Serialization>;

    fn iterate_view_transfer_protocol(&mut self, state_transfer: &mut ST) -> Result<()>
        where NT: ViewTransferProtocolSendNode<VT::Serialization>,
              ST: StateTransferProtocol<S, NT, PL>;

    fn handle_view_transfer_msg(&mut self, msg: StoredMessage<VTMsg<VT::Serialization>>) -> Result<()>
        where NT: ViewTransferProtocolSendNode<VT::Serialization>;
}

impl<RP, S, D, OP, DL, ST, LT, VT, NT, PL> Replica<RP, S, D, OP, DL, ST, LT, VT, NT, PL>
    where
        RP: ReconfigurationProtocol + 'static,
        D: ApplicationData + 'static,
        OP: LoggableOrderProtocol<D, NT> + Send + 'static,
        DL: DecisionLog<D, OP, NT, PL> + 'static,
        LT: LogTransferProtocol<D, OP, DL, NT, PL> + 'static,
        ST: StateTransferProtocol<S, NT, PL> + PersistableStateTransferProtocol + Send + 'static,
        VT: ViewTransferProtocol<OP, NT> + 'static,
        NT: SMRNetworkNode<RP::InformationProvider, RP::Serialization, D, OP::Serialization, ST::Serialization, LT::Serialization, VT::Serialization> + 'static,
        PL: SMRPersistentLog<D, OP::Serialization, OP::PersistableTypes, DL::LogSerialization> + 'static, {
    async fn bootstrap(cfg: ReplicaConfig<RP, S, D, OP, DL, ST, LT, VT, NT, PL>, executor: ExecutorHandle<D>) -> Result<Self> {
        let ReplicaConfig {
            id: log_node_id,
            n,
            f,
            view,
            next_consensus_seq,
            db_path,
            op_config,
            lt_config,
            dl_config,
            pl_config,
            vt_config,
            node: node_config,
            reconfig_node, p,
        } = cfg;

        debug!("{:?} // Bootstrapping replica, starting with networking", log_node_id);

        let network_info = RP::init_default_information(reconfig_node)?;

        let node = Arc::new(NT::bootstrap(network_info.clone(), node_config).await?);

        let (reconf_tx, reconf_rx) = channel::new_bounded_sync(REPLICA_MESSAGE_CHANNEL,
                                                               Some("Reconfiguration Channel message"));
        let (reconf_response_tx, reply_rx) = channel::new_bounded_sync(REPLICA_MESSAGE_CHANNEL,
                                                                       Some("Reconfiguration Channel Response Message"));

        let default_timeout = Duration::from_secs(3);

        let (exec_tx, exec_rx) = channel::new_bounded_sync(REPLICA_MESSAGE_CHANNEL, None);

        debug!("{:?} // Initializing timeouts", log_node_id);

        // start timeouts handler
        let timeouts = Timeouts::new::<D>(log_node_id.clone(), Duration::from_millis(1),
                                          default_timeout, exec_tx.clone());

        let replica_node_args = ReconfigurableNodeTypes::QuorumNode(reconf_tx, reply_rx);

        debug!("{:?} // Initializing reconfiguration protocol", log_node_id);
        let reconfig_protocol = RP::initialize_protocol(network_info, node.clone(), timeouts.clone(), replica_node_args, OP::get_n_for_f(1)).await?;

        info!("{:?} // Waiting for reconfiguration protocol to stabilize", log_node_id);

        let mut quorum = loop {
            let message = reconf_rx.recv().unwrap();

            match message {
                QuorumReconfigurationMessage::ReconfigurationProtocolStable(quorum) => {
                    reconf_response_tx.send(QuorumReconfigurationResponse::QuorumStableResponse(true)).unwrap();

                    break quorum;
                }
                _ => {
                    error!("Received request for quorum view alteration, but we are not even done with reconfiguration stabilization?");

                    return Err(Error::simple_with_msg(ErrorKind::ReconfigurationNotStable, "Received alteration request before stable quorum was reached"));
                }
            }
        };

        if quorum.len() < OP::get_n_for_f(1) {
            error!("Received a stable message, but the quorum is not big enough?");

            return Err(Error::simple_with_msg(ErrorKind::ReconfigurationNotStable, "The reconfiguration protocol is not stable with , but we received a stable message?"));
        }

        info!("{:?} // Reconfiguration protocol stabilized with {} nodes ({:?}), starting replica", log_node_id, quorum.len(), quorum);

        let (rq_pre_processor, batch_input) = initialize_request_pre_processor
            ::<WDRoundRobin, D, OP::Serialization, ST::Serialization, LT::Serialization, VT::Serialization, NT>(4, node.clone());

        let persistent_log = PL::init_log::<String, NoPersistentLog, OP, ST, DL>(executor.clone(), db_path)?;

        let log = persistent_log.read_decision_log(OperationMode::BlockingSync)?;

        let op_args = OrderingProtocolArgs(executor.clone(), timeouts.clone(),
                                           rq_pre_processor.clone(),
                                           batch_input, node.clone(),
                                           quorum.clone());

        let decision_log = DL::initialize_decision_log(dl_config, persistent_log.clone(), executor.clone())?;

        let ordering_protocol = OP::initialize(op_config, op_args)?;

        let view_transfer_protocol = VT::initialize_view_transfer_protocol(vt_config, node.clone(), quorum.clone())?;

        let log_transfer_protocol = LT::initialize(lt_config, timeouts.clone(), node.clone(), persistent_log.clone())?;

        info!("{:?} // Finished bootstrapping node.", log_node_id);

        let timeout_channel = channel::new_bounded_sync(1024, Some("SMR Timeout work channel"));

        let state_transfer = ReplicaPhase::StateTransferProtocol {
            state_transfer: None,
            log_transfer: None,
        };

        let mut replica = Self {
// We start with the state transfer protocol to make sure everything is up to date
            replica_phase: state_transfer,
            quorum_reconfig_data: QuorumReconfig { node_pending_join: None },
            ordering_protocol,
            decision_log,
            log_transfer_protocol,
            view_transfer_protocol,
            rq_pre_processor,
            timeouts,
            executor_handle: executor,
            node,
            execution: (exec_rx, exec_tx),
            processed_timeout: timeout_channel,
            reconf_receive: reconf_rx,
            reconf_tx: reconf_response_tx,
            persistent_log,
            reconfig_protocol,
            st: Default::default(),
        };

        let view = replica.view();

        info!("{:?} // Requesting state", log_node_id);

        replica.log_transfer_protocol.request_latest_log(&mut replica.decision_log, view)?;

        Ok(replica)
    }

    fn id(&self) -> NodeId {
        NetworkNode::id(&*self.node)
    }

    pub fn run(&mut self, state_transfer: &mut ST) -> Result<()> {
        let now = Instant::now();

        self.receive_internal(state_transfer)?;

        metric_duration(REPLICA_INTERNAL_PROCESS_TIME_ID, now.elapsed());

        match &self.replica_phase {
            ReplicaPhase::OrderingProtocol(opphase) => {
                self.run_ordering_phase(state_transfer, opphase.clone())
            }
            ReplicaPhase::StateTransferProtocol { state_transfer: st_transfer_done, log_transfer: log_transfer_done } => {
                self.run_st_phase(state_transfer, st_transfer_done.clone(), log_transfer_done.clone())
            }
        }
    }

    fn run_ordering_phase(&mut self, state_transfer: &mut ST, op_phase: OPPhase<D::Request>) -> Result<()> {
        match op_phase {
            OPPhase::OrderProtocol => {
                let poll_res = self.ordering_protocol.poll()?;

                trace!("{:?} // Polling ordering protocol with result {:?}", NetworkNode::id(&*self.node), poll_res);

                match poll_res {
                    OPPollResult::RePoll => {}
                    OPPollResult::RunCst => {
                        self.run_all_state_transfer(state_transfer)?;
                    }
                    OPPollResult::ReceiveMsg => {
                        let start = Instant::now();

                        let network_message = self.node.node_incoming_rq_handling().receive_from_replicas(Some(REPLICA_WAIT_TIME)).unwrap();

                        metric_duration(REPLICA_TAKE_FROM_NETWORK_ID, start.elapsed());

                        if let Some(network_message) = network_message {
                            let (header, message) = network_message.into_inner();

                            match message {
                                SystemMessage::ProtocolMessage(protocol) => {
                                    let message = Arc::new(ReadOnly::new(StoredMessage::new(header, protocol.into_inner())));

                                    self.execute_order_protocol_message(state_transfer, message)?;
                                }
                                SystemMessage::ViewTransferMessage(view_transfer) => {
                                    let strd_msg = StoredMessage::new(header, view_transfer.into_inner());

                                    self.handle_view_transfer_msg(strd_msg)?;
                                }
                                SystemMessage::StateTransferMessage(state_transfer_msg) => {
                                    let strd_message = StoredMessage::new(header, state_transfer_msg.into_inner());

                                    state_transfer.handle_off_ctx_message(self.view(), strd_message)?;
                                }
                                SystemMessage::ForwardedRequestMessage(fwd_reqs) => {
// Send the forwarded requests to be handled, filtered and then passed onto the ordering protocol
                                    self.rq_pre_processor.send(PreProcessorMessage::ForwardedRequests(StoredMessage::new(header, fwd_reqs))).unwrap();
                                }
                                SystemMessage::ForwardedProtocolMessage(fwd_protocol) => {
                                    let message = fwd_protocol.into_inner();

                                    let (header, message) = message.into_inner();

                                    let message = Arc::new(ReadOnly::new(StoredMessage::new(header, message.into_inner())));

                                    self.execute_order_protocol_message(state_transfer, message)?;
                                }
                                SystemMessage::LogTransferMessage(log_transfer) => {
                                    let strd_msg = StoredMessage::new(header, log_transfer.into_inner());

                                    let view = self.view();

                                    self.log_transfer_protocol.handle_off_ctx_message(&mut self.decision_log, view,
                                                                                      strd_msg).unwrap();
                                }
                                _ => {
                                    error!("{:?} // Received unsupported message {:?}", NetworkNode::id(&*self.node), message);
                                }
                            }
                        } else {
                            // Receive timeouts in the beginning of the next iteration
                            return Ok(());
                        }
                    }
                    OPPollResult::Exec(message) => {
                        self.execute_order_protocol_message(state_transfer, message)?;
                    }
                    OPPollResult::ProgressedDecision(clear_ahead, decision) => {
                        match clear_ahead {
                            DecisionsAhead::Ignore => {}
                            DecisionsAhead::ClearAhead => {
                                self.clear_currently_executing()?;
                            }
                        }

                        for decision_information in decision.into_iter() {
                            let received_info = self.decision_log.decision_information_received(decision_information)?;

                            self.execute_logged_decisions(state_transfer, received_info)?;
                        }
                    }
                    OPPollResult::QuorumJoined(clear_ahead, decision, join_info) => {
                        match clear_ahead {
                            DecisionsAhead::Ignore => {}
                            DecisionsAhead::ClearAhead => {
                                self.clear_currently_executing()?;
                            }
                        }

                        let (node_id, quorum) = join_info.into_inner();

                        self.handle_quorum_joined(node_id, quorum, state_transfer)?;

                        if let Some(decision) = decision {
                            for decision in decision.into_iter() {
                                let received_info = self.decision_log.decision_information_received(decision)?;

                                self.execute_logged_decisions(state_transfer, received_info)?;
                            }
                        }
                    }
                }
            }
            OPPhase::ViewTransferProtocol(_) => {
                self.iterate_view_transfer_protocol(state_transfer)?;
            }
        }

        Ok(())
    }

    /// Runs the state transfer protocol side of the SMR
    fn run_st_phase(&mut self, state_transfer: &mut ST, st_transfer_done: StateTransferDone, log_transfer_done: LogTransferDone<D::Request>) -> Result<()> {
        let lt_res = if !Self::is_log_transfer_done(&log_transfer_done) {
            match self.log_transfer_protocol.poll()? {
                LTPollResult::ReceiveMsg => {
                    LTPollRes::Receive
                }
                LTPollResult::RePoll => {
                    return Ok(());
                }
                LTPollResult::LTResult(res) => {
                    self.handle_log_transfer_result(state_transfer, res)?;

                    LTPollRes::Continue
                }
                LTPollResult::Exec(message) => {
                    let view = self.view();

                    let res = self.log_transfer_protocol.process_message(&mut self.decision_log,
                                                                         view, message)?;

                    self.handle_log_transfer_result(state_transfer, res)?;
                    LTPollRes::Continue
                }
            }
        } else { LTPollRes::None };

        let st_res = if !Self::is_state_transfer_done(&st_transfer_done) {
            match state_transfer.poll()? {
                STPollResult::ReceiveMsg => {
                    STPollRes::Receive
                }
                STPollResult::RePoll => {
                    return Ok(());
                }
                STPollResult::Exec(message) => {
                    let res = state_transfer.process_message(self.view(), message)?;

                    self.handle_state_transfer_result(state_transfer, res)?;
                    STPollRes::Receive
                }
                STPollResult::STResult(res) => {
                    self.handle_state_transfer_result(state_transfer, res)?;
                    STPollRes::Receive
                }
            }
        } else { STPollRes::None };

        match (lt_res, st_res) {
            (LTPollRes::Continue, _) | (_, STPollRes::Continue) => {
                return Ok(());
            }
            (_, _) => {}
        };

        let message = self.node.node_incoming_rq_handling().receive_from_replicas(Some(REPLICA_WAIT_TIME)).unwrap();

        if let Some(message) = message {
            let (header, message) = message.into_inner();

            match message {
                SystemMessage::ProtocolMessage(protocol) => {
                    let shareable_message = Arc::new(ReadOnly::new(StoredMessage::new(header, protocol.into_inner())));

                    self.ordering_protocol.handle_off_ctx_message(shareable_message);
                }
                SystemMessage::ViewTransferMessage(view_transfer) => {
                    self.handle_view_transfer_msg(StoredMessage::new(header, view_transfer.into_inner()))?;
                }
                SystemMessage::StateTransferMessage(state_transfer_msg) => {
                    let start = Instant::now();

                    let strd_message = StoredMessage::new(header, state_transfer_msg.into_inner());

                    let result = state_transfer.process_message(self.view(), strd_message)?;

                    self.handle_state_transfer_result(state_transfer, result)?;

                    metric_duration(STATE_TRANSFER_PROCESS_TIME_ID, start.elapsed());
                }
                SystemMessage::LogTransferMessage(log_transfer) => {
                    let start = Instant::now();

                    let message = StoredMessage::new(header, log_transfer.into_inner());

                    let view = self.view();

                    let result = self.log_transfer_protocol.process_message(&mut self.decision_log, view, message)?;

                    self.handle_log_transfer_result(state_transfer, result)?;

                    metric_duration(LOG_TRANSFER_PROCESS_TIME_ID, start.elapsed());
                }
                _ => {}
            }
        }

        Ok(())
    }

    fn handle_log_transfer_result(&mut self, state_transfer: &mut ST, result: LTResult<D>) -> Result<()> {
        match result {
            LTResult::RunLTP => {
                self.run_log_transfer_protocol(state_transfer)?;
            }
            LTResult::Running => {}
            LTResult::NotNeeded => {
                let log = self.decision_log.current_log()?;

                self.log_transfer_protocol_done(state_transfer, log.first_seq().unwrap_or(SeqNo::ZERO), log.sequence_number(), MaybeVec::None)?;
            }
            LTResult::LTPFinished(first_seq, last_seq, requests_to_execute) => {
                info!("{:?} // Log transfer finished. Installed log into decision log and received requests", NetworkNode::id(&*self.node));
                self.log_transfer_protocol_done(state_transfer, first_seq, last_seq, requests_to_execute)?;
            }
            LTResult::InstallSeq(seq) => {
                info!("{:?} // Log transfer protocol indicated Sequence number, installing it into the ordering protocol", NetworkNode::id(&*self.node));

                self.ordering_protocol.install_seq_no(seq)?;
            }
        }

        Ok(())
    }

    fn handle_state_transfer_result(&mut self, state_transfer: &mut ST, result: STResult) -> Result<()> {
        match result {
            STResult::StateTransferRunning => {}
            STResult::StateTransferReady => {
                self.executor_handle.poll_state_channel()?;
            }
            STResult::StateTransferFinished(seq_no) => {
                info!("{:?} // State transfer finished. Installing state in executor and running ordering protocol", NetworkNode::id(&*self.node));

                self.executor_handle.poll_state_channel()?;

                self.state_transfer_protocol_done(state_transfer, seq_no)?;
            }
            STResult::StateTransferNotNeeded(curr_seq) => {
                self.state_transfer_protocol_done(state_transfer, curr_seq)?;
            }
            STResult::RunStateTransfer => {
                self.run_state_transfer_protocol(state_transfer)?;
            }
        }

        Ok(())
    }

    fn execute_order_protocol_message(&mut self, state_transfer: &mut ST, message: ShareableMessage<ProtocolMessage<D, OP::Serialization>>) -> Result<()> {
        let start = Instant::now();

        let exec_result = self.ordering_protocol.process_message(message)?;

        metric_duration(ORDERING_PROTOCOL_PROCESS_TIME_ID, start.elapsed());

        let start = Instant::now();

        match exec_result {
            OPExecResult::MessageDropped | OPExecResult::MessageQueued | OPExecResult::MessageProcessedNoUpdate => {}
            OPExecResult::ProgressedDecision(clear_decisions, decision) => {
                match clear_decisions {
                    DecisionsAhead::Ignore => {}
                    DecisionsAhead::ClearAhead => {
                        self.clear_currently_executing()?;
                    }
                }

                for decision_information in decision.into_iter() {
                    let decision_info = self.decision_log.decision_information_received(decision_information)?;

                    self.execute_logged_decisions(state_transfer, decision_info)?;
                }
            }
            OPExecResult::QuorumJoined(clear_decisions, decision, quorum) => {
                match clear_decisions {
                    DecisionsAhead::Ignore => {}
                    DecisionsAhead::ClearAhead => {
                        self.clear_currently_executing()?;
                    }
                }

                let (node_id, quorum) = quorum.into_inner();

                self.handle_quorum_joined(node_id, quorum, state_transfer)?;

                if let Some(decision) = decision {
                    for decision in decision.into_iter() {
                        let decision_info = self.decision_log.decision_information_received(decision)?;

                        self.execute_logged_decisions(state_transfer, decision_info)?;
                    }
                }
            }
            OPExecResult::RunCst => {
                self.run_all_state_transfer(state_transfer)?;
            }
        }

        metric_duration(REPLICA_PROTOCOL_RESP_PROCESS_TIME_ID, start.elapsed());
        metric_increment(REPLICA_ORDERED_RQS_PROCESSED_ID, Some(1));

        Ok(())
    }

    fn clear_currently_executing(&mut self) -> Result<()> {
        self.decision_log.clear_decisions_forward(self.decision_log.sequence_number())
    }

    fn execute_logged_decisions(&mut self, state_transfer: &mut ST, decisions: MaybeVec<LoggedDecision<D::Request>>) -> Result<()> {
        for decision in decisions.into_iter() {
            let (seq, requests, to_batch) = decision.into_inner();

            if let Err(err) = self.rq_pre_processor.send(PreProcessorMessage::DecidedBatch(requests)) {
                error!("Error sending decided batch to pre processor: {:?}", err);
            }

            let last_seq_no_u32 = u32::from(seq);

            let checkpoint = if last_seq_no_u32 > 0 && last_seq_no_u32 % CHECKPOINT_PERIOD == 0 {
//We check that % == 0 so we don't start multiple checkpoints
                state_transfer.handle_app_state_requested(self.view(), seq)?
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

    /// FIXME: Do this with a select?
    fn receive_internal(&mut self, state_transfer: &mut ST) -> Result<()> {
        while let Ok(recvd) = self.execution.0.try_recv() {
            match recvd {
                Message::Timeout(timeout) => {
                    self.timeout_received(state_transfer, timeout)?;
//info!("{:?} // Received and ignored timeout with {} timeouts {:?}", self.node.id(), timeout.len(), timeout);
                }
                _ => {}
            }
        }

        while let Ok(received) = self.reconf_receive.try_recv() {
            match received {
                QuorumReconfigurationMessage::RequestQuorumJoin(node) => {
                    info!("Received request for quorum view alteration for {:?}, current phase: {:?}", node, self.replica_phase);

                    self.attempt_quorum_join(node)?;
                }
                QuorumReconfigurationMessage::AttemptToJoinQuorum => {
                    info!("Received request to attempt to join quorum, current phase: {:?}", self.replica_phase);

                    self.attempt_to_join_quorum()?;
                }
                QuorumReconfigurationMessage::QuorumUpdated(new_quorum) => {
                    info!("Received quorum updated message, dealing with it");

                    // If we receive a quorum updated message, that means that we are not a part of the quorum,
                    // and there is probably new information that we need to know about from the ordering protocol.
                    //TODO: Here we want to check if we are currently attempting to join and if we are then take appropriate actions
                    if new_quorum.contains(&self.id()) {
                        unreachable!("We are a part of the quorum and we have received a quorum updated message? This information should come from the ordering protocol instead");
                    } else {
                        // We are not a part of the quorum, so we need to start the state transfer protocol
                        // In order to receive any new information about the ordering protocol status (Like new views)

                        error!("TODO: Handle quorum updated")
                    }
                }
                QuorumReconfigurationMessage::ReconfigurationProtocolStable(_) => {
                    info!("Received reconfiguration protocol stable but we are already done?");
                }
            }
        }

        while let Ok((timeouts, deleted)) = self.processed_timeout.1.try_recv() {
            self.processed_timeout_recvd(state_transfer, timeouts, deleted)?;
        }

        Ok(())
    }

    fn timeout_received(&mut self, state_transfer: &mut ST, timeouts: TimedOut) -> Result<()> {
        let start = Instant::now();

        let mut client_rq = Vec::with_capacity(timeouts.len());
        let mut cst_rq = Vec::new();
        let mut log_transfer = Vec::new();
        let mut reconfiguration = Vec::new();

        for timeout in timeouts {
            match timeout.timeout_kind() {
                TimeoutKind::ClientRequestTimeout(_) => {
                    client_rq.push(timeout);
                }
                TimeoutKind::Cst(_) => {
                    cst_rq.push(timeout);
                }
                TimeoutKind::LogTransfer(_) => {
                    log_transfer.push(timeout);
                }
                TimeoutKind::Reconfiguration(_) => {
                    reconfiguration.push(timeout);
                }
            }
        }

        if !client_rq.is_empty() {
            debug!("{:?} // Received client request timeouts: {}", NetworkNode::id(&*self.node), client_rq.len());

            self.rq_pre_processor.process_timeouts(client_rq, self.processed_timeout.0.clone());
        }

        if !cst_rq.is_empty() {
            debug!("{:?} // Received cst timeouts: {}", NetworkNode::id(&*self.node), cst_rq.len());

            match state_transfer.handle_timeout(self.view(), cst_rq)? {
                STTimeoutResult::RunCst => {
                    self.run_state_transfer_protocol(state_transfer)?;
                }
                _ => {}
            };
        }

        if !log_transfer.is_empty() {
            debug!("{:?} // Received log transfer timeouts: {}", NetworkNode::id(&*self.node), log_transfer.len());

            match self.log_transfer_protocol.handle_timeout(self.view(), log_transfer)? {
                LTTimeoutResult::RunLTP => {
                    self.run_log_transfer_protocol(state_transfer)?;
                }
                _ => {}
            };
        }

        if !reconfiguration.is_empty() {
            debug!("{:?} // Received reconfiguration timeouts: {}", NetworkNode::id(&*self.node), reconfiguration.len());

            self.reconfig_protocol.handle_timeout(reconfiguration)?;
        }

        metric_duration(TIMEOUT_PROCESS_TIME_ID, start.elapsed());

        Ok(())
    }

    // Process a processed timeout request
    fn processed_timeout_recvd(&mut self, state_transfer: &mut ST, timed_out: Vec<RqTimeout>, to_delete: Vec<RqTimeout>) -> Result<()> {
        self.timeouts.cancel_client_rq_timeouts(Some(to_delete.into_iter().map(|t| match t.into_timeout_kind() {
            TimeoutKind::ClientRequestTimeout(info) => {
                info
            }
            _ => unreachable!()
        }).collect()));

        match self.ordering_protocol.handle_timeout(timed_out)? {
            OPExecResult::RunCst => {
                self.run_all_state_transfer(state_transfer)?;
            }
            _ => {}
        };

        Ok(())
    }

    /// Run the ordering protocol on this replica
    fn run_ordering_protocol(&mut self) -> Result<()> {
        info!("{:?} // Running ordering protocol.", NetworkNode::id(&*self.node));

        let phase = std::mem::replace(&mut self.replica_phase, ReplicaPhase::OrderingProtocol(OPPhase::OrderProtocol));

        match phase {
            ReplicaPhase::OrderingProtocol(_) => {}
            ReplicaPhase::StateTransferProtocol { log_transfer, state_transfer } => {
                if let Some((log_first, log_last, requests_to_execute)) = log_transfer {
                    /// deliver the requests to the executor

                    let mut requests = MaybeVec::builder();

                    requests_to_execute.into_iter().for_each(|decision| {
                        let (_, _, logged_val) = decision.into_inner();

                        match logged_val {
                            LoggedDecisionValue::Execute(mut rqs) => {
                                requests.push(rqs);
                            }
                            _ => {}
                        }
                    });

                    self.executor_handle.catch_up_to_quorum(requests.build())?;
                }
            }
        }

        self.replica_phase = ReplicaPhase::OrderingProtocol(OPPhase::OrderProtocol);

        self.ordering_protocol.handle_execution_changed(true)?;

        if let Some(node) = self.quorum_reconfig_data.pop_pending_node_join() {
            self.attempt_quorum_join(node)?;
        }

        Ok(())
    }

    /// Is the log transfer protocol finished?
    fn is_log_transfer_done(log: &LogTransferDone<D::Request>) -> bool {
        return log.is_some();
    }

    /// Is the state transfer protocol finished?
    fn is_state_transfer_done(state: &StateTransferDone) -> bool {
        return state.is_some();
    }

    /// Mark the log transfer protocol as done
    fn log_transfer_protocol_done(&mut self, state_transfer_protocol: &mut ST, first_seq: SeqNo, last_seq: SeqNo, requests_to_execute: MaybeVec<LoggedDecision<D::Request>>) -> Result<()> {
        let log_transfer_protocol_done = match &mut self.replica_phase {
            ReplicaPhase::OrderingProtocol(_) => false,
            ReplicaPhase::StateTransferProtocol { log_transfer, state_transfer } => {
                *log_transfer = Some((first_seq, last_seq, requests_to_execute));

                if Self::is_log_transfer_done(log_transfer) & &Self::is_state_transfer_done(state_transfer) {
                    true
                } else {
                    false
                }
            }
        };

        if log_transfer_protocol_done {
            self.finish_state_transfer(state_transfer_protocol)?;
        }

        Ok(())
    }

    /// Mark the state transfer protocol as done
    fn state_transfer_protocol_done(&mut self, state_transfer_protocol: &mut ST, seq_no: SeqNo) -> Result<()> {
        let state_transfer_protocol_done = match &mut self.replica_phase {
            ReplicaPhase::OrderingProtocol(_) => false,
            ReplicaPhase::StateTransferProtocol { state_transfer, log_transfer } => {
                *state_transfer = Some(seq_no);

                if Self::is_log_transfer_done(log_transfer) & &Self::is_state_transfer_done(state_transfer) {
                    true
                } else {
                    false
                }
            }
        };

        if state_transfer_protocol_done {
            self.finish_state_transfer(state_transfer_protocol)?;
        }

        Ok(())
    }

    /// Handle the log transfer and the state transfer protocol finishing
    /// their execution
    fn finish_state_transfer(&mut self, state_transfer_protocol: &mut ST) -> Result<()> {
        match &self.replica_phase {
            ReplicaPhase::OrderingProtocol(_) => {}
            ReplicaPhase::StateTransferProtocol { state_transfer, log_transfer } => {
                let state_transfer = state_transfer.clone().unwrap();
                let (log_first, log_last, _) = log_transfer.as_ref().unwrap();

// If both the state and the log start at 0, then we can just run the ordering protocol since
// There is no state currently present.
                if state_transfer.next() != *log_first && (state_transfer != SeqNo::ZERO && *log_first != SeqNo::ZERO) {
                    error!("{:?} // Log transfer protocol and state transfer protocol are not in sync. Received {:?} state and {:?} - {:?} log", self.id(), state_transfer, * log_first, * log_last);

// Run both the protocols again
// This might work better since we already have a more up-to-date state (in
// The case of a hugely large state) so the state transfer protocol should take less time
                    self.run_all_state_transfer(state_transfer_protocol)?;
                } else {
                    info!("{:?} // State transfer protocol and log transfer protocol are in sync. Received {:?} state and {:?} - {:?} log", self.id(), state_transfer, * log_first, * log_last);

                    /// If the protocols are lined up so we can start running the ordering protocol
                    self.run_ordering_protocol()?;
                }
            }
        }

        Ok(())
    }

    /// Run the state transfer and log transfer protocols
    fn run_all_state_transfer(&mut self, state_transfer: &mut ST) -> Result<()> {
        info!("{:?} // Running state and log transfer protocols. {:?}", NetworkNode::id(&*self.node), self.replica_phase);

        match &mut self.replica_phase {
            ReplicaPhase::OrderingProtocol(_) => {
                self.ordering_protocol.handle_execution_changed(false)?;

                self.replica_phase = ReplicaPhase::StateTransferProtocol {
                    state_transfer: None,
                    log_transfer: None,
                };
            }
            ReplicaPhase::StateTransferProtocol { state_transfer, log_transfer } => {
                warn!("{:?} // Why would we want to run the protocols when we are already running them?", NetworkNode::id(&*self.node));

                return Ok(());
            }
        }

        let view = self.view();

// Start by requesting the current state from neighbour replicas
        state_transfer.request_latest_state(view.clone())?;
        self.log_transfer_protocol.request_latest_log(&mut self.decision_log, view)?;

        Ok(())
    }

    /// Run the state transfer protocol on this replica
    fn run_state_transfer_protocol(&mut self, state_transfer_p: &mut ST) -> Result<()> {
        info!("{:?} // Running state transfer protocol. {:?}", NetworkNode::id(&*self.node), self.replica_phase);

        match &mut self.replica_phase {
            ReplicaPhase::OrderingProtocol(_) => {
                self.run_all_state_transfer(state_transfer_p)?;
            }
            ReplicaPhase::StateTransferProtocol { state_transfer, .. } => {
                warn!("{:?} // Why would we want to run the state transfer protocol when we are already running it?", NetworkNode::id(&*self.node));

                *state_transfer = None;

                state_transfer_p.request_latest_state(self.view())?;
            }
        }

        Ok(())
    }

    /// Runs the log transfer protocol on this replica
    fn run_log_transfer_protocol(&mut self, state_transfer: &mut ST) -> Result<()> {
        info!("{:?} // Running log transfer protocol. {:?}", NetworkNode::id(&*self.node), self.replica_phase);

        match &mut self.replica_phase {
            ReplicaPhase::OrderingProtocol(_) => {
                self.run_all_state_transfer(state_transfer)?;
            }
            ReplicaPhase::StateTransferProtocol { log_transfer, .. } => {
                warn!("{:?} // Why would we want to run the log transfer protocol when we are already running it?", NetworkNode::id(&*self.node));

                *log_transfer = None;

                let view = self.view();

                self.log_transfer_protocol.request_latest_log(&mut self.decision_log, view)?;
            }
        }

        Ok(())
    }

    /// We are pending a node join
    fn append_pending_join_node(&mut self, node: NodeId) -> bool {
        self.quorum_reconfig_data.append_pending_node_join(node)
    }

    fn handle_quorum_joined(&mut self, node: NodeId, members: Vec<NodeId>, state_transfer: &mut ST) -> Result<()> {
        if node == self.id() {
            info!("{:?} // We have joined the quorum, responding to the reconfiguration protocol", self.id());

            self.reply_to_attempt_quorum_join(false)?;

            return Ok(());
        }

        self.reply_to_quorum_entrance_request(node, None)?;

        Ok(())
    }

    fn reply_to_attempt_quorum_join(&mut self, failed: bool) -> Result<()> {
        info!("{:?} // Sending attempt quorum join response to reconfiguration protocol. Has failed: {:?}", self.id(), failed);

        if failed {
            self.reconf_tx.send(QuorumReconfigurationResponse::QuorumAttemptJoinResponse(QuorumAttemptJoinResponse::Failed))
                .wrapped_msg(ErrorKind::CommunicationChannel, "Error sending quorum entrance response to reconfiguration protocol")?;
        } else {
            self.reconf_tx.send(QuorumReconfigurationResponse::QuorumAttemptJoinResponse(QuorumAttemptJoinResponse::Success))
                .wrapped_msg(ErrorKind::CommunicationChannel, "Error sending quorum entrance response to reconfiguration protocol")?;
        }

        Ok(())
    }

    /// Send the result of attempting to join the quorum to the reconfiguration protocol, so that
    /// it can proceed with execution
    fn reply_to_quorum_entrance_request(&mut self, node_id: NodeId, failed_reason: Option<AlterationFailReason>) -> Result<()> {
        info!("{:?} // Sending quorum entrance response to reconfiguration protocol with success: {:?}", self.id(), failed_reason);

        match failed_reason {
            None => {
                self.reconf_tx.send(QuorumReconfigurationResponse::QuorumAlterationResponse(QuorumAlterationResponse::Successful(node_id)))
                    .wrapped_msg(ErrorKind::CommunicationChannel, "Error sending quorum entrance response to reconfiguration protocol")?;
            }
            Some(fail_reason) => {
                self.reconf_tx.send(QuorumReconfigurationResponse::QuorumAlterationResponse(QuorumAlterationResponse::Failed(node_id, fail_reason)))
                    .wrapped_msg(ErrorKind::CommunicationChannel, "Error sending quorum entrance response to reconfiguration protocol")?;
            }
        }

        Ok(())
    }
}

impl QuorumReconfig {
    /// Attempt to register the node
    fn append_pending_node_join(&mut self, node: NodeId) -> bool {
        if let None = self.node_pending_join {
            self.node_pending_join = Some(node);
            true
        } else {
            false
        }
    }

    fn pop_pending_node_join(&mut self) -> Option<NodeId> {
        std::mem::replace(&mut self.node_pending_join, None)
    }
}

impl<RP, S, D, OP, DL, ST, LT, VT, NT, PL> PermissionedProtocolHandling<S, VT, OP, ST, NT, PL> for Replica<RP, S, D, OP, DL, ST, LT, VT, NT, PL>
    where RP: ReconfigurationProtocol + 'static,
          D: ApplicationData + 'static,
          OP: LoggableOrderProtocol<D, NT> + 'static,
          DL: DecisionLog<D, OP, NT, PL> + 'static,
          LT: LogTransferProtocol<D, OP, DL, NT, PL> + 'static,
          ST: StateTransferProtocol<S, NT, PL> + PersistableStateTransferProtocol + Send + 'static,
          VT: ViewTransferProtocol<OP, NT> + 'static,
          NT: SMRNetworkNode<RP::InformationProvider, RP::Serialization, D, OP::Serialization, ST::Serialization, LT::Serialization, VT::Serialization> + 'static,
          PL: SMRPersistentLog<D, OP::Serialization, OP::PersistableTypes, DL::LogSerialization> + 'static, {
    default type View = MockView;

    default fn view(&self) -> Self::View {
        todo!()
    }

    default fn run_view_transfer(&mut self) -> Result<()>
        where NT: ViewTransferProtocolSendNode<VT::Serialization> {
        todo!()
    }

    default fn iterate_view_transfer_protocol(&mut self, state_transfer: &mut ST) -> Result<()> where NT: ViewTransferProtocolSendNode<VT::Serialization>, ST: StateTransferProtocol<S, NT, PL> {
        todo!()
    }

    default fn handle_view_transfer_msg(&mut self, message: StoredMessage<VTMsg<VT::Serialization>>) -> Result<()>
        where NT: ViewTransferProtocolSendNode<VT::Serialization> {
        todo!()
    }
}

impl<RP, S, D, OP, DL, ST, LT, VT, NT, PL> PermissionedProtocolHandling<S, VT, OP, ST, NT, PL> for Replica<RP, S, D, OP, DL, ST, LT, VT, NT, PL>
    where RP: ReconfigurationProtocol + 'static,
          D: ApplicationData + 'static,
          OP: LoggableOrderProtocol<D, NT> + PermissionedOrderingProtocol + 'static,
          DL: DecisionLog<D, OP, NT, PL> + 'static,
          LT: LogTransferProtocol<D, OP, DL, NT, PL> + 'static,
          ST: StateTransferProtocol<S, NT, PL> + PersistableStateTransferProtocol + Send + 'static,
          VT: ViewTransferProtocol<OP, NT> + 'static,
          NT: SMRNetworkNode<RP::InformationProvider, RP::Serialization, D, OP::Serialization, ST::Serialization, LT::Serialization, VT::Serialization> + 'static,
          PL: SMRPersistentLog<D, OP::Serialization, OP::PersistableTypes, DL::LogSerialization> + 'static, {
    type View = View<OP::PermissionedSerialization>;

    fn view(&self) -> Self::View {
        self.ordering_protocol.view()
    }

    fn run_view_transfer(&mut self) -> Result<()> where NT: ViewTransferProtocolSendNode<VT::Serialization> {
        self.replica_phase = ReplicaPhase::OrderingProtocol(OPPhase::ViewTransferProtocol(None));

        //TODO: What to do if called when running log/state transfer

        self.view_transfer_protocol.request_latest_view(&self.ordering_protocol)?;

        Ok(())
    }

    fn iterate_view_transfer_protocol(&mut self, state_transfer: &mut ST) -> Result<()>
        where NT: ViewTransferProtocolSendNode<VT::Serialization>,
              ST: StateTransferProtocol<S, NT, PL> {
        match self.view_transfer_protocol.poll()? {
            VTPollResult::RePoll => {}
            VTPollResult::Exec(msg) => {
                self.handle_view_transfer_msg(msg)?;
            }
            VTPollResult::ReceiveMsg => {
                let rcvd_msg = self.node.node_incoming_rq_handling().receive_from_replicas(None)?;

                if let Some(msg) = rcvd_msg {
                    let (header, message) = msg.into_inner();

                    match message {
                        SystemMessage::ProtocolMessage(protocol) => {
                            let message = Arc::new(ReadOnly::new(StoredMessage::new(header, protocol.into_inner())));

                            self.ordering_protocol.handle_off_ctx_message(message);
                        }
                        SystemMessage::ViewTransferMessage(view_transfer) => {
                            let strd_msg = StoredMessage::new(header, view_transfer.into_inner());

                            self.handle_view_transfer_msg(strd_msg)?;
                        }
                        SystemMessage::StateTransferMessage(state_transfer_msg) => {
                            let strd_msg = StoredMessage::new(header, state_transfer_msg.into_inner());

                            state_transfer.handle_off_ctx_message(self.view(), strd_msg)?;
                        }
                        SystemMessage::ForwardedRequestMessage(fwd_reqs) => {
// Send the forwarded requests to be handled, filtered and then passed onto the ordering protocol
                            self.rq_pre_processor.send(PreProcessorMessage::ForwardedRequests(StoredMessage::new(header, fwd_reqs))).unwrap();
                        }
                        SystemMessage::ForwardedProtocolMessage(fwd_protocol) => {
                            let message = fwd_protocol.into_inner();

                            let (header, message) = message.into_inner();

                            let message = Arc::new(ReadOnly::new(StoredMessage::new(header, message.into_inner())));

                            self.ordering_protocol.handle_off_ctx_message(message);
                        }
                        SystemMessage::LogTransferMessage(log_transfer) => {
                            let view = self.view();

                            self.log_transfer_protocol.handle_off_ctx_message(&mut self.decision_log, view,
                                                                              StoredMessage::new(header, log_transfer.into_inner())).unwrap();
                        }
                        _ => {
                            error!("{:?} // Received unsupported message {:?}", NetworkNode::id(&*self.node), message);
                        }
                    }
                }
            }
            VTPollResult::VTResult(res) => {

            }
        }

        Ok(())
    }

    fn handle_view_transfer_msg(&mut self, message: StoredMessage<VTMsg<VT::Serialization>>) -> Result<()>
        where NT: ViewTransferProtocolSendNode<VT::Serialization> {
        match &self.replica_phase {
            ReplicaPhase::OrderingProtocol(op_phase) => {
                match op_phase {
                    OPPhase::OrderProtocol => {
                        self.view_transfer_protocol.handle_off_context_msg(&self.ordering_protocol, message)?;
                    }
                    OPPhase::ViewTransferProtocol(prev) => {
                        let vt_result = self.view_transfer_protocol.process_message(&mut self.ordering_protocol, message)?;

                        match vt_result {
                            VTResult::RunVTP => {}
                            VTResult::VTransferNotNeeded => {}
                            VTResult::VTransferRunning => {}
                            VTResult::VTransferFinished => {
                                if let Some(protocol_stack) = prev {} else {
                                    self.run_ordering_protocol()?;
                                }
                            }
                        }
                    }
                }
            }
            ReplicaPhase::StateTransferProtocol { .. } => {
                self.view_transfer_protocol.handle_off_context_msg(&self.ordering_protocol, message)?;
            }
        }


        todo!()
    }
}

/// Default protocol with no reconfiguration support handling
impl<RP, S, D, OP, DL, ST, LT, VT, NT, PL> ReconfigurableProtocolHandling for Replica<RP, S, D, OP, DL, ST, LT, VT, NT, PL>
    where RP: ReconfigurationProtocol + 'static,
          D: ApplicationData + 'static,
          OP: LoggableOrderProtocol<D, NT> + Send + 'static,
          DL: DecisionLog<D, OP, NT, PL> + 'static,
          LT: LogTransferProtocol<D, OP, DL, NT, PL> + 'static,
          VT: ViewTransferProtocol<OP, NT> + 'static,
          ST: StateTransferProtocol<S, NT, PL> + PersistableStateTransferProtocol + Send + 'static,
          NT: SMRNetworkNode<RP::InformationProvider, RP::Serialization, D, OP::Serialization, ST::Serialization, LT::Serialization, VT::Serialization> + 'static,
          PL: SMRPersistentLog<D, OP::Serialization, OP::PersistableTypes, DL::LogSerialization> + 'static, {
    default fn attempt_quorum_join(&mut self, node: NodeId) -> Result<()> {
        self.reply_to_quorum_entrance_request(node, Some(AlterationFailReason::Failed))
    }

    default fn attempt_to_join_quorum(&mut self) -> Result<()> {
        self.reply_to_attempt_quorum_join(true)
    }
}

/// Implement reconfigurable order protocol support
impl<RP, S, D, OP, DL, ST, LT, VT, NT, PL> ReconfigurableProtocolHandling for Replica<RP, S, D, OP, DL, ST, LT, VT, NT, PL>
    where RP: ReconfigurationProtocol + 'static,
          D: ApplicationData + 'static,
          OP: LoggableOrderProtocol<D, NT> + ReconfigurableOrderProtocol<RP::Serialization> + Send + 'static,
          DL: DecisionLog<D, OP, NT, PL> + 'static,
          LT: LogTransferProtocol<D, OP, DL, NT, PL> + 'static,
          ST: StateTransferProtocol<S, NT, PL> + PersistableStateTransferProtocol + Send + 'static,
          VT: ViewTransferProtocol<OP, NT> + 'static,
          NT: SMRNetworkNode<RP::InformationProvider, RP::Serialization, D, OP::Serialization, ST::Serialization, LT::Serialization, VT::Serialization> + 'static,
          PL: SMRPersistentLog<D, OP::Serialization, OP::PersistableTypes, DL::LogSerialization> + 'static, {
    fn attempt_quorum_join(&mut self, node: NodeId) -> Result<()> {
        match &self.replica_phase {
            ReplicaPhase::OrderingProtocol(op_phase) => {
                let quorum_node_join = self.ordering_protocol.attempt_quorum_node_join(node)?;

                debug!("{:?} // Attempting to join quorum with result {:?}", self.id(), quorum_node_join);

                match quorum_node_join {
                    ReconfigurationAttemptResult::Failed => {
                        self.reply_to_quorum_entrance_request(node, Some(AlterationFailReason::Failed))?;
                    }
                    ReconfigurationAttemptResult::AlreadyPartOfQuorum => {
                        self.reply_to_quorum_entrance_request(node, Some(AlterationFailReason::AlreadyPartOfQuorum))?;
                    }
                    ReconfigurationAttemptResult::CurrentlyReconfiguring(_) => {
                        self.reply_to_quorum_entrance_request(node, Some(AlterationFailReason::OngoingReconfiguration))?;
                    }
                    ReconfigurationAttemptResult::InProgress => {}
                    ReconfigurationAttemptResult::Successful => {
                        self.reply_to_quorum_entrance_request(node, None)?;
                    }
                };
            }
            ReplicaPhase::StateTransferProtocol { .. } => {
                self.append_pending_join_node(node);
            }
        }

        Ok(())
    }

    fn attempt_to_join_quorum(&mut self) -> Result<()> {
        match self.ordering_protocol.joining_quorum()? {
            ReconfigurationAttemptResult::Failed => {
                self.reply_to_attempt_quorum_join(true)?;
            }
            ReconfigurationAttemptResult::CurrentlyReconfiguring(_) => {
                self.reply_to_attempt_quorum_join(true)?;
            }
            ReconfigurationAttemptResult::InProgress => {}
            ReconfigurationAttemptResult::Successful | ReconfigurationAttemptResult::AlreadyPartOfQuorum => {
                // If we are already part of the quorum, then we can immediately reply to it
                self.reply_to_attempt_quorum_join(false)?;
            }
        };

        Ok(())
    }
}

#[derive(Clone, Debug)]
pub struct MockView(Vec<NodeId>);

impl Orderable for MockView {
    fn sequence_number(&self) -> SeqNo {
        SeqNo::ZERO
    }
}

impl NetworkView for MockView {
    fn primary(&self) -> NodeId {
        todo!()
    }

    fn quorum(&self) -> usize {
        todo!()
    }

    fn quorum_members(&self) -> &Vec<NodeId> {
        todo!()
    }

    fn f(&self) -> usize {
        todo!()
    }

    fn n(&self) -> usize {
        todo!()
    }
}

/// Checkpoint period.
///
/// Every `PERIOD` messages, the message log is cleared,
/// and a new log checkpoint is initiated.
/// TODO: Move this to an env variable as it can be highly dependent on the service implemented on top of it
pub const CHECKPOINT_PERIOD: u32 = 1000;

impl<R> PartialEq for ReplicaPhase<R> where {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (ReplicaPhase::OrderingProtocol(prot), ReplicaPhase::OrderingProtocol(prot2)) => *prot == *prot2,
            (ReplicaPhase::StateTransferProtocol { .. }, ReplicaPhase::StateTransferProtocol { .. }) => true,
            (_, _) => false
        }
    }
}

impl<R> Debug for OPPhase<R> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            OPPhase::OrderProtocol => {
                write!(f, "Order Protocol")
            }
            OPPhase::ViewTransferProtocol(previous) => {
                write!(f, "View Tranfer Protocol with Prev: {:?}", previous)
            }
        }
    }
}

impl<R> PartialEq<Self> for OPPhase<R> {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (OPPhase::OrderProtocol, OPPhase::OrderProtocol) => true,
            (OPPhase::ViewTransferProtocol(_), OPPhase::ViewTransferProtocol(_)) => true,
            _ => false
        }
    }
}

impl<R> Eq for OPPhase<R> {}

impl<R> Debug for ReplicaPhase<R> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            ReplicaPhase::OrderingProtocol(op_phase) => {
                write!(f, "OrderingProtocol {:?}", op_phase)
            }
            ReplicaPhase::StateTransferProtocol { log_transfer, state_transfer } => {
                write!(f, "StateTransferProtocol {:?}", state_transfer)?;

                write!(f, ", Log transfer protocol")?;

                if let Some((seq, seq_2, _)) = log_transfer {
                    write!(f, " from {:?} to {:?}", seq, seq_2)
                } else {
                    write!(f, " None")
                }
            }
        }
    }
}