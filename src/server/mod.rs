//! Contains the server side core protocol logic of `Atlas`.

use std::fmt::Debug;
use std::marker::PhantomData;
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{anyhow, Context};
use either::Either;
use log::{debug, error, info, trace};
use thiserror::Error;

use atlas_common::channel::{ChannelSyncRx, ChannelSyncTx};
use atlas_common::error::*;
use atlas_common::globals::ReadOnly;
use atlas_common::maybe_vec::MaybeVec;
use atlas_common::node_id::NodeId;
use atlas_common::ordering::{Orderable, SeqNo};
use atlas_common::{channel, Err};
use atlas_communication::message::StoredMessage;
use atlas_communication::reconfiguration::{
    NetworkInformationProvider, ReconfigurationMessageHandler,
};
use atlas_communication::stub::{ModuleIncomingStub, RegularNetworkStub};
use atlas_core::executor::DecisionExecutorHandle;
use atlas_core::messages::Message;
use atlas_core::ordering_protocol::loggable::LoggableOrderProtocol;
use atlas_core::ordering_protocol::networking::serialize::{NetworkView, OrderingProtocolMessage};
use atlas_core::ordering_protocol::networking::{
    NetworkedOrderProtocolInitializer, ViewTransferProtocolSendNode,
};
use atlas_core::ordering_protocol::permissioned::{
    VTMsg, VTPollResult, VTResult, ViewTransferProtocol, ViewTransferProtocolInitializer,
};
use atlas_core::ordering_protocol::reconfigurable_order_protocol::{
    ReconfigurableOrderProtocol, ReconfigurationAttemptResult,
};
use atlas_core::ordering_protocol::{
    DecisionsAhead, ExecutionResult, OPExecResult, OPPollResult, OrderingProtocol,
    OrderingProtocolArgs, PermissionedOrderingProtocol, ProtocolMessage, ShareableMessage, View,
};
use atlas_core::persistent_log::OperationMode;
use atlas_core::persistent_log::PersistableStateTransferProtocol;
use atlas_core::reconfiguration_protocol::{
    AlterationFailReason, QuorumAlterationResponse, QuorumAttemptJoinResponse,
    QuorumReconfigurationMessage, QuorumReconfigurationResponse, ReconfigurableNodeTypes,
    ReconfigurationProtocol,
};
use atlas_core::request_pre_processing::work_dividers::WDRoundRobin;
use atlas_core::request_pre_processing::{PreProcessorMessage, RequestPreProcessor};
use atlas_core::timeouts::{RqTimeout, TimedOut, TimeoutKind, Timeouts};
use atlas_logging_core::decision_log::{
    DecisionLog, DecisionLogInitializer, LoggedDecision, LoggedDecisionValue,
};
use atlas_logging_core::log_transfer::networking::serialize::LogTransferMessage;
use atlas_logging_core::log_transfer::{LogTransferProtocol, LogTransferProtocolInitializer};
use atlas_metrics::metrics::{metric_duration, metric_increment};
use atlas_persistent_log::NoPersistentLog;
use atlas_smr_application::serialize::ApplicationData;
use atlas_smr_core::exec::WrappedExecHandle;
use atlas_smr_core::message::SystemMessage;
use atlas_smr_core::networking::SMRReplicaNetworkNode;
use atlas_smr_core::request_pre_processing::initialize_request_pre_processor;
use atlas_smr_core::state_transfer::networking::serialize::StateTransferMessage;
use atlas_smr_core::state_transfer::{STResult, StateTransferProtocol};
use atlas_smr_core::SMRReq;

use crate::config::ReplicaConfig;
use crate::metric::{
    ORDERING_PROTOCOL_PROCESS_TIME_ID, REPLICA_INTERNAL_PROCESS_TIME_ID,
    REPLICA_ORDERED_RQS_PROCESSED_ID, REPLICA_PROTOCOL_RESP_PROCESS_TIME_ID,
    REPLICA_TAKE_FROM_NETWORK_ID, TIMEOUT_PROCESS_TIME_ID,
};
use crate::persistent_log::SMRPersistentLog;
use crate::server::decision_log::{
    DLWorkMessage, DecisionLogHandle, DecisionLogManager, DecisionLogWorkMessage,
    LogTransferWorkMessage, ReplicaWorkResponses,
};
use crate::server::state_transfer::{
    StateTransferProgress, StateTransferThreadHandle, StateTransferWorkMessage,
};

pub mod client_replier;
mod decision_log;
pub mod divisible_state_server;
pub mod follower_handling;
pub mod monolithic_server;
pub mod state_transfer;
mod unordered_rq_handler;
// pub mod rq_finalizer;

const REPLICA_MESSAGE_CHANNEL: usize = 1024;
pub const REPLICA_WAIT_TIME: Duration = Duration::from_millis(1000);

/// The current phase of the order protocol phase
#[derive(Clone, Debug)]
pub(crate) enum ExecutionPhase {
    OrderProtocol,
    ViewTransferProtocol,
}

/// The current state of the transfer protocol
#[derive(Clone, Debug)]
pub(crate) enum TransferPhase {
    NotRunning,
    RunningTransferProtocols {
        state_transfer: StateTransferState,
        log_transfer: LogTransferState,
    },
}

/// The current State Transfer state
#[derive(Clone, Debug)]
pub(crate) enum StateTransferState {
    Idle,
    Running,
    Done(SeqNo),
}

/// The current state of the log transfer
#[derive(Clone, Debug)]
pub(crate) enum LogTransferState {
    Idle,
    Running,
    Done(SeqNo, SeqNo),
}

pub type Exec<D: ApplicationData> = WrappedExecHandle<D::Request>;

type ViewType<D, VT, OP, NT, R: PermissionedProtocolHandling<D, VT, OP, NT>> =
    <R as PermissionedProtocolHandling<D, VT, OP, NT>>::View;

pub struct Replica<RP, S, D, OP, DL, ST, LT, VT, NT, PL>
where
    NT: SMRReplicaNetworkNode<
            RP::InformationProvider,
            RP::Serialization,
            D,
            OP::Serialization,
            LT::Serialization,
            VT::Serialization,
            ST::Serialization,
        > + 'static,
    D: ApplicationData + 'static,
    OP: LoggableOrderProtocol<SMRReq<D>>,
    DL: DecisionLog<SMRReq<D>, OP>,
    LT: LogTransferProtocol<SMRReq<D>, OP, DL>,
    VT: ViewTransferProtocol<OP>,
    ST: StateTransferProtocol<S> + PersistableStateTransferProtocol,
    PL: SMRPersistentLog<D, OP::Serialization, OP::PersistableTypes, DL::LogSerialization>
        + 'static,
    RP: ReconfigurationProtocol + 'static,
{
    execution_state: ExecutionPhase,
    transfer_states: TransferPhase,

    quorum_reconfig_data: QuorumReconfig,

    // The ordering protocol, responsible for ordering requests
    ordering_protocol: OP,
    // The view transfer protocol, couple with the ordering protocol
    view_transfer_protocol: VT,
    // The handle to communicate with the decision log thread.
    decision_log_handle: DecisionLogHandle<
        ViewType<D, VT, OP, NT, Self>,
        SMRReq<D>,
        OP::Serialization,
        OP::PersistableTypes,
        LT::Serialization,
    >,
    // The pre processor handle to the decision log
    rq_pre_processor: RequestPreProcessor<SMRReq<D>>,
    timeouts: Timeouts,
    executor_handle: WrappedExecHandle<D::Request>,
    // The networking layer for a Node in the network (either Client or Replica)
    node: Arc<NT>,
    // The handle to the execution and timeouts handler
    execution: (ChannelSyncRx<Message>, ChannelSyncTx<Message>),
    // The handle for processed timeouts
    processed_timeout: (
        ChannelSyncTx<(Vec<RqTimeout>, Vec<RqTimeout>)>,
        ChannelSyncRx<(Vec<RqTimeout>, Vec<RqTimeout>)>,
    ),
    // Receive reconfiguration messages from the reconfiguration protocol
    reconf_receive: ChannelSyncRx<QuorumReconfigurationMessage>,
    // reconfiguration protocol send
    reconf_tx: ChannelSyncTx<QuorumReconfigurationResponse>,
    // handle the state transfer handle
    state_transfer_handle: StateTransferThreadHandle<ViewType<D, VT, OP, NT, Self>>,
    // Persistent log
    persistent_log: PL,
    // The reconfiguration protocol handle
    reconfig_protocol: RP,

    st: PhantomData<fn() -> (S, ST, DL, LT)>,
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

/// The trait with methods specific to reconfigurable protocol handle
/// This is then combined with specialization in order to maintain
/// optional support for this type of protocols
pub trait ReconfigurableProtocolHandling {
    fn attempt_quorum_join(&mut self, node: NodeId) -> Result<()>;

    fn attempt_to_join_quorum(&mut self) -> Result<()>;
}

/// Trait with methods specific to reconfigurable protocol handle
/// This is then combined with specialization in order to provide
/// optional support for this type of protocols
pub(crate) trait PermissionedProtocolHandling<D, VT, OP, NT>
where
    OP: OrderingProtocol<SMRReq<D>>,
    VT: ViewTransferProtocol<OP>,
    D: ApplicationData,
{
    type View: NetworkView + 'static;

    fn view(&self) -> Self::View;

    fn run_view_transfer(&mut self) -> Result<()>;

    fn iterate_view_transfer_protocol(&mut self) -> Result<()>;

    fn handle_view_transfer_msg(
        &mut self,
        msg: StoredMessage<VTMsg<VT::Serialization>>,
    ) -> Result<()>;
}

impl<RP, S, D, OP, DL, ST, LT, VT, NT, PL> Replica<RP, S, D, OP, DL, ST, LT, VT, NT, PL>
where
    RP: ReconfigurationProtocol + 'static,
    D: ApplicationData + 'static,
    OP: LoggableOrderProtocol<SMRReq<D>> + Send,
    DL: DecisionLog<SMRReq<D>, OP>,
    LT: LogTransferProtocol<SMRReq<D>, OP, DL>,
    VT: ViewTransferProtocol<OP>,
    ST: StateTransferProtocol<S> + PersistableStateTransferProtocol + Send,
    NT: SMRReplicaNetworkNode<
        RP::InformationProvider,
        RP::Serialization,
        D,
        OP::Serialization,
        LT::Serialization,
        VT::Serialization,
        ST::Serialization,
    >,
    PL: SMRPersistentLog<D, OP::Serialization, OP::PersistableTypes, DL::LogSerialization>,
{
    async fn bootstrap(
        cfg: ReplicaConfig<RP, S, D, OP, DL, ST, LT, VT, NT, PL>,
        executor: Exec<D>,
        state: StateTransferThreadHandle<
            <Self as PermissionedProtocolHandling<D, VT, OP, NT>>::View,
        >,
    ) -> Result<Self>
    where
        OP: NetworkedOrderProtocolInitializer<SMRReq<D>, NT::ProtocolNode>,
        VT: ViewTransferProtocolInitializer<OP, NT::ProtocolNode>,
        LT: LogTransferProtocolInitializer<SMRReq<D>, OP, DL, PL, Exec<D>, NT::ProtocolNode>,
        DL: DecisionLogInitializer<SMRReq<D>, OP, PL, Exec<D>>,
    {
        let ReplicaConfig {
            next_consensus_seq,
            db_path,
            op_config,
            lt_config,
            dl_config,
            pl_config,
            vt_config,
            node: node_config,
            reconfig_node,
            p,
        } = cfg;

        let network_info = RP::init_default_information(reconfig_node)?;

        let log_node_id = network_info.own_node_info().node_id();

        info!(
            "{:?} // Bootstrapping replica, starting with networking",
            log_node_id
        );

        let reconfiguration_handler = ReconfigurationMessageHandler::initialize();

        let node = NT::bootstrap(
            network_info.clone(),
            node_config,
            reconfiguration_handler.clone(),
        )
        .await?;
        let node = Arc::new(node);

        let (reconf_tx, reconf_rx) = channel::new_bounded_sync(
            REPLICA_MESSAGE_CHANNEL,
            Some("Reconfiguration Channel message"),
        );

        let (reconf_response_tx, reply_rx) = channel::new_bounded_sync(
            REPLICA_MESSAGE_CHANNEL,
            Some("Reconfiguration Channel Response Message"),
        );

        let default_timeout = Duration::from_secs(3);

        let (exec_tx, exec_rx) =
            channel::new_bounded_sync(REPLICA_MESSAGE_CHANNEL, Some("Processed message channel"));

        debug!("{:?} // Initializing timeouts", log_node_id);

        // start timeouts handler
        let timeouts = Timeouts::new::<SMRReq<D>>(
            log_node_id.clone(),
            Duration::from_millis(1),
            default_timeout,
            exec_tx.clone(),
        );

        let replica_node_args = ReconfigurableNodeTypes::QuorumNode(reconf_tx, reply_rx);

        debug!("{:?} // Initializing reconfiguration protocol", log_node_id);
        let reconfig_protocol = RP::initialize_protocol(
            network_info,
            node.reconfiguration_node().clone(),
            timeouts.clone(),
            replica_node_args,
            reconfiguration_handler,
            OP::get_n_for_f(1),
        )
        .await?;

        info!(
            "{:?} // Waiting for reconfiguration protocol to stabilize",
            log_node_id
        );

        let mut quorum = {
            let message = reconf_rx.recv().unwrap();

            match message {
                QuorumReconfigurationMessage::ReconfigurationProtocolStable(quorum) => {
                    reconf_response_tx
                        .send_return(QuorumReconfigurationResponse::QuorumStableResponse(true))
                        .unwrap();

                    quorum
                }
                _ => {
                    return Err!(SMRReplicaError::AlterationReceivedBeforeStable);
                }
            }
        };

        if quorum.len() < OP::get_n_for_f(1) {
            return Err!(SMRReplicaError::QuorumNotLargeEnough(
                quorum.len(),
                OP::get_n_for_f(1)
            ));
        }

        info!(
            "{:?} // Reconfiguration protocol stabilized with {} nodes ({:?}), starting replica",
            log_node_id,
            quorum.len(),
            quorum
        );

        let (ordered, unordered) = initialize_request_pre_processor::<
            WDRoundRobin,
            D,
            NT::ApplicationNode,
        >(4, node.app_node());
        
        unordered_rq_handler::start_unordered_rq_thread::<D>(unordered, executor.clone());

        let persistent_log =
            PL::init_log::<String, NoPersistentLog, OP, ST, DL>(executor.clone(), db_path)?;
        
        let (rq_pre_processor, batch_input) = ordered.into();

        let op_args = OrderingProtocolArgs(
            log_node_id,
            timeouts.clone(),
            rq_pre_processor.clone(),
            batch_input,
            node.protocol_node().clone(),
            quorum.clone(),
        );

        let log = persistent_log.read_decision_log(OperationMode::BlockingSync)?;
        
        let decision_handle = DecisionLogManager::<
            ViewType<D, VT, OP, NT, Self>,
            D::Request,
            OP,
            DL,
            LT,
            NT::ProtocolNode,
            PL,
        >::initialize_decision_log_mngt(
            dl_config,
            lt_config,
            persistent_log.clone(),
            timeouts.clone(),
            node.protocol_node().clone(),
            rq_pre_processor.clone(),
            state.clone(),
            executor.clone(),
        )?;

        let ordering_protocol = OP::initialize(op_config, op_args)?;

        let view_transfer_protocol = VT::initialize_view_transfer_protocol(
            vt_config,
            node.protocol_node().clone(),
            quorum.clone(),
        )?;

        info!("{:?} // Finished bootstrapping node.", log_node_id);

        let timeout_channel = channel::new_bounded_sync(1024, Some("SMR Timeout work channel"));

        let mut replica = Self {
            execution_state: ExecutionPhase::ViewTransferProtocol,
            transfer_states: TransferPhase::NotRunning,
            quorum_reconfig_data: QuorumReconfig {
                node_pending_join: None,
            },
            ordering_protocol,
            view_transfer_protocol,
            decision_log_handle: decision_handle,
            rq_pre_processor,
            timeouts,
            executor_handle: executor,
            node,
            execution: (exec_rx, exec_tx),
            processed_timeout: timeout_channel,
            reconf_receive: reconf_rx,
            reconf_tx: reconf_response_tx,
            state_transfer_handle: state,
            persistent_log,
            reconfig_protocol,
            st: Default::default(),
        };

        Ok(replica)
    }

    fn id(&self) -> NodeId {
        self.node.id()
    }

    fn bootstrap_protocols(&mut self) -> Result<()> {
        info!("{:?} // Bootstrapping SMR Replica protocols", self.id());

        self.run_view_transfer()?;
        self.run_transfer_protocols()?;

        Ok(())
    }

    pub fn run(&mut self) -> Result<()> {
        let now = Instant::now();

        self.receive_internal()?;

        self.poll_other_protocols()?;

        metric_duration(REPLICA_INTERNAL_PROCESS_TIME_ID, now.elapsed());

        match self.execution_state {
            ExecutionPhase::OrderProtocol => {
                self.run_order_protocol()?;
            }
            ExecutionPhase::ViewTransferProtocol => {
                self.iterate_view_transfer_protocol()?;
            }
        }

        Ok(())
    }

    fn run_order_protocol(&mut self) -> Result<()> {
        let poll_result = self.ordering_protocol.poll()?;

        match poll_result {
            OPPollResult::RePoll => {}
            OPPollResult::RunCst => {
                self.run_transfer_protocols()?;
            }
            OPPollResult::ReceiveMsg => {
                let start = Instant::now();

                let network_message = self
                    .node
                    .protocol_node()
                    .incoming_stub()
                    .try_receive_messages(Some(REPLICA_WAIT_TIME))?;

                metric_duration(REPLICA_TAKE_FROM_NETWORK_ID, start.elapsed());

                if let Some(network_message) = network_message {
                    let (header, message) = network_message.into_inner();

                    match message {
                        SystemMessage::ProtocolMessage(protocol) => {
                            let message = Arc::new(ReadOnly::new(StoredMessage::new(
                                header,
                                protocol.into_inner(),
                            )));

                            self.execute_order_protocol_message(message)?;
                        }
                        SystemMessage::ViewTransferMessage(view_transfer) => {
                            let strd_msg = StoredMessage::new(header, view_transfer.into_inner());

                            self.handle_view_transfer_msg(strd_msg)?;
                        }
                        SystemMessage::ForwardedRequestMessage(fwd_reqs) => {
                            // Send the forwarded requests to be handled, filtered and then passed onto the ordering protocol
                            self.rq_pre_processor
                                .send_return(PreProcessorMessage::ForwardedRequests(
                                    StoredMessage::new(header, fwd_reqs),
                                ))
                                .unwrap();
                        }
                        SystemMessage::ForwardedProtocolMessage(fwd_protocol) => {
                            let message = fwd_protocol.into_inner();

                            let (header, message) = message.into_inner();

                            let message = Arc::new(ReadOnly::new(StoredMessage::new(
                                header,
                                message.into_inner(),
                            )));

                            self.execute_order_protocol_message(message)?;
                        }
                        SystemMessage::LogTransferMessage(log_transfer) => {
                            let strd_msg = StoredMessage::new(header, log_transfer.into_inner());

                            let view = self.view();

                            self.decision_log_handle.send_work(
                                DLWorkMessage::init_log_transfer_message(
                                    view,
                                    LogTransferWorkMessage::LogTransferMessage(strd_msg),
                                ),
                            );
                        }
                        _ => {
                            error!(
                                "{:?} // Received unsupported message {:?}",
                                self.node.id(),
                                message
                            );
                        }
                    }
                } else {
                    // Receive timeouts in the beginning of the next iteration
                    return Ok(());
                }
            }
            OPPollResult::Exec(message) => {
                self.execute_order_protocol_message(message)?;
            }
            OPPollResult::ProgressedDecision(clear_ahead, decision) => {
                match clear_ahead {
                    DecisionsAhead::Ignore => {}
                    DecisionsAhead::ClearAhead => {
                        self.clear_currently_executing()?;
                    }
                }

                let dec_log_msg = DLWorkMessage::init_dec_log_message(
                    self.view(),
                    DecisionLogWorkMessage::DecisionInformation(decision),
                );

                self.decision_log_handle.send_work(dec_log_msg);
            }
            OPPollResult::QuorumJoined(clear_ahead, decision, join_info) => {
                match clear_ahead {
                    DecisionsAhead::Ignore => {}
                    DecisionsAhead::ClearAhead => {
                        self.clear_currently_executing()?;
                    }
                }

                let (node_id, quorum) = join_info.into_inner();

                self.handle_quorum_joined(node_id, quorum)?;

                if let Some(decision) = decision {
                    let dec_log_msg = DLWorkMessage::init_dec_log_message(
                        self.view(),
                        DecisionLogWorkMessage::DecisionInformation(decision),
                    );

                    self.decision_log_handle.send_work(dec_log_msg);
                }
            }
        }

        Ok(())
    }

    fn poll_other_protocols(&mut self) -> Result<()> {
        while let Some(state_result) = self.state_transfer_handle.try_recv_state_transfer_update() {
            match state_result {
                StateTransferProgress::StateTransferProgress(progress) => {
                    self.handle_state_transfer_result(progress)?;
                }
                StateTransferProgress::CheckpointReceived(checkpoint) => {
                    self.decision_log_handle
                        .send_work(DLWorkMessage::init_dec_log_message(
                            self.view(),
                            DecisionLogWorkMessage::CheckpointDone(checkpoint),
                        ));
                }
            }
        }

        while let Some(dec_log_res) = self.decision_log_handle.try_to_recv_resp() {
            match dec_log_res {
                ReplicaWorkResponses::InstallSeqNo(seq_no) => {
                    info!(
                        "Installing sequence number {:?} into order protocol",
                        seq_no
                    );
                    self.ordering_protocol.install_seq_no(seq_no)?;
                    debug!("Done installing");
                }
                ReplicaWorkResponses::LogTransferFinalized(first_seq, last_seq) => {
                    info!(
                        "Log transfer finalized with sequence range {:?} to {:?}",
                        first_seq, last_seq
                    );

                    self.handle_log_transfer_done(first_seq, last_seq)?;
                }
                ReplicaWorkResponses::LogTransferNotNeeded(first_seq, last_seq) => {
                    info!(
                        "Log transfer not needed with sequence range {:?} to {:?}",
                        first_seq, last_seq
                    );

                    self.handle_log_transfer_done(first_seq, last_seq)?;
                }
            }
        }

        Ok(())
    }

    fn handle_state_transfer_result(&mut self, result: STResult) -> Result<()> {
        match result {
            STResult::StateTransferRunning => {}
            STResult::StateTransferReady => {
                self.executor_handle.poll_state_channel()?;
            }
            STResult::StateTransferFinished(seq_no) => {
                info!("{:?} // State transfer finished. Registering result and comparing with log transfer result", self.node.id());

                self.executor_handle.poll_state_channel()?;

                self.handle_state_transfer_done(seq_no)?;
            }
            STResult::StateTransferNotNeeded(curr_seq) => {
                info!("{:?} // State transfer not needed. Registering result and comparing with log transfer result", self.node.id());

                self.handle_state_transfer_done(curr_seq)?;
            }
            STResult::RunStateTransfer => {
                self.run_state_transfer_protocol()?;
            }
        }

        Ok(())
    }

    /// Handles receiving the log transfer finalized
    fn handle_log_transfer_done(&mut self, initial_seq: SeqNo, last_seq: SeqNo) -> Result<()> {
        info!(
            "Handling log transfer result {:?} to {:?} with current phase {:?}",
            initial_seq, last_seq, self.transfer_states
        );

        let prev_state = std::mem::replace(&mut self.transfer_states, TransferPhase::NotRunning);

        self.transfer_states = match prev_state {
            TransferPhase::NotRunning => {
                return Err(anyhow!("How can we have finished the log transfer result when we are not running transfer protocols"));
            }
            TransferPhase::RunningTransferProtocols {
                log_transfer,
                state_transfer,
            } => {
                match log_transfer {
                    LogTransferState::Idle => {
                        unreachable!("Received result of the log transfer while not running it?")
                    }
                    _ => {}
                }

                TransferPhase::RunningTransferProtocols {
                    state_transfer,
                    log_transfer: LogTransferState::Done(initial_seq, last_seq),
                }
            }
        };

        // We know that the log transfer is already done, so if the state transfer is done then they are both done
        if Self::is_state_transfer_done(&self.transfer_states) {
            self.finish_transfer()?;
        }

        Ok(())
    }

    /// Handles receiving the state transfer state
    fn handle_state_transfer_done(&mut self, seq: SeqNo) -> Result<()> {
        info!(
            "Handling state transfer result {:?} with current phase {:?}",
            seq, self.transfer_states
        );

        let prev_state = std::mem::replace(&mut self.transfer_states, TransferPhase::NotRunning);

        self.transfer_states = match prev_state {
            TransferPhase::NotRunning => {
                return Err(anyhow!("How can we have finished the state transfer protocol when we are not running transfer protocols"));
            }
            TransferPhase::RunningTransferProtocols {
                log_transfer,
                state_transfer,
            } => {
                match log_transfer {
                    LogTransferState::Idle => {
                        unreachable!("Received result of the log transfer while not running it?")
                    }
                    _ => {}
                }

                TransferPhase::RunningTransferProtocols {
                    log_transfer,
                    state_transfer: StateTransferState::Done(seq),
                }
            }
        };

        // We know that the state transfer is already done, so if the log transfer is also done, then they are both done
        if Self::is_log_transfer_done(&self.transfer_states) {
            self.finish_transfer()?;
        }

        Ok(())
    }

    fn execute_order_protocol_message(
        &mut self,
        message: ShareableMessage<ProtocolMessage<SMRReq<D>, OP::Serialization>>,
    ) -> Result<()> {
        let start = Instant::now();

        let exec_result = self.ordering_protocol.process_message(message)?;

        metric_duration(ORDERING_PROTOCOL_PROCESS_TIME_ID, start.elapsed());

        let start = Instant::now();

        match exec_result {
            OPExecResult::MessageDropped
            | OPExecResult::MessageQueued
            | OPExecResult::MessageProcessedNoUpdate => {}
            OPExecResult::ProgressedDecision(clear_decisions, decision) => {
                match clear_decisions {
                    DecisionsAhead::Ignore => {}
                    DecisionsAhead::ClearAhead => {
                        self.clear_currently_executing()?;
                    }
                }

                let dec_log_work = DLWorkMessage::init_dec_log_message(
                    self.view(),
                    DecisionLogWorkMessage::DecisionInformation(decision),
                );

                self.decision_log_handle.send_work(dec_log_work);
            }
            OPExecResult::QuorumJoined(clear_decisions, decision, quorum) => {
                match clear_decisions {
                    DecisionsAhead::Ignore => {}
                    DecisionsAhead::ClearAhead => {
                        self.clear_currently_executing()?;
                    }
                }

                let (node_id, quorum) = quorum.into_inner();

                self.handle_quorum_joined(node_id, quorum)?;

                if let Some(decision) = decision {
                    let dec_log_work = DLWorkMessage::init_dec_log_message(
                        self.view(),
                        DecisionLogWorkMessage::DecisionInformation(decision),
                    );

                    self.decision_log_handle.send_work(dec_log_work);
                }
            }
            OPExecResult::RunCst => {
                self.run_transfer_protocols()?;
            }
        }

        metric_duration(REPLICA_PROTOCOL_RESP_PROCESS_TIME_ID, start.elapsed());
        metric_increment(REPLICA_ORDERED_RQS_PROCESSED_ID, Some(1));

        Ok(())
    }

    fn clear_currently_executing(&mut self) -> Result<()> {
        let work_message = DLWorkMessage::init_dec_log_message(
            self.view(),
            DecisionLogWorkMessage::ClearUnfinishedDecisions,
        );

        self.decision_log_handle.send_work(work_message);

        Ok(())
    }

    fn execute_logged_decisions(
        &mut self,
        decisions: MaybeVec<LoggedDecision<SMRReq<D>>>,
    ) -> Result<()> {
        for decision in decisions.into_iter() {
            let (seq, requests, to_batch) = decision.into_inner();

            if let Err(err) = self
                .rq_pre_processor
                .send_return(PreProcessorMessage::DecidedBatch(requests))
            {
                error!("Error sending decided batch to pre processor: {:?}", err);
            }

            let last_seq_no_u32 = u32::from(seq);

            let checkpoint = if last_seq_no_u32 > 0 && last_seq_no_u32 % CHECKPOINT_PERIOD == 0 {
                //We check that % == 0 so we don't start multiple checkpoints

                let (e_tx, e_rx) = channel::new_oneshot_channel();

                self.state_transfer_handle
                    .send_work_message(StateTransferWorkMessage::ShouldRequestAppState(seq, e_tx));

                if let Ok(res) = e_rx.recv() {
                    res
                } else {
                    ExecutionResult::Nil
                }
            } else {
                ExecutionResult::Nil
            };

            match to_batch {
                LoggedDecisionValue::Execute(requests) => match checkpoint {
                    ExecutionResult::Nil => self.executor_handle.queue_update(requests)?,
                    ExecutionResult::BeginCheckpoint => {
                        self.executor_handle.queue_update_and_get_appstate(
                            WrappedExecHandle::transform_update_batch(requests),
                        )?
                    }
                },
                LoggedDecisionValue::ExecutionNotNeeded => {
                    // When the execution is handled
                }
            }
        }

        Ok(())
    }

    fn receive_internal(&mut self) -> Result<()> {
        // FIXME: Do this with a select?
        while let Ok(recvd) = self.execution.0.try_recv() {
            match recvd {
                Message::Timeout(timeout) => {
                    self.timeout_received(timeout)?;
                    //info!("{:?} // Received and ignored timeout with {} timeouts {:?}", self.node.id(), timeout.len(), timeout);
                }
                _ => {}
            }
        }

        while let Ok(received) = self.reconf_receive.try_recv() {
            match received {
                QuorumReconfigurationMessage::RequestQuorumJoin(node) => {
                    info!(
                        "Received request for quorum view alteration for {:?}, current phase: {:?}",
                        node, self.execution_state
                    );

                    self.attempt_quorum_join(node)?;
                }
                QuorumReconfigurationMessage::AttemptToJoinQuorum => {
                    info!(
                        "Received request to attempt to join quorum, current phase: {:?}",
                        self.execution_state
                    );

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
            self.processed_timeout_recvd(timeouts, deleted)?;
        }

        Ok(())
    }

    fn timeout_received(&mut self, timeouts: TimedOut) -> Result<()> {
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
            debug!(
                "{:?} // Received client request timeouts: {}",
                self.node.id(),
                client_rq.len()
            );

            self.rq_pre_processor
                .process_timeouts(client_rq, self.processed_timeout.0.clone());
        }

        if !cst_rq.is_empty() {
            debug!(
                "{:?} // Received cst timeouts: {}",
                self.node.id(),
                cst_rq.len()
            );

            self.state_transfer_handle
                .send_work_message(StateTransferWorkMessage::Timeout(self.view(), cst_rq));
        }

        if !log_transfer.is_empty() {
            debug!(
                "{:?} // Received log transfer timeouts: {}",
                self.node.id(),
                log_transfer.len()
            );

            let lt_work_message = DLWorkMessage::init_log_transfer_message(
                self.view(),
                LogTransferWorkMessage::ReceivedTimeout(log_transfer),
            );

            self.decision_log_handle.send_work(lt_work_message);
        }

        if !reconfiguration.is_empty() {
            debug!(
                "{:?} // Received reconfiguration timeouts: {}",
                self.node.id(),
                reconfiguration.len()
            );

            self.reconfig_protocol.handle_timeout(reconfiguration)?;
        }

        metric_duration(TIMEOUT_PROCESS_TIME_ID, start.elapsed());

        Ok(())
    }

    // Process a processed timeout request
    fn processed_timeout_recvd(
        &mut self,
        timed_out: Vec<RqTimeout>,
        to_delete: Vec<RqTimeout>,
    ) -> Result<()> {
        self.timeouts.cancel_client_rq_timeouts(Some(
            to_delete
                .into_iter()
                .map(|t| match t.into_timeout_kind() {
                    TimeoutKind::ClientRequestTimeout(info) => info,
                    _ => unreachable!(),
                })
                .collect(),
        ));

        match self.ordering_protocol.handle_timeout(timed_out)? {
            OPExecResult::RunCst => {
                self.run_transfer_protocols()?;
            }
            _ => {}
        };

        Ok(())
    }

    /// Run the ordering protocol on this replica
    fn run_ordering_protocol(&mut self) -> Result<()> {
        info!("{:?} // Running ordering protocol.", self.node.id());

        self.execution_state = ExecutionPhase::OrderProtocol;

        self.ordering_protocol.handle_execution_changed(true)?;

        if let Some(node) = self.quorum_reconfig_data.pop_pending_node_join() {
            self.attempt_quorum_join(node)?;
        }

        Ok(())
    }

    /// Is the log transfer protocol finished?
    fn is_log_transfer_done(state: &TransferPhase) -> bool {
        match state {
            TransferPhase::NotRunning => false,
            TransferPhase::RunningTransferProtocols { log_transfer, .. } => match log_transfer {
                LogTransferState::Done(_, _) => true,
                _ => false,
            },
        }
    }

    /// Is the state transfer protocol finished?
    fn is_state_transfer_done(state: &TransferPhase) -> bool {
        match state {
            TransferPhase::NotRunning => false,
            TransferPhase::RunningTransferProtocols { state_transfer, .. } => {
                match state_transfer {
                    StateTransferState::Done(_) => true,
                    _ => false,
                }
            }
        }
    }

    // Finish the transfer protocols
    fn finish_transfer(&mut self) -> Result<()> {
        let done = match &self.transfer_states {
            TransferPhase::NotRunning => unreachable!(),
            TransferPhase::RunningTransferProtocols {
                log_transfer,
                state_transfer,
            } => {
                match (log_transfer, state_transfer) {
                    (
                        LogTransferState::Done(initial_seq, final_seq),
                        StateTransferState::Done(state_transfer_seq),
                    ) => {
                        if (state_transfer_seq.next() < *initial_seq
                            || state_transfer_seq.next() > *final_seq)
                            && (*state_transfer_seq != SeqNo::ZERO && *initial_seq != SeqNo::ZERO)
                        {
                            error!("{:?} // Log transfer protocol and state transfer protocol are not in sync. Received {:?} state and {:?} - {:?} log", self.id(), *state_transfer_seq, * initial_seq, * final_seq);

                            self.run_transfer_protocols()?;

                            false
                        } else {
                            let mut to_execute_seq = *initial_seq;

                            if *state_transfer_seq > to_execute_seq {
                                to_execute_seq = state_transfer_seq.next();
                            }

                            info!("{:?} // State transfer protocol and log transfer protocol are in sync. Received {:?} state and {:?} - {:?} log", self.id(), *state_transfer_seq, * initial_seq, * final_seq);

                            // We now have to report to the decision log that he can send the executions to the executor
                            let decision_log_work = DLWorkMessage::init_log_transfer_message(
                                self.view(),
                                LogTransferWorkMessage::TransferDone(to_execute_seq, *final_seq),
                            );

                            self.decision_log_handle.send_work(decision_log_work);

                            true
                        }
                    }
                    _ => unreachable!(),
                }
            }
        };

        if done {
            self.transfer_states = TransferPhase::NotRunning;
        }

        Ok(())
    }

    /// Run both the transfer protocols
    fn run_transfer_protocols(&mut self) -> Result<()> {
        self.transfer_states = TransferPhase::RunningTransferProtocols {
            state_transfer: StateTransferState::Running,
            log_transfer: LogTransferState::Running,
        };

        info!(
            "{:?} // Running state and log transfer protocols. {:?}",
            self.node.id(),
            self.transfer_states
        );

        self.state_transfer_handle
            .send_work_message(StateTransferWorkMessage::RequestLatestState(self.view()));
        self.decision_log_handle
            .send_work(DLWorkMessage::init_log_transfer_message(
                self.view(),
                LogTransferWorkMessage::RequestLogTransfer,
            ));

        Ok(())
    }

    fn run_state_transfer_protocol(&mut self) -> Result<()> {
        let state = std::mem::replace(&mut self.transfer_states, TransferPhase::NotRunning);

        self.transfer_states = match state {
            TransferPhase::NotRunning => TransferPhase::RunningTransferProtocols {
                state_transfer: StateTransferState::Running,
                log_transfer: LogTransferState::Idle,
            },
            TransferPhase::RunningTransferProtocols { log_transfer, .. } => {
                TransferPhase::RunningTransferProtocols {
                    state_transfer: StateTransferState::Running,
                    log_transfer,
                }
            }
        };

        info!(
            "Running state transfer protocol. {:?}",
            self.transfer_states
        );

        self.state_transfer_handle
            .send_work_message(StateTransferWorkMessage::RequestLatestState(self.view()));

        Ok(())
    }

    fn run_log_transfer_protocol(&mut self) -> Result<()> {
        let state = std::mem::replace(&mut self.transfer_states, TransferPhase::NotRunning);

        self.transfer_states = match state {
            TransferPhase::NotRunning => TransferPhase::RunningTransferProtocols {
                state_transfer: StateTransferState::Idle,
                log_transfer: LogTransferState::Running,
            },
            TransferPhase::RunningTransferProtocols { state_transfer, .. } => {
                TransferPhase::RunningTransferProtocols {
                    state_transfer,
                    log_transfer: LogTransferState::Running,
                }
            }
        };

        info!("Running log transfer protocol. {:?}", self.transfer_states);

        self.decision_log_handle
            .send_work(DLWorkMessage::init_log_transfer_message(
                self.view(),
                LogTransferWorkMessage::RequestLogTransfer,
            ));

        Ok(())
    }

    /// We are pending a node join
    fn append_pending_join_node(&mut self, node: NodeId) -> bool {
        self.quorum_reconfig_data.append_pending_node_join(node)
    }

    /// Handles the quorum joined
    fn handle_quorum_joined(&mut self, node: NodeId, members: Vec<NodeId>) -> Result<()> {
        if node == self.id() {
            info!(
                "{:?} // We have joined the quorum, responding to the reconfiguration protocol",
                self.id()
            );

            self.reply_to_attempt_quorum_join(false)?;

            return Ok(());
        }

        self.reply_to_quorum_entrance_request(node, Either::Left(members))?;

        Ok(())
    }

    fn reply_to_attempt_quorum_join(&mut self, failed: bool) -> Result<()> {
        info!("{:?} // Sending attempt quorum join response to reconfiguration protocol. Has failed: {:?}", self.id(), failed);

        if failed {
            self.reconf_tx
                .send_return(QuorumReconfigurationResponse::QuorumAttemptJoinResponse(
                    QuorumAttemptJoinResponse::Failed,
                ))
                .context("Error sending quorum entrance response to reconfiguration protocol")?;
        } else {
            self.reconf_tx
                .send_return(QuorumReconfigurationResponse::QuorumAttemptJoinResponse(
                    QuorumAttemptJoinResponse::Success,
                ))
                .context("Error sending quorum entrance response to reconfiguration protocol")?;
        }

        Ok(())
    }

    /// Send the result of attempting to join the quorum to the reconfiguration protocol, so that
    /// it can proceed with execution
    fn reply_to_quorum_entrance_request(
        &mut self,
        node_id: NodeId,
        failed_reason: Either<Vec<NodeId>, AlterationFailReason>,
    ) -> Result<()> {
        info!("{:?} // Sending quorum entrance response to reconfiguration protocol with success: {:?}", self.id(), failed_reason);

        match failed_reason {
            Either::Left(quorum) => {
                self.reconf_tx
                    .send_return(QuorumReconfigurationResponse::QuorumAlterationResponse(
                        QuorumAlterationResponse::Successful(
                            node_id,
                            OP::get_f_for_n(quorum.len()),
                        ),
                    ))
                    .context(
                        "Error sending quorum entrance response to reconfiguration protocol",
                    )?;
            }
            Either::Right(fail_reason) => {
                self.reconf_tx
                    .send_return(QuorumReconfigurationResponse::QuorumAlterationResponse(
                        QuorumAlterationResponse::Failed(node_id, fail_reason),
                    ))
                    .context(
                        "Error sending quorum entrance response to reconfiguration protocol",
                    )?;
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

impl<RP, S, D, OP, DL, ST, LT, VT, NT, PL> PermissionedProtocolHandling<D, VT, OP, NT>
    for Replica<RP, S, D, OP, DL, ST, LT, VT, NT, PL>
where
    D: ApplicationData + 'static,
    OP: LoggableOrderProtocol<SMRReq<D>>,
    DL: DecisionLog<SMRReq<D>, OP>,
    LT: LogTransferProtocol<SMRReq<D>, OP, DL>,
    VT: ViewTransferProtocol<OP>,
    ST: StateTransferProtocol<S> + PersistableStateTransferProtocol,
    PL: SMRPersistentLog<D, OP::Serialization, OP::PersistableTypes, DL::LogSerialization>
        + 'static,
    RP: ReconfigurationProtocol + 'static,
    NT: SMRReplicaNetworkNode<
            RP::InformationProvider,
            RP::Serialization,
            D,
            OP::Serialization,
            LT::Serialization,
            VT::Serialization,
            ST::Serialization,
        > + 'static,
{
    default type View = MockView;

    default fn view(&self) -> Self::View {
        todo!()
    }

    default fn run_view_transfer(&mut self) -> Result<()> {
        info!(
            "Running the default view transfer protocol, where there actually is no view transfer"
        );

        Ok(())
    }

    default fn iterate_view_transfer_protocol(&mut self) -> Result<()> {
        Ok(())
    }

    default fn handle_view_transfer_msg(
        &mut self,
        message: StoredMessage<VTMsg<VT::Serialization>>,
    ) -> Result<()> {
        Ok(())
    }
}

impl<RP, S, D, OP, DL, ST, LT, VT, NT, PL> PermissionedProtocolHandling<D, VT, OP, NT>
    for Replica<RP, S, D, OP, DL, ST, LT, VT, NT, PL>
where
    D: ApplicationData + 'static,
    OP: LoggableOrderProtocol<SMRReq<D>> + PermissionedOrderingProtocol + 'static,
    DL: DecisionLog<SMRReq<D>, OP> + 'static,
    LT: LogTransferProtocol<SMRReq<D>, OP, DL> + 'static,
    VT: ViewTransferProtocol<OP> + 'static,
    ST: StateTransferProtocol<S> + PersistableStateTransferProtocol + Send + 'static,
    PL: SMRPersistentLog<D, OP::Serialization, OP::PersistableTypes, DL::LogSerialization>
        + 'static,
    RP: ReconfigurationProtocol + 'static,
    NT: SMRReplicaNetworkNode<
            RP::InformationProvider,
            RP::Serialization,
            D,
            OP::Serialization,
            LT::Serialization,
            VT::Serialization,
            ST::Serialization,
        > + 'static,
{
    type View = View<OP::PermissionedSerialization>;

    fn view(&self) -> Self::View {
        self.ordering_protocol.view()
    }

    fn run_view_transfer(&mut self) -> Result<()> {
        info!("Running view transfer protocol on the replica");

        self.execution_state = ExecutionPhase::ViewTransferProtocol;

        self.view_transfer_protocol
            .request_latest_view(&self.ordering_protocol)?;

        Ok(())
    }

    fn iterate_view_transfer_protocol(&mut self) -> Result<()> {
        trace!("Iterating view transfer protocol.");

        match self.view_transfer_protocol.poll()? {
            VTPollResult::RePoll => {}
            VTPollResult::Exec(msg) => {
                self.handle_view_transfer_msg(msg)?;
            }
            VTPollResult::ReceiveMsg => {
                let rcvd_msg = self
                    .node
                    .protocol_node()
                    .incoming_stub()
                    .try_receive_messages(None)?;

                if let Some(msg) = rcvd_msg {
                    let (header, message) = msg.into_inner();

                    match message {
                        SystemMessage::ProtocolMessage(protocol) => {
                            let message = Arc::new(ReadOnly::new(StoredMessage::new(
                                header,
                                protocol.into_inner(),
                            )));

                            self.ordering_protocol.handle_off_ctx_message(message);
                        }
                        SystemMessage::ViewTransferMessage(view_transfer) => {
                            let strd_msg = StoredMessage::new(header, view_transfer.into_inner());

                            self.handle_view_transfer_msg(strd_msg)?;
                        }
                        SystemMessage::ForwardedRequestMessage(fwd_reqs) => {
                            // Send the forwarded requests to be handled, filtered and then passed onto the ordering protocol
                            self.rq_pre_processor
                                .send_return(PreProcessorMessage::ForwardedRequests(
                                    StoredMessage::new(header, fwd_reqs),
                                ))
                                .unwrap();
                        }
                        SystemMessage::ForwardedProtocolMessage(fwd_protocol) => {
                            let message = fwd_protocol.into_inner();

                            let (header, message) = message.into_inner();

                            let message = Arc::new(ReadOnly::new(StoredMessage::new(
                                header,
                                message.into_inner(),
                            )));

                            self.ordering_protocol.handle_off_ctx_message(message);
                        }
                        SystemMessage::LogTransferMessage(log_transfer) => {
                            let view = self.view();

                            let strd_msg = StoredMessage::new(header, log_transfer.into_inner());

                            let work_msg = DLWorkMessage::init_log_transfer_message(
                                view,
                                LogTransferWorkMessage::LogTransferMessage(strd_msg),
                            );

                            self.decision_log_handle.send_work(work_msg);
                        }
                        _ => {
                            error!(
                                "{:?} // Received unsupported message {:?}",
                                self.node.id(),
                                message
                            );
                        }
                    }
                }
            }
            VTPollResult::VTResult(res) => match res {
                VTResult::RunVTP => {}
                VTResult::VTransferNotNeeded => {}
                VTResult::VTransferRunning => {}
                VTResult::VTransferFinished => {
                    self.run_ordering_protocol()?;
                }
            },
        }

        Ok(())
    }

    fn handle_view_transfer_msg(
        &mut self,
        message: StoredMessage<VTMsg<VT::Serialization>>,
    ) -> Result<()> {
        match self.execution_state {
            ExecutionPhase::OrderProtocol => {
                self.view_transfer_protocol
                    .handle_off_context_msg(&self.ordering_protocol, message)?;
            }
            ExecutionPhase::ViewTransferProtocol => {
                let vt_result = self
                    .view_transfer_protocol
                    .process_message(&mut self.ordering_protocol, message)?;

                match vt_result {
                    VTResult::RunVTP => {}
                    VTResult::VTransferNotNeeded => {}
                    VTResult::VTransferRunning => {}
                    VTResult::VTransferFinished => {
                        self.run_ordering_protocol()?;
                    }
                }
            }
        }

        Ok(())
    }
}

/// Default protocol with no reconfiguration support handling
impl<RP, S, D, OP, DL, ST, LT, VT, NT, PL> ReconfigurableProtocolHandling
    for Replica<RP, S, D, OP, DL, ST, LT, VT, NT, PL>
where
    RP: ReconfigurationProtocol + 'static,
    D: ApplicationData + 'static,
    OP: LoggableOrderProtocol<SMRReq<D>> + Send,
    DL: DecisionLog<SMRReq<D>, OP>,
    LT: LogTransferProtocol<SMRReq<D>, OP, DL>,
    VT: ViewTransferProtocol<OP>,
    ST: StateTransferProtocol<S> + PersistableStateTransferProtocol + Send,
    NT: SMRReplicaNetworkNode<
            RP::InformationProvider,
            RP::Serialization,
            D,
            OP::Serialization,
            LT::Serialization,
            VT::Serialization,
            ST::Serialization,
        > + 'static,
    PL: SMRPersistentLog<D, OP::Serialization, OP::PersistableTypes, DL::LogSerialization>
        + 'static,
{
    default fn attempt_quorum_join(&mut self, node: NodeId) -> Result<()> {
        self.reply_to_quorum_entrance_request(node, Either::Right(AlterationFailReason::Failed))
    }

    default fn attempt_to_join_quorum(&mut self) -> Result<()> {
        self.reply_to_attempt_quorum_join(true)
    }
}

/// Implement reconfigurable order protocol support
impl<RP, S, D, OP, DL, ST, LT, VT, NT, PL> ReconfigurableProtocolHandling
    for Replica<RP, S, D, OP, DL, ST, LT, VT, NT, PL>
where
    RP: ReconfigurationProtocol + 'static,
    D: ApplicationData + 'static,
    OP: LoggableOrderProtocol<SMRReq<D>>
        + ReconfigurableOrderProtocol<RP::Serialization>
        + Send
        + 'static,
    DL: DecisionLog<SMRReq<D>, OP> + 'static,
    LT: LogTransferProtocol<SMRReq<D>, OP, DL> + 'static,
    VT: ViewTransferProtocol<OP> + 'static,
    ST: StateTransferProtocol<S> + PersistableStateTransferProtocol + Send + 'static,
    NT: SMRReplicaNetworkNode<
            RP::InformationProvider,
            RP::Serialization,
            D,
            OP::Serialization,
            LT::Serialization,
            VT::Serialization,
            ST::Serialization,
        > + 'static,
    PL: SMRPersistentLog<D, OP::Serialization, OP::PersistableTypes, DL::LogSerialization>
        + 'static,
{
    fn attempt_quorum_join(&mut self, node: NodeId) -> Result<()> {
        match self.execution_state {
            ExecutionPhase::OrderProtocol => {
                let quorum_node_join = self.ordering_protocol.attempt_quorum_node_join(node)?;

                debug!(
                    "{:?} // Attempting to join quorum with result {:?}",
                    self.id(),
                    quorum_node_join
                );

                match quorum_node_join {
                    ReconfigurationAttemptResult::Failed => {
                        self.reply_to_quorum_entrance_request(
                            node,
                            Either::Right(AlterationFailReason::Failed),
                        )?;
                    }
                    ReconfigurationAttemptResult::AlreadyPartOfQuorum => {
                        self.reply_to_quorum_entrance_request(
                            node,
                            Either::Right(AlterationFailReason::AlreadyPartOfQuorum),
                        )?;
                    }
                    ReconfigurationAttemptResult::CurrentlyReconfiguring(_) => {
                        self.reply_to_quorum_entrance_request(
                            node,
                            Either::Right(AlterationFailReason::OngoingReconfiguration),
                        )?;
                    }
                    ReconfigurationAttemptResult::InProgress => {}
                    ReconfigurationAttemptResult::Successful(members) => {
                        self.reply_to_quorum_entrance_request(node, Either::Left(members))?;
                    }
                };
            }
            ExecutionPhase::ViewTransferProtocol => {
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
            ReconfigurationAttemptResult::Successful(_)
            | ReconfigurationAttemptResult::AlreadyPartOfQuorum => {
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

#[derive(Error, Debug)]
pub enum SMRReplicaError {
    #[error("The provided quorum is not of sufficient size. Expected at least {0} but got {1}")]
    QuorumNotLargeEnough(usize, usize),
    #[error("The reconfiguration protocol is not stable and we received an alteration request")]
    AlterationReceivedBeforeStable,
}
