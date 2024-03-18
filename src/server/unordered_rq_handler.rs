use atlas_common::ordering::Orderable;
use atlas_common::quiet_unwrap;
use log::error;
use atlas_core::executor::DecisionExecutorHandle;
use atlas_core::messages::SessionBased;
use atlas_metrics::metrics::metric_increment;
use atlas_smr_application::app::UnorderedBatch;
use atlas_smr_application::ExecutorHandle;
use atlas_smr_application::serialize::ApplicationData;
use atlas_smr_core::request_pre_processing::UnorderedRqHandles;
use atlas_smr_core::SMRReq;
use crate::metric::UNORDERED_OPS_PER_SECOND_ID;

pub(super) fn start_unordered_rq_thread<O: ApplicationData>(unordered_rqs: UnorderedRqHandles<SMRReq<O>>,
                                           executor_handle: impl DecisionExecutorHandle<SMRReq<O>> + Send)
{
    std::thread::spawn(move || unordered_rq_thread::<O>(unordered_rqs, executor_handle));
}

fn unordered_rq_thread<O: ApplicationData>(unordered_rqs: UnorderedRqHandles<SMRReq<O>>,
                                             executor_handle: impl DecisionExecutorHandle<SMRReq<O>>)
{
    while let Ok(work_message) = unordered_rqs.recv() {
        let op_count = work_message.len();
        
        quiet_unwrap!(executor_handle.queue_update_unordered(work_message.into()));
        
        metric_increment(UNORDERED_OPS_PER_SECOND_ID, Some(op_count as u64));
    }
}