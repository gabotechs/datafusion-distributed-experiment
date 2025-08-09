use crate::context::{InputStage, StageContext};
use crate::{ArrowFlightReadExec, ChannelManager};
use datafusion::common::internal_datafusion_err;
use datafusion::common::tree_node::{Transformed, TreeNode, TreeNodeRecursion};
use datafusion::error::DataFusionError;
use datafusion::physical_plan::ExecutionPlan;
use std::ops::AddAssign;
use std::sync::Arc;
use uuid::Uuid;

pub fn assign_stages(
    plan: Arc<dyn ExecutionPlan>,
) -> Result<Arc<dyn ExecutionPlan>, DataFusionError> {
    Ok(_assign_stages_inner(plan, Uuid::new_v4(), &mut 0, 1)?.0)
}

fn _assign_stages_inner(
    plan: Arc<dyn ExecutionPlan>,
    query_id: Uuid,
    idx: &mut usize,
    n_tasks: usize,
) -> Result<(Arc<dyn ExecutionPlan>, usize), DataFusionError> {
    let stage = StageContext {
        query_id,
        idx: fetch_add(idx),
        n_tasks,
        input: None,
    };

    let transformed = plan.transform_down(|plan| {
        let Some(node) = plan.as_any().downcast_ref::<ArrowFlightReadExec>() else {
            return Ok(Transformed::no(plan));
        };
        let child = Arc::clone(node.children().first().cloned().ok_or(
            internal_datafusion_err!("Expected ArrowFlightExecRead to have a child"),
        )?);

        let input_n_tasks = node.properties().partitioning.partition_count();
        let (child, input_stage_idx) = _assign_stages_inner(child, query_id, idx, input_n_tasks)?;

        let mut stage = stage.clone();
        stage.input = Some(InputStage {
            idx: input_stage_idx,
            tasks: vec![None; input_n_tasks],
        });
        let mut node = node.clone();
        node.stage_context = Some(stage);

        Ok(Transformed::new(
            Arc::new(node).with_new_children(vec![child])?,
            true,
            TreeNodeRecursion::Jump,
        ))
    })?;

    Ok((transformed.data, stage.idx))
}

fn fetch_add(n: &mut usize) -> usize {
    let curr = *n;
    n.add_assign(1);
    curr
}

pub fn assign_urls(
    plan: Arc<dyn ExecutionPlan>,
    channel_manager: impl TryInto<ChannelManager, Error = DataFusionError>,
) -> Result<Arc<dyn ExecutionPlan>, DataFusionError> {
    let mut i = 0;

    let urls = channel_manager.try_into()?.get_urls()?;

    let transformed = plan.transform_down(|plan| {
        let Some(node) = plan.as_any().downcast_ref::<ArrowFlightReadExec>() else {
            return Ok(Transformed::no(plan));
        };

        let Some(ref ctx) = node.stage_context else {
            return Ok(Transformed::no(plan));
        };

        let mut ctx = ctx.clone();

        let Some(ref mut input) = ctx.input else {
            return Ok(Transformed::no(plan));
        };

        for task in input.tasks.iter_mut() {
            // Round-robin
            *task = Some(urls[i % urls.len()].clone());
            i += 1;
        }

        let mut node = node.clone();
        node.stage_context = Some(ctx);
        Ok(Transformed::yes(Arc::new(node)))
    })?;

    Ok(transformed.data)
}
