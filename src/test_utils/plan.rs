use crate::ArrowFlightReadExec;
use datafusion::common::plan_err;
use datafusion::common::tree_node::{Transformed, TreeNode};
use datafusion::error::DataFusionError;
use datafusion::physical_expr::Partitioning;
use datafusion::physical_plan::aggregates::{AggregateExec, AggregateMode};
use datafusion::physical_plan::repartition::RepartitionExec;
use datafusion::physical_plan::ExecutionPlan;
use std::sync::Arc;

pub fn distribute_aggregate(
    plan: Arc<dyn ExecutionPlan>,
) -> Result<Arc<dyn ExecutionPlan>, DataFusionError> {
    let mut aggregate_partial_found = false;
    let transformed = plan.transform_up(|node| {
        let Some(agg) = node.as_any().downcast_ref::<AggregateExec>() else {
            return Ok(Transformed::no(node));
        };

        match agg.mode() {
            AggregateMode::Partial => {
                if aggregate_partial_found {
                    return plan_err!("Two consecutive partial aggregations found");
                }
                aggregate_partial_found = true;
                let expr = agg
                    .group_expr()
                    .expr()
                    .iter()
                    .map(|(v, _)| Arc::clone(v))
                    .collect::<Vec<_>>();

                if node.children().len() != 1 {
                    return plan_err!("Aggregate must have exactly one child");
                }
                let child = node.children()[0].clone();

                let node = node.with_new_children(vec![Arc::new(ArrowFlightReadExec::new(
                    child,
                    Partitioning::Hash(expr, 1),
                ))])?;
                Ok(Transformed::yes(node))
            }
            AggregateMode::Final
            | AggregateMode::FinalPartitioned
            | AggregateMode::Single
            | AggregateMode::SinglePartitioned => {
                if !aggregate_partial_found {
                    return plan_err!("No partial aggregate found before the final one");
                }

                if node.children().len() != 1 {
                    return plan_err!("Aggregate must have exactly one child");
                }
                let child = node.children()[0].clone();

                let node = node.with_new_children(vec![Arc::new(ArrowFlightReadExec::new(
                    child,
                    Partitioning::RoundRobinBatch(8),
                ))])?;
                Ok(Transformed::yes(node))
            }
        }
    })?;

    Ok(transformed.data)
}

pub fn distribute_repartitions(
    plan: Arc<dyn ExecutionPlan>,
) -> Result<Arc<dyn ExecutionPlan>, DataFusionError> {
    let transformed = plan.transform_up(|node| {
        let Some(repartition_exec) = node.as_any().downcast_ref::<RepartitionExec>() else {
            return Ok(Transformed::no(node));
        };
        let Some(repartition_exec_child) = repartition_exec.children().first().cloned() else {
            return plan_err!("Repartition must have exactly one child");
        };

        let n = repartition_exec_child
            .properties()
            .partitioning
            .partition_count();

        let arrow_flight_exec = Arc::new(ArrowFlightReadExec::new(
            Arc::clone(repartition_exec_child),
            match repartition_exec.partitioning() {
                Partitioning::RoundRobinBatch(_) => Partitioning::RoundRobinBatch(n),
                Partitioning::Hash(exprs, _) => Partitioning::Hash(exprs.to_vec(), n),
                Partitioning::UnknownPartitioning(_) => Partitioning::UnknownPartitioning(n),
            },
        ));

        let new_node = node.with_new_children(vec![arrow_flight_exec])?;

        Ok(Transformed::yes(new_node))
    })?;

    Ok(transformed.data)
}
