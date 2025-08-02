#[allow(dead_code)]
mod common;

#[cfg(test)]
mod tests {
    use crate::assert_snapshot;
    use crate::common::localhost::{start_localhost_context, NoopSessionBuilder};
    use crate::common::parquet::register_parquet_tables;
    use datafusion::physical_expr::Partitioning;
    use datafusion::physical_plan::{displayable, execute_stream};
    use datafusion_distributed::{assign_stages, ArrowFlightReadExec, ChannelManager};
    use futures::TryStreamExt;
    use std::error::Error;
    use std::sync::Arc;

    #[tokio::test]
    async fn highly_distributed_query() -> Result<(), Box<dyn Error>> {
        let (ctx, _guard) = start_localhost_context(
            [
                50050, 50051, 50053, 50054, 50055, 50056, 50057, 50058, 50059,
            ],
            NoopSessionBuilder,
        )
        .await;
        register_parquet_tables(&ctx).await?;

        let df = ctx.sql(r#"SELECT * FROM flights_1m"#).await?;
        let physical = df.create_physical_plan().await?;
        let physical_str = displayable(physical.as_ref()).indent(true).to_string();

        let mut physical_distributed = physical.clone();
        for size in [1, 10, 5] {
            physical_distributed = Arc::new(ArrowFlightReadExec::new(
                physical_distributed.clone(),
                Partitioning::RoundRobinBatch(size),
            ));
        }

        let physical_distributed = assign_stages(
            physical_distributed,
            &ChannelManager::try_from_session(ctx.task_ctx().session_config())?
                .as_ref()
                .get_urls()?,
        )?;

        let physical_distributed_str = displayable(physical_distributed.as_ref())
            .indent(true)
            .to_string();

        assert_snapshot!(physical_str,
            @"DataSourceExec: file_groups={1 group: [[/testdata/flights-1m.parquet]]}, projection=[FL_DATE, DEP_DELAY, ARR_DELAY, AIR_TIME, DISTANCE, DEP_TIME, ARR_TIME], file_type=parquet",
        );

        assert_snapshot!(physical_distributed_str,
            @r"
        ArrowFlightReadExec: input_tasks=5 hash_expr=[] stage_id=UUID input_stage_id=UUID input_hosts=[http://localhost:50050/, http://localhost:50051/, http://localhost:50053/, http://localhost:50054/, http://localhost:50055/]
          ArrowFlightReadExec: input_tasks=10 hash_expr=[] stage_id=UUID input_stage_id=UUID input_hosts=[http://localhost:50056/, http://localhost:50057/, http://localhost:50058/, http://localhost:50059/, http://localhost:50050/, http://localhost:50051/, http://localhost:50053/, http://localhost:50054/, http://localhost:50055/, http://localhost:50056/]
            ArrowFlightReadExec: input_tasks=1 hash_expr=[] stage_id=UUID input_stage_id=UUID input_hosts=[http://localhost:50057/]
              DataSourceExec: file_groups={1 group: [[/testdata/flights-1m.parquet]]}, projection=[FL_DATE, DEP_DELAY, ARR_DELAY, AIR_TIME, DISTANCE, DEP_TIME, ARR_TIME], file_type=parquet
        ",
        );

        let time = std::time::Instant::now();
        let batches = execute_stream(physical, ctx.task_ctx())?
            .try_collect::<Vec<_>>()
            .await?;
        println!("time: {:?}", time.elapsed());

        let time = std::time::Instant::now();
        let batches_distributed = execute_stream(physical_distributed, ctx.task_ctx())?
            .try_collect::<Vec<_>>()
            .await?;
        println!("time: {:?}", time.elapsed());

        assert_eq!(
            batches.iter().map(|v| v.num_rows()).sum::<usize>(),
            batches_distributed
                .iter()
                .map(|v| v.num_rows())
                .sum::<usize>(),
        );

        Ok(())
    }
}
