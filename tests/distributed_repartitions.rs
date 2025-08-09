#[cfg(all(feature = "integration", test))]
mod tests {
    use datafusion::arrow::util::pretty::pretty_format_batches;
    use datafusion::physical_plan::{displayable, execute_stream};
    use datafusion_distributed::test_utils::localhost::start_localhost_context;
    use datafusion_distributed::test_utils::parquet::register_parquet_tables;
    use datafusion_distributed::test_utils::plan::distribute_repartitions;
    use datafusion_distributed::{assert_snapshot, assign_stages, assign_urls, NoopSessionBuilder};
    use futures::TryStreamExt;
    use std::error::Error;

    #[tokio::test]
    async fn distributed_repartitions() -> Result<(), Box<dyn Error>> {
        let (ctx, _guard) =
            start_localhost_context([50050, 50051, 50052], NoopSessionBuilder).await;
        register_parquet_tables(&ctx).await?;

        let query = r#"
        WITH a AS (
            SELECT
                AVG("MinTemp") as "MinTemp",
                "RainTomorrow"
            FROM weather
            WHERE "RainToday" = 'yes'
            GROUP BY "RainTomorrow"
        ), b AS (
            SELECT
                AVG("MaxTemp") as "MaxTemp",
                "RainTomorrow"
            FROM weather
            WHERE "RainToday" = 'no'
            GROUP BY "RainTomorrow"
        )
        SELECT
            a."MinTemp",
            b."MaxTemp"
        FROM a
        LEFT JOIN b
        ON a."RainTomorrow" = b."RainTomorrow"

        "#;

        let df = ctx.sql(query).await?;
        let physical = df.create_physical_plan().await?;
        let physical_str = displayable(physical.as_ref()).indent(true).to_string();

        let physical_distributed = distribute_repartitions(physical.clone())?;
        let physical_distributed = assign_stages(physical_distributed)?;
        let physical_distributed = assign_urls(physical_distributed, &ctx)?;

        let physical_distributed_str = displayable(physical_distributed.as_ref())
            .indent(true)
            .to_string();

        assert_snapshot!(physical_str,
            @r"
        CoalesceBatchesExec: target_batch_size=8192
          HashJoinExec: mode=CollectLeft, join_type=Left, on=[(RainTomorrow@1, RainTomorrow@1)], projection=[MinTemp@0, MaxTemp@2]
            CoalescePartitionsExec
              ProjectionExec: expr=[avg(weather.MinTemp)@1 as MinTemp, RainTomorrow@0 as RainTomorrow]
                AggregateExec: mode=FinalPartitioned, gby=[RainTomorrow@0 as RainTomorrow], aggr=[avg(weather.MinTemp)]
                  CoalesceBatchesExec: target_batch_size=8192
                    RepartitionExec: partitioning=Hash([RainTomorrow@0], 3), input_partitions=3
                      AggregateExec: mode=Partial, gby=[RainTomorrow@1 as RainTomorrow], aggr=[avg(weather.MinTemp)]
                        CoalesceBatchesExec: target_batch_size=8192
                          FilterExec: RainToday@1 = yes, projection=[MinTemp@0, RainTomorrow@2]
                            RepartitionExec: partitioning=RoundRobinBatch(3), input_partitions=1
                              DataSourceExec: file_groups={1 group: [[/testdata/weather.parquet]]}, projection=[MinTemp, RainToday, RainTomorrow], file_type=parquet, predicate=RainToday@1 = yes, pruning_predicate=RainToday_null_count@2 != row_count@3 AND RainToday_min@0 <= yes AND yes <= RainToday_max@1, required_guarantees=[RainToday in (yes)]

            ProjectionExec: expr=[avg(weather.MaxTemp)@1 as MaxTemp, RainTomorrow@0 as RainTomorrow]
              AggregateExec: mode=FinalPartitioned, gby=[RainTomorrow@0 as RainTomorrow], aggr=[avg(weather.MaxTemp)]
                CoalesceBatchesExec: target_batch_size=8192
                  RepartitionExec: partitioning=Hash([RainTomorrow@0], 3), input_partitions=3
                    AggregateExec: mode=Partial, gby=[RainTomorrow@1 as RainTomorrow], aggr=[avg(weather.MaxTemp)]
                      CoalesceBatchesExec: target_batch_size=8192
                        FilterExec: RainToday@1 = no, projection=[MaxTemp@0, RainTomorrow@2]
                          RepartitionExec: partitioning=RoundRobinBatch(3), input_partitions=1
                            DataSourceExec: file_groups={1 group: [[/testdata/weather.parquet]]}, projection=[MaxTemp, RainToday, RainTomorrow], file_type=parquet, predicate=RainToday@1 = no, pruning_predicate=RainToday_null_count@2 != row_count@3 AND RainToday_min@0 <= no AND no <= RainToday_max@1, required_guarantees=[RainToday in (no)]
        ",
        );

        assert_snapshot!(physical_distributed_str,
            @r"
        CoalesceBatchesExec: target_batch_size=8192
          HashJoinExec: mode=CollectLeft, join_type=Left, on=[(RainTomorrow@1, RainTomorrow@1)], projection=[MinTemp@0, MaxTemp@2]
            CoalescePartitionsExec
              ProjectionExec: expr=[avg(weather.MinTemp)@1 as MinTemp, RainTomorrow@0 as RainTomorrow]
                AggregateExec: mode=FinalPartitioned, gby=[RainTomorrow@0 as RainTomorrow], aggr=[avg(weather.MinTemp)]
                  CoalesceBatchesExec: target_batch_size=8192
                    ArrowFlightReadExec: stage_idx=0 input_stage_idx=1 input_tasks=3 hash_expr=[RainTomorrow@0]
                      AggregateExec: mode=Partial, gby=[RainTomorrow@1 as RainTomorrow], aggr=[avg(weather.MinTemp)]
                        CoalesceBatchesExec: target_batch_size=8192
                          FilterExec: RainToday@1 = yes, projection=[MinTemp@0, RainTomorrow@2]
                            ArrowFlightReadExec: stage_idx=1 input_stage_idx=2 input_tasks=3
                              DataSourceExec: file_groups={1 group: [[/testdata/weather.parquet]]}, projection=[MinTemp, RainToday, RainTomorrow], file_type=parquet, predicate=RainToday@1 = yes, pruning_predicate=RainToday_null_count@2 != row_count@3 AND RainToday_min@0 <= yes AND yes <= RainToday_max@1, required_guarantees=[RainToday in (yes)]

            ProjectionExec: expr=[avg(weather.MaxTemp)@1 as MaxTemp, RainTomorrow@0 as RainTomorrow]
              AggregateExec: mode=FinalPartitioned, gby=[RainTomorrow@0 as RainTomorrow], aggr=[avg(weather.MaxTemp)]
                CoalesceBatchesExec: target_batch_size=8192
                  ArrowFlightReadExec: stage_idx=0 input_stage_idx=3 input_tasks=3 hash_expr=[RainTomorrow@0]
                    AggregateExec: mode=Partial, gby=[RainTomorrow@1 as RainTomorrow], aggr=[avg(weather.MaxTemp)]
                      CoalesceBatchesExec: target_batch_size=8192
                        FilterExec: RainToday@1 = no, projection=[MaxTemp@0, RainTomorrow@2]
                          ArrowFlightReadExec: stage_idx=3 input_stage_idx=4 input_tasks=3
                            DataSourceExec: file_groups={1 group: [[/testdata/weather.parquet]]}, projection=[MaxTemp, RainToday, RainTomorrow], file_type=parquet, predicate=RainToday@1 = no, pruning_predicate=RainToday_null_count@2 != row_count@3 AND RainToday_min@0 <= no AND no <= RainToday_max@1, required_guarantees=[RainToday in (no)]
        ",
        );

        let batches = pretty_format_batches(
            &execute_stream(physical, ctx.task_ctx())?
                .try_collect::<Vec<_>>()
                .await?,
        )?;

        assert_snapshot!(batches, @r"
        ++
        ++
        ");

        let batches_distributed = pretty_format_batches(
            &execute_stream(physical_distributed, ctx.task_ctx())?
                .try_collect::<Vec<_>>()
                .await?,
        )?;
        assert_snapshot!(batches_distributed, @r"
        ++
        ++
        ");

        Ok(())
    }
}
