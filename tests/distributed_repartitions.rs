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
        WITH temp_by_dir AS (
            SELECT
                "WindGustDir",
                AVG("MaxTemp") AS avg_max_temp
            FROM weather
            WHERE "MaxTemp" IS NOT NULL
            GROUP BY "WindGustDir"
        )
        SELECT
            t."WindGustDir",
            t.avg_max_temp,
            COUNT(*) AS days_count
        FROM temp_by_dir t
        JOIN weather w
          ON w."WindGustDir" = t."WindGustDir"
        WHERE w."Humidity3pm" IS NOT NULL
        GROUP BY t."WindGustDir", t.avg_max_temp
        ORDER BY days_count DESC;
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
        SortPreservingMergeExec: [days_count@2 DESC]
          SortExec: expr=[days_count@2 DESC], preserve_partitioning=[true]
            ProjectionExec: expr=[WindGustDir@0 as WindGustDir, avg_max_temp@1 as avg_max_temp, count(Int64(1))@2 as days_count]
              AggregateExec: mode=FinalPartitioned, gby=[WindGustDir@0 as WindGustDir, avg_max_temp@1 as avg_max_temp], aggr=[count(Int64(1))]
                CoalesceBatchesExec: target_batch_size=8192
                  RepartitionExec: partitioning=Hash([WindGustDir@0, avg_max_temp@1], 3), input_partitions=3
                    AggregateExec: mode=Partial, gby=[WindGustDir@0 as WindGustDir, avg_max_temp@1 as avg_max_temp], aggr=[count(Int64(1))]
                      CoalesceBatchesExec: target_batch_size=8192
                        HashJoinExec: mode=CollectLeft, join_type=Inner, on=[(WindGustDir@0, WindGustDir@0)], projection=[WindGustDir@0, avg_max_temp@1]
                          CoalescePartitionsExec
                            ProjectionExec: expr=[WindGustDir@0 as WindGustDir, avg(weather.MaxTemp)@1 as avg_max_temp]
                              AggregateExec: mode=FinalPartitioned, gby=[WindGustDir@0 as WindGustDir], aggr=[avg(weather.MaxTemp)]
                                CoalesceBatchesExec: target_batch_size=8192
                                  RepartitionExec: partitioning=Hash([WindGustDir@0], 3), input_partitions=3
                                    AggregateExec: mode=Partial, gby=[WindGustDir@1 as WindGustDir], aggr=[avg(weather.MaxTemp)]
                                      CoalesceBatchesExec: target_batch_size=8192
                                        FilterExec: MaxTemp@0 IS NOT NULL
                                          RepartitionExec: partitioning=RoundRobinBatch(3), input_partitions=1
                                            DataSourceExec: file_groups={1 group: [[/testdata/weather.parquet]]}, projection=[MaxTemp, WindGustDir], file_type=parquet, predicate=MaxTemp@0 IS NOT NULL, pruning_predicate=MaxTemp_null_count@1 != row_count@0, required_guarantees=[]

                          CoalesceBatchesExec: target_batch_size=8192
                            FilterExec: Humidity3pm@1 IS NOT NULL, projection=[WindGustDir@0]
                              RepartitionExec: partitioning=RoundRobinBatch(3), input_partitions=1
                                DataSourceExec: file_groups={1 group: [[/testdata/weather.parquet]]}, projection=[WindGustDir, Humidity3pm], file_type=parquet, predicate=Humidity3pm@1 IS NOT NULL, pruning_predicate=Humidity3pm_null_count@1 != row_count@0, required_guarantees=[]
        ",
        );

        assert_snapshot!(physical_distributed_str,
            @r"
        SortPreservingMergeExec: [days_count@2 DESC]
          SortExec: expr=[days_count@2 DESC], preserve_partitioning=[true]
            ProjectionExec: expr=[WindGustDir@0 as WindGustDir, avg_max_temp@1 as avg_max_temp, count(Int64(1))@2 as days_count]
              AggregateExec: mode=FinalPartitioned, gby=[WindGustDir@0 as WindGustDir, avg_max_temp@1 as avg_max_temp], aggr=[count(Int64(1))]
                CoalesceBatchesExec: target_batch_size=8192
                  RepartitionExec: partitioning=Hash([WindGustDir@0, avg_max_temp@1], 3), input_partitions=3
                    ArrowFlightReadExec: stage_idx=0 input_stage_idx=1 input_tasks=3 hash_expr=[WindGustDir@0, avg_max_temp@1]
                      AggregateExec: mode=Partial, gby=[WindGustDir@0 as WindGustDir, avg_max_temp@1 as avg_max_temp], aggr=[count(Int64(1))]
                        CoalesceBatchesExec: target_batch_size=8192
                          HashJoinExec: mode=CollectLeft, join_type=Inner, on=[(WindGustDir@0, WindGustDir@0)], projection=[WindGustDir@0, avg_max_temp@1]
                            CoalescePartitionsExec
                              ProjectionExec: expr=[WindGustDir@0 as WindGustDir, avg(weather.MaxTemp)@1 as avg_max_temp]
                                AggregateExec: mode=FinalPartitioned, gby=[WindGustDir@0 as WindGustDir], aggr=[avg(weather.MaxTemp)]
                                  CoalesceBatchesExec: target_batch_size=8192
                                    RepartitionExec: partitioning=Hash([WindGustDir@0], 3), input_partitions=3
                                      ArrowFlightReadExec: stage_idx=1 input_stage_idx=2 input_tasks=3 hash_expr=[WindGustDir@0]
                                        AggregateExec: mode=Partial, gby=[WindGustDir@1 as WindGustDir], aggr=[avg(weather.MaxTemp)]
                                          CoalesceBatchesExec: target_batch_size=8192
                                            FilterExec: MaxTemp@0 IS NOT NULL
                                              RepartitionExec: partitioning=RoundRobinBatch(3), input_partitions=1
                                                ArrowFlightReadExec: stage_idx=2 input_stage_idx=3 input_tasks=1
                                                  DataSourceExec: file_groups={1 group: [[/testdata/weather.parquet]]}, projection=[MaxTemp, WindGustDir], file_type=parquet, predicate=MaxTemp@0 IS NOT NULL, pruning_predicate=MaxTemp_null_count@1 != row_count@0, required_guarantees=[]

                            CoalesceBatchesExec: target_batch_size=8192
                              FilterExec: Humidity3pm@1 IS NOT NULL, projection=[WindGustDir@0]
                                RepartitionExec: partitioning=RoundRobinBatch(3), input_partitions=1
                                  ArrowFlightReadExec: stage_idx=1 input_stage_idx=4 input_tasks=1
                                    DataSourceExec: file_groups={1 group: [[/testdata/weather.parquet]]}, projection=[WindGustDir, Humidity3pm], file_type=parquet, predicate=Humidity3pm@1 IS NOT NULL, pruning_predicate=Humidity3pm_null_count@1 != row_count@0, required_guarantees=[]
        ",
        );

        let batches = pretty_format_batches(
            &execute_stream(physical, ctx.task_ctx())?
                .try_collect::<Vec<_>>()
                .await?,
        )?;

        assert_snapshot!(batches, @r"
        +-------------+--------------------+------------+
        | WindGustDir | avg_max_temp       | days_count |
        +-------------+--------------------+------------+
        | NW          | 19.47808219178083  | 73         |
        | NNW         | 20.459090909090914 | 44         |
        | E           | 24.086486486486482 | 37         |
        | WNW         | 17.53714285714286  | 35         |
        | ENE         | 23.333333333333336 | 30         |
        | ESE         | 20.82173913043478  | 23         |
        | S           | 17.013636363636365 | 22         |
        | N           | 18.538095238095238 | 21         |
        | W           | 19.509999999999998 | 20         |
        | NE          | 25.89375           | 16         |
        | SSE         | 18.749999999999996 | 12         |
        | SE          | 18.491666666666667 | 12         |
        | NNE         | 23.7375            | 8          |
        | SSW         | 23.4               | 5          |
        | NA          | 16.46666666666667  | 3          |
        | SW          | 27.0               | 3          |
        | WSW         | 31.55              | 2          |
        +-------------+--------------------+------------+
        ");

        let batches_distributed = pretty_format_batches(
            &execute_stream(physical_distributed, ctx.task_ctx())?
                .try_collect::<Vec<_>>()
                .await?,
        )?;
        assert_snapshot!(batches_distributed, @r"
        +-------------+--------------------+------------+
        | WindGustDir | avg_max_temp       | days_count |
        +-------------+--------------------+------------+
        | NW          | 19.47808219178083  | 73         |
        | NNW         | 20.459090909090914 | 44         |
        | E           | 24.086486486486482 | 37         |
        | WNW         | 17.53714285714286  | 35         |
        | ENE         | 23.333333333333336 | 30         |
        | ESE         | 20.82173913043478  | 23         |
        | S           | 17.013636363636365 | 22         |
        | N           | 18.538095238095238 | 21         |
        | W           | 19.509999999999998 | 20         |
        | NE          | 25.89375           | 16         |
        | SSE         | 18.749999999999996 | 12         |
        | SE          | 18.491666666666667 | 12         |
        | NNE         | 23.7375            | 8          |
        | SSW         | 23.4               | 5          |
        | NA          | 16.46666666666667  | 3          |
        | SW          | 27.0               | 3          |
        | WSW         | 31.55              | 2          |
        +-------------+--------------------+------------+
        ");

        Ok(())
    }
}
