#[cfg(all(feature = "integration", test))]
mod tests {
    use async_trait::async_trait;
    use datafusion::arrow::util::pretty::pretty_format_batches;
    use datafusion::common::DataFusionError;
    use datafusion::datasource::file_format::parquet::ParquetFormat;
    use datafusion::datasource::listing::{
        ListingOptions, ListingTable, ListingTableConfig, ListingTableUrl,
    };
    use datafusion::physical_plan::collect;
    use datafusion::prelude::SessionContext;
    use datafusion_distributed::test_utils::localhost::start_localhost_context;
    use datafusion_distributed::test_utils::plan::distribute_repartitions;
    use datafusion_distributed::{assign_stages, assign_urls, SessionBuilder};
    use std::error::Error;
    use std::fs;
    use std::path::PathBuf;
    use std::sync::Arc;

    #[tokio::test]
    async fn test_tpch_1() -> Result<(), Box<dyn Error>> {
        test_tpch_query(1).await
    }

    #[tokio::test]
    async fn test_tpch_2() -> Result<(), Box<dyn Error>> {
        test_tpch_query(2).await
    }

    #[tokio::test]
    async fn test_tpch_3() -> Result<(), Box<dyn Error>> {
        test_tpch_query(3).await
    }

    #[tokio::test]
    async fn test_tpch_4() -> Result<(), Box<dyn Error>> {
        test_tpch_query(4).await
    }

    #[tokio::test]
    async fn test_tpch_5() -> Result<(), Box<dyn Error>> {
        test_tpch_query(5).await
    }

    #[tokio::test]
    async fn test_tpch_6() -> Result<(), Box<dyn Error>> {
        test_tpch_query(6).await
    }

    #[tokio::test]
    async fn test_tpch_7() -> Result<(), Box<dyn Error>> {
        test_tpch_query(7).await
    }

    #[tokio::test]
    async fn test_tpch_8() -> Result<(), Box<dyn Error>> {
        test_tpch_query(8).await
    }

    #[tokio::test]
    async fn test_tpch_9() -> Result<(), Box<dyn Error>> {
        test_tpch_query(9).await
    }

    #[tokio::test]
    async fn test_tpch_10() -> Result<(), Box<dyn Error>> {
        test_tpch_query(10).await
    }

    #[tokio::test]
    async fn test_tpch_11() -> Result<(), Box<dyn Error>> {
        test_tpch_query(11).await
    }

    #[tokio::test]
    async fn test_tpch_12() -> Result<(), Box<dyn Error>> {
        test_tpch_query(12).await
    }

    #[tokio::test]
    async fn test_tpch_13() -> Result<(), Box<dyn Error>> {
        test_tpch_query(13).await
    }

    #[tokio::test]
    async fn test_tpch_14() -> Result<(), Box<dyn Error>> {
        test_tpch_query(14).await
    }

    #[tokio::test]
    #[ignore]
    // TODO: Support query 15?
    // Skip because it contains DDL statements not supported in single SQL execution
    async fn test_tpch_15() -> Result<(), Box<dyn Error>> {
        test_tpch_query(15).await
    }

    #[tokio::test]
    async fn test_tpch_16() -> Result<(), Box<dyn Error>> {
        test_tpch_query(16).await
    }

    #[tokio::test]
    async fn test_tpch_17() -> Result<(), Box<dyn Error>> {
        test_tpch_query(17).await
    }

    #[tokio::test]
    async fn test_tpch_18() -> Result<(), Box<dyn Error>> {
        test_tpch_query(18).await
    }

    #[tokio::test]
    async fn test_tpch_19() -> Result<(), Box<dyn Error>> {
        test_tpch_query(19).await
    }

    #[tokio::test]
    async fn test_tpch_20() -> Result<(), Box<dyn Error>> {
        test_tpch_query(20).await
    }

    #[tokio::test]
    async fn test_tpch_21() -> Result<(), Box<dyn Error>> {
        test_tpch_query(21).await
    }

    #[tokio::test]
    async fn test_tpch_22() -> Result<(), Box<dyn Error>> {
        test_tpch_query(22).await
    }

    #[derive(Clone)]
    struct TPCHSessionBuilder;

    #[async_trait]
    impl SessionBuilder for TPCHSessionBuilder {
        async fn session_context(
            &self,
            ctx: SessionContext,
        ) -> Result<SessionContext, DataFusionError> {
            let root = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
            let base = root.join("testdata/tpch/parquet");
            let dir = fs::read_dir(base)?;
            for entry in dir {
                let entry = entry?;
                let table_name = entry.file_name().into_string().unwrap();

                let format = ParquetFormat::default()
                    .with_options(ctx.state().table_options().parquet.clone());

                let options = ListingOptions::new(Arc::new(format)).with_file_extension(".parquet");

                let table_path = ListingTableUrl::parse(entry.path().display().to_string())?;
                let schema = options.infer_schema(&ctx.state(), &table_path).await?;

                let config = ListingTableConfig::new(table_path)
                    .with_listing_options(options)
                    .with_schema(schema);

                ctx.register_table(&table_name, Arc::new(ListingTable::try_new(config)?))?;
            }

            Ok(ctx)
        }
    }

    async fn test_tpch_query(query_id: u8) -> Result<(), Box<dyn Error>> {
        let (ctx, _guard) =
            start_localhost_context([40041 + query_id as u32], TPCHSessionBuilder).await;

        let root = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        let sql = fs::read_to_string(root.join(format!("testdata/tpch/queries/q{query_id}.sql")))?;

        let logical_plan = ctx.sql(&sql).await?;
        let (state, logical_plan) = logical_plan.into_parts();
        let logical_plan = state.optimize(&logical_plan)?;

        // single node plan
        let physical_plan = state.create_physical_plan(&logical_plan).await?;
        let expected = collect(physical_plan.clone(), state.task_ctx()).await?;

        // distributed plan
        let physical_plan = state.create_physical_plan(&logical_plan).await?;
        let physical_plan = distribute_repartitions(physical_plan)?;
        let physical_plan = assign_stages(physical_plan)?;
        let physical_plan = assign_urls(physical_plan, &ctx)?;
        let actual = collect(physical_plan.clone(), state.task_ctx()).await?;

        let expected = pretty_format_batches(&expected)?.to_string();
        let actual = pretty_format_batches(&actual)?.to_string();
        assert_eq!(expected, actual);
        Ok(())
    }
}
