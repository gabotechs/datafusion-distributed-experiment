use url::Url;
use uuid::Uuid;

#[derive(Debug, Clone)]
pub struct StageContext {
    /// Unique identifier of the query.
    pub query_id: Uuid,
    /// Unique identifier of the stage in the query.
    pub idx: usize,
    /// Number of tasks involved in the query.
    pub n_tasks: usize,
    /// Unique identifiers of the input stages.
    pub input: Option<InputStage>,
}

#[derive(Debug, Clone)]
pub struct InputStage {
    /// Unique identifier of the input Stage.
    pub idx: usize,
    /// Urls of the input tasks
    pub tasks: Vec<Option<Url>>,
}

#[derive(Debug, Clone)]
pub struct StageTaskContext {
    /// Index of the current task in a stage
    pub task_idx: usize,
}
