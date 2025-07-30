use datafusion::execution::SessionStateBuilder;

pub trait SessionBuilder {
    fn on_new_session(&self, builder: SessionStateBuilder) -> SessionStateBuilder;
}

pub struct NoopSessionBuilder;

impl SessionBuilder for NoopSessionBuilder {
    fn on_new_session(&self, builder: SessionStateBuilder) -> SessionStateBuilder {
        builder
    }
}
