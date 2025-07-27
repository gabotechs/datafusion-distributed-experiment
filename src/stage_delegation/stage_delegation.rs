use super::StageContext;
use dashmap::{DashMap, Entry};
use datafusion::common::{exec_datafusion_err, exec_err};
use datafusion::error::DataFusionError;
use tokio::sync::oneshot;

/// In each stage of the distributed plan, there will be N workers. All these workers
/// need to coordinate to pull data from the next stage, which will contain M workers.
///
/// The way this is done is that for each stage, 1 worker is elected as "delegate", and
/// the rest of the workers are mere actors that wait for the delegate to tell them
/// where to go.
///
/// Each actor in a stage knows the url of the rest of the actors, so the delegate actor can
/// go one by one telling them what does the next stage look like. That way, all the actors
/// will agree on where to go to pull data from even if they are hosted in different physical
/// machines.
///
/// While starting a stage, several things can happen:
/// 1. The delegate can be very quick and choose the next stage context even before the other
///    actors have started waiting.
/// 2. The delegate can be very slow, and other actors might be waiting for the next context
///    info before the delegate even starting the choice of the next stage context.
///
/// On 1, the `add_delegate_info` call will create an entry in the [DashMap] with a
/// [oneshot::Receiver] already populated with the [StageContext], that other actors
/// are free to pick up at their own pace.
///
/// On 2, the `wait_for_delegate_info` call will create an entry in the [DashMap] with a
/// [oneshot::Sender], and listen on the other end of the channel [oneshot::Receiver] for
/// the delegate to put something there.
pub struct StageDelegation {
    stage_targets: DashMap<String, Oneof>,
}

impl StageDelegation {
    /// Puts the [StageContext] info so that an actor can pick it up with `wait_for_delegate_info`.
    ///
    /// - If the actor was already waiting for this info, it just puts it on the
    ///   existing transmitter end.
    /// - If no actor was waiting for this info, build a new channel and store the receiving end
    ///   so that actor can pick it up when it is ready.
    pub fn add_delegate_info(&self, stage_context: StageContext) -> Result<(), DataFusionError> {
        let tx = match self.stage_targets.entry(stage_context.id.clone()) {
            Entry::Occupied(entry) => match entry.get() {
                Oneof::Sender(_) => match entry.remove() {
                    Oneof::Sender(tx) => tx,
                    Oneof::Receiver(_) => unreachable!(),
                },
                // This call is idempotent. If there's already a Receiver end here, it means that
                // add_delegate_info() for the same stage_id was already called once.
                Oneof::Receiver(_) => return Ok(()),
            },
            Entry::Vacant(entry) => {
                let (tx, rx) = oneshot::channel();
                entry.insert(Oneof::Receiver(rx));
                tx
            }
        };

        // TODO: `send` does not wait for the other end of the channel to receive the message,
        //  so if nobody waits for it, we might leak an entry in `stage_targets` that will never
        //  be cleaned up. We can either:
        //  1. schedule a cleanup task that iterates the entries cleaning up old ones
        //  2. find some other API that allows us to .await until the other end receives the message,
        //     and on a timeout, cleanup the entry anyway.
        tx.send(stage_context)
            .map_err(|_| exec_datafusion_err!("Could not send stage context info"))
    }

    /// Waits for the [StageContext] info to be provided by the delegate and returns it.
    ///
    /// - If the delegate already put this info, consume it immediately and return it.
    /// - If the delegate did not put this info yet, create a new channel for the delegate to
    ///   store the info, and wait for that to happen, returning the info when it's ready.
    pub async fn wait_for_delegate_info(
        &self,
        stage_id: String,
    ) -> Result<StageContext, DataFusionError> {
        let rx = match self.stage_targets.entry(stage_id) {
            Entry::Occupied(entry) => match entry.get() {
                Oneof::Sender(_) => return exec_err!("Programming error: while waiting for delegate info the entry in the StageDelegation target map cannot be a Sender"),
                Oneof::Receiver(_) => match entry.remove() {
                    Oneof::Sender(_) => unreachable!(),
                    Oneof::Receiver(rx) => rx
                },
            },
            Entry::Vacant(entry) => {
                let (tx, rx) = oneshot::channel();
                entry.insert(Oneof::Sender(tx));
                rx
            }
        };

        // TODO: add a timeout here.
        rx.await.map_err(|err| {
            exec_datafusion_err!(
                "Error waiting for delegate to tell us in which stage we are in: {err}"
            )
        })
    }
}

enum Oneof {
    Sender(oneshot::Sender<StageContext>),
    Receiver(oneshot::Receiver<StageContext>),
}

impl Default for StageDelegation {
    fn default() -> Self {
        Self {
            stage_targets: DashMap::new(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::stage_delegation::stage_context::PreviousStage;
    use crate::stage_delegation::StageContext;
    use std::sync::Arc;
    use uuid::Uuid;

    fn create_test_stage_context(id: String) -> StageContext {
        StageContext {
            id: id.to_string(),
            previous_stage: None,
            current: 0,
            delegate: 1,
            actors: vec![
                "http://localhost:8080".to_string(),
                "http://localhost:8081".to_string(),
            ],
            partitioning: Default::default()
        }
    }

    #[tokio::test]
    async fn test_delegate_first_then_actor_waits() {
        let delegation = StageDelegation::default();
        let stage_id = Uuid::new_v4().to_string();
        let stage_context = create_test_stage_context(stage_id.clone());

        // Delegate adds info first
        delegation.add_delegate_info(stage_context.clone()).unwrap();

        // Actor waits for info (should get it immediately)
        let received_context = delegation.wait_for_delegate_info(stage_id).await.unwrap();
        assert_eq!(received_context.id, stage_context.id);
        assert_eq!(received_context.current, stage_context.current);
        assert_eq!(received_context.delegate, stage_context.delegate);
        assert_eq!(received_context.actors, stage_context.actors);

        // The stage target was cleaned up.
        assert_eq!(delegation.stage_targets.len(), 0);
    }

    #[tokio::test]
    async fn test_actor_waits_first_then_delegate_adds() {
        let delegation = Arc::new(StageDelegation::default());
        let stage_id = Uuid::new_v4().to_string();
        let stage_context = create_test_stage_context(stage_id.clone());

        // Spawn a task that waits for delegate info
        let delegation_clone = Arc::clone(&delegation);
        let wait_task =
            tokio::spawn(async move { delegation_clone.wait_for_delegate_info(stage_id).await });

        // Give the wait task a moment to start
        tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;

        // Delegate adds info
        delegation.add_delegate_info(stage_context.clone()).unwrap();

        // Wait task should complete with the stage context
        let received_context = wait_task.await.unwrap().unwrap();
        assert_eq!(received_context.id, stage_context.id);
        assert_eq!(received_context.current, stage_context.current);
        assert_eq!(received_context.delegate, stage_context.delegate);
        assert_eq!(received_context.actors, stage_context.actors);

        // The stage target was cleaned up.
        assert_eq!(delegation.stage_targets.len(), 0);
    }

    #[tokio::test]
    async fn test_multiple_actors_waiting_for_same_stage() {
        let delegation = Arc::new(StageDelegation::default());
        let stage_id = Uuid::new_v4().to_string();
        let stage_context = create_test_stage_context(stage_id.clone());

        // First actor waits
        let delegation_clone1 = Arc::clone(&delegation);
        let id = stage_id.clone();
        let wait_task1 =
            tokio::spawn(async move { delegation_clone1.wait_for_delegate_info(id).await });

        // Give the first wait task a moment to start
        tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;

        // Second actor tries to wait for the same stage - this should fail gracefully
        // since there can only be one waiting receiver per stage
        let result = delegation.wait_for_delegate_info(stage_id).await;
        assert!(result.is_err());

        // Delegate adds info - the first actor should receive it
        delegation.add_delegate_info(stage_context.clone()).unwrap();

        let received_context = wait_task1.await.unwrap().unwrap();
        assert_eq!(received_context.id, stage_context.id);
    }

    #[tokio::test]
    async fn test_different_stages_concurrent() {
        let delegation = Arc::new(StageDelegation::default());
        let stage_id1 = Uuid::new_v4().to_string();
        let stage_id2 = Uuid::new_v4().to_string();
        let stage_context1 = create_test_stage_context(stage_id1.clone());
        let stage_context2 = create_test_stage_context(stage_id2.clone());

        // Both actors wait for different stages
        let delegation_clone1 = Arc::clone(&delegation);
        let delegation_clone2 = Arc::clone(&delegation);
        let wait_task1 =
            tokio::spawn(async move { delegation_clone1.wait_for_delegate_info(stage_id1).await });
        let wait_task2 =
            tokio::spawn(async move { delegation_clone2.wait_for_delegate_info(stage_id2).await });

        // Give wait tasks a moment to start
        tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;

        // Delegates add info for both stages
        delegation
            .add_delegate_info(stage_context1.clone())
            .unwrap();
        delegation
            .add_delegate_info(stage_context2.clone())
            .unwrap();

        // Both should receive their respective contexts
        let received_context1 = wait_task1.await.unwrap().unwrap();
        let received_context2 = wait_task2.await.unwrap().unwrap();

        assert_eq!(received_context1.id, stage_context1.id.to_string());
        assert_eq!(received_context2.id, stage_context2.id.to_string());

        // The stage target was cleaned up.
        assert_eq!(delegation.stage_targets.len(), 0);
    }

    #[tokio::test]
    async fn test_stage_context_with_previous_stage() {
        let delegation = StageDelegation::default();
        let stage_id = Uuid::new_v4().to_string();
        let previous_id = Uuid::new_v4().to_string();

        let stage_context = StageContext {
            id: stage_id.to_string(),
            previous_stage: Some(PreviousStage {
                id: previous_id.to_string(),
                caller: 2,
                actors: vec!["http://localhost:8079".to_string()],
            }),
            current: 1,
            delegate: 2,
            actors: vec![
                "http://localhost:8080".to_string(),
                "http://localhost:8081".to_string(),
            ],
            partitioning: None
        };

        // Delegate adds info first
        delegation.add_delegate_info(stage_context.clone()).unwrap();

        // Actor waits for info
        let received_context = delegation.wait_for_delegate_info(stage_id).await.unwrap();
        assert_eq!(received_context.id, stage_context.id);
        assert_eq!(received_context.current, stage_context.current);
        assert_eq!(received_context.delegate, stage_context.delegate);
        assert_eq!(received_context.actors, stage_context.actors);

        if let (Some(orig_prev), Some(recv_prev)) = (
            &stage_context.previous_stage,
            &received_context.previous_stage,
        ) {
            assert_eq!(orig_prev.id, recv_prev.id);
            assert_eq!(orig_prev.caller, recv_prev.caller);
            assert_eq!(orig_prev.actors, recv_prev.actors);
        } else {
            panic!("Previous stage should be present");
        }
    }

    #[tokio::test]
    async fn test_add_delegate_info_twice_same_stage() {
        let delegation = StageDelegation::default();
        let stage_id = Uuid::new_v4().to_string();
        let stage_context = create_test_stage_context(stage_id.clone());

        // First add should succeed
        delegation.add_delegate_info(stage_context.clone()).unwrap();

        // Second add for same stage should succeed (idempotent)
        delegation.add_delegate_info(stage_context.clone()).unwrap();

        // Receiving should still work even if `add_delegate_info` was called two times
        let received_context = delegation.wait_for_delegate_info(stage_id).await.unwrap();
        assert_eq!(received_context.id, stage_context.id);
        assert_eq!(received_context.current, stage_context.current);
        assert_eq!(received_context.delegate, stage_context.delegate);
        assert_eq!(received_context.actors, stage_context.actors);
    }
}
