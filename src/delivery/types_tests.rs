use super::*;

struct HandlerWithoutReconciliation;

#[async_trait]
impl DeliveryQueueHandler for HandlerWithoutReconciliation {
    async fn deliver(&self, _: &QueuedDelivery) -> DeliveryAttemptResult {
        panic!("reconciliation must not retry a send");
    }
}

#[tokio::test]
async fn default_reconciliation_reports_no_verdict_without_sending() {
    let handler: &dyn DeliveryQueueHandler = &HandlerWithoutReconciliation;
    assert_eq!(
        handler
            .reconcile_unknown_send(&QueuedDelivery::default())
            .await
            .unwrap(),
        None
    );
}
