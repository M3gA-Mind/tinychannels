//! Adapter from the module's delivery state machine to host-owned storage.
use tinybus::Connection;
use tinychannels::delivery::{
    DeliveryQueueError, DeliveryQueueStore, DeliveryResultStore, QueuedDelivery,
};
use tinychannels_bus::module_delivery::{
    DELIVERY_PENDING_CAPACITY, DELIVERY_RECORD_BYTES, DELIVERY_RESULT_TTL_SECONDS,
};
use tinychannels_bus::names::{HOST_BUS_NAME, HOST_OBJECT_PATH, host_methods};

/// Typed callback adapter. A missing host method is an error; required sends
/// never substitute an in-memory or no-op store.
#[derive(Clone)]
pub(crate) struct HostDeliveryStore {
    connection: Connection,
}

impl HostDeliveryStore {
    pub(crate) fn new(connection: Connection) -> Self {
        Self { connection }
    }

    async fn call<A, R>(&self, method: &str, args: A) -> Result<R, DeliveryQueueError>
    where
        A: serde::Serialize,
        R: serde::de::DeserializeOwned,
    {
        self.connection
            .proxy(HOST_BUS_NAME, HOST_OBJECT_PATH, HOST_BUS_NAME)
            .map_err(store_error)?
            .call(method, args)
            .await
            .map_err(store_error)
    }
}

#[async_trait::async_trait]
impl DeliveryQueueStore for HostDeliveryStore {
    async fn save_pending(&self, entry: QueuedDelivery) -> Result<(), DeliveryQueueError> {
        validate_entry(&entry)?;
        let count = self.load_pending_all().await?.len();
        if let Some(existing) = self.load_pending(&entry.id).await? {
            if existing.intent != entry.intent {
                return Err(DeliveryQueueError::Store(
                    "delivery key conflicts with a different intent".into(),
                ));
            }
        } else if count >= DELIVERY_PENDING_CAPACITY {
            return Err(DeliveryQueueError::Store("delivery queue is full".into()));
        }
        self.call(host_methods::SAVE_PENDING_DELIVERY, (entry,))
            .await
    }

    async fn load_pending(&self, id: &str) -> Result<Option<QueuedDelivery>, DeliveryQueueError> {
        validate_id(id)?;
        let entry: Option<QueuedDelivery> = self
            .call(host_methods::LOAD_PENDING_DELIVERY, (id.to_owned(),))
            .await?;
        if let Some(entry) = &entry {
            validate_entry(entry)?;
        }
        Ok(entry)
    }

    async fn load_pending_all(&self) -> Result<Vec<QueuedDelivery>, DeliveryQueueError> {
        let entries: Vec<QueuedDelivery> =
            self.call(host_methods::LOAD_PENDING_DELIVERIES, ()).await?;
        if entries.len() > DELIVERY_PENDING_CAPACITY {
            return Err(DeliveryQueueError::Store(
                "host returned an oversized pending queue".into(),
            ));
        }
        for entry in &entries {
            validate_entry(entry)?;
        }
        Ok(entries)
    }

    async fn remove_pending(&self, id: &str) -> Result<(), DeliveryQueueError> {
        validate_id(id)?;
        self.call(host_methods::REMOVE_PENDING_DELIVERY, (id.to_owned(),))
            .await
    }

    async fn save_failed(&self, entry: QueuedDelivery) -> Result<(), DeliveryQueueError> {
        validate_entry(&entry)?;
        self.call(host_methods::SAVE_FAILED_DELIVERY, (entry,))
            .await
    }
}

#[async_trait::async_trait]
impl DeliveryResultStore for HostDeliveryStore {
    async fn load_failed(&self, id: &str) -> Result<Option<QueuedDelivery>, DeliveryQueueError> {
        validate_id(id)?;
        let entry: Option<QueuedDelivery> = self
            .call(host_methods::LOAD_FAILED_DELIVERY, (id.to_owned(),))
            .await?;
        if let Some(entry) = &entry {
            validate_entry(entry)?;
        }
        Ok(entry)
    }

    async fn load_result(
        &self,
        id: &str,
    ) -> Result<Option<tinychannels_bus::DeliveryTerminalResult>, DeliveryQueueError> {
        validate_id(id)?;
        let result: Option<tinychannels_bus::DeliveryTerminalResult> = self
            .call(host_methods::LOAD_DELIVERY_RESULT, (id.to_owned(),))
            .await?;
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis()
            .min(u64::MAX as u128) as u64;
        Ok(result.filter(|result| {
            result
                .completed_at_unix_ms
                .saturating_add(DELIVERY_RESULT_TTL_SECONDS.saturating_mul(1000))
                > now
        }))
    }

    async fn save_result(
        &self,
        result: tinychannels_bus::DeliveryTerminalResult,
    ) -> Result<(), DeliveryQueueError> {
        validate_id(&result.id)?;
        if serde_json::to_vec(&result).map_or(true, |bytes| bytes.len() > DELIVERY_RECORD_BYTES) {
            return Err(DeliveryQueueError::Store(
                "delivery result exceeds the record limit".into(),
            ));
        }
        self.call(host_methods::SAVE_DELIVERY_RESULT, (result,))
            .await
    }
}

fn validate_entry(entry: &QueuedDelivery) -> Result<(), DeliveryQueueError> {
    validate_id(&entry.id)?;
    if entry.intent.idempotency_key != entry.id
        || serde_json::to_vec(entry).map_or(true, |bytes| bytes.len() > DELIVERY_RECORD_BYTES)
    {
        return Err(DeliveryQueueError::Store(
            "delivery record is invalid or exceeds the record limit".into(),
        ));
    }
    Ok(())
}

fn validate_id(id: &str) -> Result<(), DeliveryQueueError> {
    if id.is_empty() || id.len() > 256 {
        return Err(DeliveryQueueError::Store("delivery id is invalid".into()));
    }
    Ok(())
}

fn store_error(error: tinybus::Error) -> DeliveryQueueError {
    if matches!(
        error.wire_name(),
        tinybus::Error::UNKNOWN_METHOD | "ai.tinyhumans.tinybus.Error.NameHasNoOwner"
    ) {
        DeliveryQueueError::Unsupported
    } else {
        DeliveryQueueError::Store(error.to_string())
    }
}
