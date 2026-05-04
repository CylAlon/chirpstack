use std::time::Duration;

use tracing::{error, info};

pub mod api;
pub mod device_query;
pub mod uplink_history;

pub async fn setup() {
    let retention_days = crate::config::get().alinkwise.history.retention_days;
    if retention_days == 0 {
        info!("Alinkwise history cleanup disabled");
        return;
    }

    tokio::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_secs(60 * 60 * 24));
        loop {
            interval.tick().await;
            match uplink_history::delete_expired(retention_days).await {
                Ok(deleted_count) => {
                    if deleted_count > 0 {
                        info!(
                            retention_days,
                            deleted_count, "Deleted expired Alinkwise history"
                        );
                    }
                }
                Err(error) => {
                    error!(error = %error, "Delete expired Alinkwise history failed");
                }
            }
        }
    });
}
