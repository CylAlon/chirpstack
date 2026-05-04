use std::collections::HashMap;

use chrono::{DateTime, Duration, Utc};
#[cfg(any(feature = "postgres", feature = "sqlite"))]
use diesel::sql_types::Bool;
use diesel::{dsl, prelude::*};
use diesel_async::RunQueryDsl;
use lrwn::EUI64;
use uuid::Uuid;

use crate::storage::device::DeviceClass;
use crate::storage::schema::{application, device, device_profile};
use crate::storage::{error::Error, fields, get_async_db_conn};

#[derive(Clone, Debug)]
pub enum OnlineState {
    All,
    Online,
    Offline,
    NeverSeen,
    Disabled,
}

#[derive(Clone, Debug)]
pub enum OrderBy {
    Name,
    DevEui,
    LastSeenAt,
    DeviceProfileName,
    ApplicationName,
    CreatedAt,
    UpdatedAt,
}

#[derive(Clone, Debug)]
pub struct Filters {
    pub tenant_id: Uuid,
    pub application_id: Option<Uuid>,
    pub device_profile_id: Option<Uuid>,
    pub search: Option<String>,
    pub online_state: OnlineState,
    pub tags: HashMap<String, String>,
}

#[cfg(feature = "postgres")]
macro_rules! apply_online_state_filter {
    ($q:ident, $online_state:expr) => {
        match $online_state {
            OnlineState::All => {}
            OnlineState::Disabled => {
                $q = $q.filter(device::is_disabled.eq(true));
            }
            OnlineState::NeverSeen => {
                $q = $q.filter(
                    device::is_disabled
                        .eq(false)
                        .and(device::last_seen_at.is_null()),
                );
            }
            OnlineState::Online => {
                $q = $q.filter(dsl::sql::<Bool>(
                    "device.is_disabled = false and device.last_seen_at is not null and (now() - (make_interval(secs => device_profile.uplink_interval) * 1.5)) <= device.last_seen_at",
                ));
            }
            OnlineState::Offline => {
                $q = $q.filter(dsl::sql::<Bool>(
                    "device.is_disabled = false and device.last_seen_at is not null and (now() - (make_interval(secs => device_profile.uplink_interval) * 1.5)) > device.last_seen_at",
                ));
            }
        }
    };
}

#[cfg(feature = "sqlite")]
macro_rules! apply_online_state_filter {
    ($q:ident, $online_state:expr) => {
        match $online_state {
            OnlineState::All => {}
            OnlineState::Disabled => {
                $q = $q.filter(device::is_disabled.eq(true));
            }
            OnlineState::NeverSeen => {
                $q = $q.filter(
                    device::is_disabled
                        .eq(false)
                        .and(device::last_seen_at.is_null()),
                );
            }
            OnlineState::Online => {
                $q = $q.filter(dsl::sql::<Bool>(
                    "device.is_disabled = false and device.last_seen_at is not null and (unixepoch('now') - unixepoch(device.last_seen_at)) <= (device_profile.uplink_interval * 1.5)",
                ));
            }
            OnlineState::Offline => {
                $q = $q.filter(dsl::sql::<Bool>(
                    "device.is_disabled = false and device.last_seen_at is not null and (unixepoch('now') - unixepoch(device.last_seen_at)) > (device_profile.uplink_interval * 1.5)",
                ));
            }
        }
    };
}

#[derive(Queryable, Debug)]
pub struct TenantDeviceListItem {
    pub dev_eui: EUI64,
    pub name: String,
    pub description: String,
    pub application_id: fields::Uuid,
    pub application_name: String,
    pub device_profile_id: fields::Uuid,
    pub device_profile_name: String,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    pub last_seen_at: Option<DateTime<Utc>>,
    pub margin: Option<i32>,
    pub external_power_source: bool,
    pub battery_level: Option<fields::BigDecimal>,
    pub tags: fields::KeyValue,
    pub join_eui: EUI64,
    pub is_disabled: bool,
    pub class_enabled: DeviceClass,
    pub uplink_interval: i32,
}

pub async fn get_count(filters: &Filters) -> Result<i64, Error> {
    let mut q = device::dsl::device
        .inner_join(application::table.on(application::id.eq(device::application_id)))
        .inner_join(device_profile::table.on(device_profile::id.eq(device::device_profile_id)))
        .select(dsl::count_star())
        .into_boxed();

    q = q.filter(application::tenant_id.eq(fields::Uuid::from(filters.tenant_id)));

    if let Some(application_id) = &filters.application_id {
        q = q.filter(device::application_id.eq(fields::Uuid::from(application_id)));
    }

    if let Some(device_profile_id) = &filters.device_profile_id {
        q = q.filter(device::device_profile_id.eq(fields::Uuid::from(device_profile_id)));
    }

    if let Some(search) = &filters.search {
        #[cfg(feature = "postgres")]
        {
            let search = format!("%{}%", search);
            q = q.filter(
                device::name
                    .ilike(search.clone())
                    .or(application::name.ilike(search.clone()))
                    .or(device_profile::name.ilike(search)),
            );
        }
        #[cfg(feature = "sqlite")]
        {
            let search = format!("%{}%", search);
            q = q.filter(
                device::name
                    .like(search.clone())
                    .or(application::name.like(search.clone()))
                    .or(device_profile::name.like(search)),
            );
        }
    }

    apply_online_state_filter!(q, &filters.online_state);

    if !filters.tags.is_empty() {
        #[cfg(feature = "postgres")]
        {
            q = q.filter(device::tags.contains(serde_json::json!(&filters.tags)));
        }
        #[cfg(feature = "sqlite")]
        {
            for (k, v) in filters.tags.iter() {
                q = q.filter(
                    dsl::sql::<Bool>(&format!("device.tags->>'{}' =", k))
                        .bind::<diesel::sql_types::Text, _>(v),
                );
            }
        }
    }

    q.first(&mut get_async_db_conn().await?)
        .await
        .map_err(|e| Error::from_diesel(e, "".into()))
}

pub async fn list(
    limit: i64,
    offset: i64,
    filters: &Filters,
    order_by: OrderBy,
    order_by_desc: bool,
) -> Result<Vec<TenantDeviceListItem>, Error> {
    let mut q = device::dsl::device
        .inner_join(application::table.on(application::id.eq(device::application_id)))
        .inner_join(device_profile::table.on(device_profile::id.eq(device::device_profile_id)))
        .select((
            device::dev_eui,
            device::name,
            device::description,
            application::id,
            application::name,
            device_profile::id,
            device_profile::name,
            device::created_at,
            device::updated_at,
            device::last_seen_at,
            device::margin,
            device::external_power_source,
            device::battery_level,
            device::tags,
            device::join_eui,
            device::is_disabled,
            device::enabled_class,
            device_profile::uplink_interval,
        ))
        .into_boxed();

    q = q.filter(application::tenant_id.eq(fields::Uuid::from(filters.tenant_id)));

    if let Some(application_id) = &filters.application_id {
        q = q.filter(device::application_id.eq(fields::Uuid::from(application_id)));
    }

    if let Some(device_profile_id) = &filters.device_profile_id {
        q = q.filter(device::device_profile_id.eq(fields::Uuid::from(device_profile_id)));
    }

    if let Some(search) = &filters.search {
        #[cfg(feature = "postgres")]
        {
            let search = format!("%{}%", search);
            q = q.filter(
                device::name
                    .ilike(search.clone())
                    .or(application::name.ilike(search.clone()))
                    .or(device_profile::name.ilike(search)),
            );
        }
        #[cfg(feature = "sqlite")]
        {
            let search = format!("%{}%", search);
            q = q.filter(
                device::name
                    .like(search.clone())
                    .or(application::name.like(search.clone()))
                    .or(device_profile::name.like(search)),
            );
        }
    }

    apply_online_state_filter!(q, &filters.online_state);

    if !filters.tags.is_empty() {
        #[cfg(feature = "postgres")]
        {
            q = q.filter(device::tags.contains(serde_json::json!(&filters.tags)));
        }
        #[cfg(feature = "sqlite")]
        {
            for (k, v) in filters.tags.iter() {
                q = q.filter(
                    dsl::sql::<Bool>(&format!("device.tags->>'{}' =", k))
                        .bind::<diesel::sql_types::Text, _>(v),
                );
            }
        }
    }

    q = match order_by_desc {
        true => match order_by {
            OrderBy::Name => q.order_by(device::name.desc()),
            OrderBy::DevEui => q.order_by(device::dev_eui.desc()),
            OrderBy::LastSeenAt => {
                #[cfg(feature = "postgres")]
                {
                    q.order_by(device::last_seen_at.desc().nulls_last())
                        .then_order_by(device::name)
                }

                #[cfg(feature = "sqlite")]
                {
                    q.order_by(device::last_seen_at.desc())
                        .then_order_by(device::name)
                }
            }
            OrderBy::DeviceProfileName => q.order_by(device_profile::name.desc()),
            OrderBy::ApplicationName => q.order_by(application::name.desc()),
            OrderBy::CreatedAt => q.order_by(device::created_at.desc()),
            OrderBy::UpdatedAt => q.order_by(device::updated_at.desc()),
        },
        false => match order_by {
            OrderBy::Name => q.order_by(device::name),
            OrderBy::DevEui => q.order_by(device::dev_eui),
            OrderBy::LastSeenAt => {
                #[cfg(feature = "postgres")]
                {
                    q.order_by(device::last_seen_at.asc().nulls_first())
                        .then_order_by(device::name)
                }

                #[cfg(feature = "sqlite")]
                {
                    q.order_by(device::last_seen_at.asc())
                        .then_order_by(device::name)
                }
            }
            OrderBy::DeviceProfileName => q.order_by(device_profile::name),
            OrderBy::ApplicationName => q.order_by(application::name),
            OrderBy::CreatedAt => q.order_by(device::created_at),
            OrderBy::UpdatedAt => q.order_by(device::updated_at),
        },
    };

    q.limit(limit)
        .offset(offset)
        .load(&mut get_async_db_conn().await?)
        .await
        .map_err(|e| Error::from_diesel(e, "".into()))
}

pub fn is_active(item: &TenantDeviceListItem) -> bool {
    if item.is_disabled {
        return false;
    }

    match item.last_seen_at {
        Some(last_seen_at) => {
            let max_silence = Duration::seconds(item.uplink_interval as i64) * 3 / 2;
            Utc::now() - max_silence <= last_seen_at
        }
        None => false,
    }
}
