use std::str::FromStr;

use async_trait::async_trait;
use bigdecimal::ToPrimitive;
use chirpstack_api::api::alinkwise_service_server::AlinkwiseService;
use chirpstack_api::{api, tonic};
use tonic::{Request, Response, Status};
use uuid::Uuid;

use crate::alinkwise::{device_query, uplink_history};
use crate::api::auth::validator;
use crate::api::error::ToStatus;
use crate::api::helpers::{ToProto, datetime_to_prost_timestamp};
use crate::storage::{get_async_redis_conn, redis_key};
use lrwn::EUI64;

pub struct Alinkwise {
    validator: validator::RequestValidator,
}

impl Alinkwise {
    pub fn new(validator: validator::RequestValidator) -> Self {
        Alinkwise { validator }
    }
}

#[async_trait]
impl AlinkwiseService for Alinkwise {
    async fn clear_gateway_frame_log(
        &self,
        request: Request<api::ClearGatewayFrameLogRequest>,
    ) -> Result<Response<api::ClearGatewayFrameLogResponse>, Status> {
        let req = request.get_ref();
        let gateway_id = EUI64::from_str(&req.gateway_id).map_err(|e| e.status())?;

        self.validator
            .validate(
                request.extensions(),
                validator::ValidateGatewayAccess::new(validator::Flag::Delete, gateway_id),
            )
            .await?;

        let key = redis_key(format!("gw:{{{}}}:stream:frame", req.gateway_id));
        let mut redis_conn = get_async_redis_conn()
            .await
            .map_err(|e| Status::internal(format!("{:#}", e)))?;

        () = redis::cmd("DEL")
            .arg(key)
            .query_async(&mut redis_conn)
            .await
            .map_err(|e| Status::internal(format!("{:#}", e)))?;

        Ok(Response::new(api::ClearGatewayFrameLogResponse {}))
    }

    async fn clear_device_frame_log(
        &self,
        request: Request<api::ClearDeviceFrameLogRequest>,
    ) -> Result<Response<api::ClearDeviceFrameLogResponse>, Status> {
        let req = request.get_ref();
        let dev_eui = EUI64::from_str(&req.dev_eui).map_err(|e| e.status())?;

        self.validator
            .validate(
                request.extensions(),
                validator::ValidateDeviceAccess::new(validator::Flag::Delete, dev_eui),
            )
            .await?;

        let key = redis_key(format!("device:{{{}}}:stream:frame", dev_eui));
        let mut redis_conn = get_async_redis_conn()
            .await
            .map_err(|e| Status::internal(format!("{:#}", e)))?;

        () = redis::cmd("DEL")
            .arg(key)
            .query_async(&mut redis_conn)
            .await
            .map_err(|e| Status::internal(format!("{:#}", e)))?;

        Ok(Response::new(api::ClearDeviceFrameLogResponse {}))
    }

    async fn clear_device_metrics(
        &self,
        request: Request<api::ClearDeviceMetricsRequest>,
    ) -> Result<Response<api::ClearDeviceMetricsResponse>, Status> {
        let req = request.get_ref();
        let dev_eui = EUI64::from_str(&req.dev_eui).map_err(|e| e.status())?;

        self.validator
            .validate(
                request.extensions(),
                validator::ValidateDeviceAccess::new(validator::Flag::Delete, dev_eui),
            )
            .await?;

        let pattern = redis_key(format!("metrics:{{device:{}}}*", dev_eui));
        delete_redis_keys_by_pattern(pattern).await?;

        Ok(Response::new(api::ClearDeviceMetricsResponse {}))
    }

    async fn clear_device_uplink_history(
        &self,
        request: Request<api::ClearDeviceUplinkHistoryRequest>,
    ) -> Result<Response<api::ClearDeviceUplinkHistoryResponse>, Status> {
        let req = request.get_ref();
        let dev_eui = EUI64::from_str(&req.dev_eui).map_err(|e| e.status())?;

        self.validator
            .validate(
                request.extensions(),
                validator::ValidateDeviceAccess::new(validator::Flag::Delete, dev_eui),
            )
            .await?;

        let deleted_count = uplink_history::delete_for_device(&dev_eui.to_string())
            .await
            .map_err(|e| Status::internal(format!("{:#}", e)))?;

        Ok(Response::new(api::ClearDeviceUplinkHistoryResponse {
            deleted_count: deleted_count as u32,
        }))
    }

    async fn list_tenant_devices(
        &self,
        request: Request<api::ListTenantDevicesRequest>,
    ) -> Result<Response<api::ListTenantDevicesResponse>, Status> {
        let req = request.get_ref();
        let tenant_id = Uuid::from_str(&req.tenant_id).map_err(|e| e.status())?;
        let application_id = optional_uuid(&req.application_id)?;
        let device_profile_id = optional_uuid(&req.device_profile_id)?;

        self.validator
            .validate(
                request.extensions(),
                validator::ValidateApplicationsAccess::new(validator::Flag::List, tenant_id),
            )
            .await?;

        let filters = device_query::Filters {
            tenant_id,
            application_id,
            device_profile_id,
            search: if req.search.is_empty() {
                None
            } else {
                Some(req.search.clone())
            },
            online_state: req.online_state().into(),
            tags: req.tags.clone(),
        };

        let count = device_query::get_count(&filters)
            .await
            .map_err(|e| e.status())?;
        let items = device_query::list(
            req.limit as i64,
            req.offset as i64,
            &filters,
            req.order_by().into(),
            req.order_by_desc,
        )
        .await
        .map_err(|e| e.status())?;

        let mut resp = Response::new(api::ListTenantDevicesResponse {
            total_count: count as u32,
            result: items.iter().map(to_tenant_device_list_item).collect(),
        });
        resp.metadata_mut()
            .insert("x-log-tenant_id", req.tenant_id.parse().unwrap());

        Ok(resp)
    }

    async fn list_device_uplink_history(
        &self,
        request: Request<api::ListDeviceUplinkHistoryRequest>,
    ) -> Result<Response<api::ListDeviceUplinkHistoryResponse>, Status> {
        let req = request.get_ref();
        let dev_eui = EUI64::from_str(&req.dev_eui).map_err(|e| e.status())?;

        self.validator
            .validate(
                request.extensions(),
                validator::ValidateDeviceAccess::new(validator::Flag::Read, dev_eui),
            )
            .await?;

        let filters = uplink_history::Filters {
            dev_eui: dev_eui.to_string(),
            event_type: req.event_type.clone(),
            search: req.search.clone(),
            f_port: if req.has_f_port {
                Some(req.f_port)
            } else {
                None
            },
        };
        let count = uplink_history::get_count(&filters)
            .await
            .map_err(|e| Status::internal(format!("{:#}", e)))?;
        let items = uplink_history::list(req.limit as i64, req.offset as i64, &filters)
            .await
            .map_err(|e| Status::internal(format!("{:#}", e)))?;

        Ok(Response::new(api::ListDeviceUplinkHistoryResponse {
            total_count: count as u32,
            result: items.iter().map(to_device_uplink_history_item).collect(),
        }))
    }
}

async fn delete_redis_keys_by_pattern(pattern: String) -> Result<(), Status> {
    let mut redis_conn = get_async_redis_conn()
        .await
        .map_err(|e| Status::internal(format!("{:#}", e)))?;
    let mut cursor = 0_u64;

    loop {
        let (next_cursor, keys): (u64, Vec<String>) = redis::cmd("SCAN")
            .arg(cursor)
            .arg("MATCH")
            .arg(&pattern)
            .arg("COUNT")
            .arg(100_u32)
            .query_async(&mut redis_conn)
            .await
            .map_err(|e| Status::internal(format!("{:#}", e)))?;

        if !keys.is_empty() {
            let _: usize = redis::cmd("DEL")
                .arg(keys)
                .query_async(&mut redis_conn)
                .await
                .map_err(|e| Status::internal(format!("{:#}", e)))?;
        }

        if next_cursor == 0 {
            break;
        }
        cursor = next_cursor;
    }

    Ok(())
}

fn optional_uuid(value: &str) -> Result<Option<Uuid>, Status> {
    if value.is_empty() {
        return Ok(None);
    }

    Ok(Some(Uuid::from_str(value).map_err(|e| e.status())?))
}

fn to_device_uplink_history_item(
    item: &uplink_history::DeviceUplinkHistoryItem,
) -> api::DeviceUplinkHistoryItem {
    api::DeviceUplinkHistoryItem {
        deduplication_id: item.deduplication_id.clone(),
        event_type: item.event_type.clone(),
        time: item.time.clone(),
        dev_addr: item.dev_addr.clone(),
        dr: item.dr,
        f_cnt: item.f_cnt.clone(),
        f_port: item.f_port,
        confirmed: item.confirmed,
        data_hex: item.data_hex.clone(),
        object_json: item.object_json.clone(),
        rx_info_json: item.rx_info_json.clone(),
        tx_info_json: item.tx_info_json.clone(),
        tags_json: item.tags_json.clone(),
        summary_json: item.summary_json.clone(),
    }
}

fn to_tenant_device_list_item(
    item: &device_query::TenantDeviceListItem,
) -> api::TenantDeviceListItem {
    api::TenantDeviceListItem {
        dev_eui: item.dev_eui.to_string(),
        name: item.name.clone(),
        description: item.description.clone(),
        application_id: item.application_id.to_string(),
        application_name: item.application_name.clone(),
        device_profile_id: item.device_profile_id.to_string(),
        device_profile_name: item.device_profile_name.clone(),
        created_at: Some(datetime_to_prost_timestamp(&item.created_at)),
        updated_at: Some(datetime_to_prost_timestamp(&item.updated_at)),
        last_seen_at: item.last_seen_at.as_ref().map(datetime_to_prost_timestamp),
        device_status: match item.margin {
            Some(margin) => Some(api::DeviceStatus {
                margin,
                external_power_source: item.external_power_source,
                battery_level: match &item.battery_level {
                    Some(v) => v.to_f32().unwrap_or(-1.0),
                    None => -1.0,
                },
            }),
            None => None,
        },
        tags: item.tags.clone().into_hashmap(),
        join_eui: item.join_eui.to_string(),
        is_disabled: item.is_disabled,
        is_active: device_query::is_active(item),
        class_enabled: item.class_enabled.to_proto() as i32,
    }
}

impl From<api::list_tenant_devices_request::OnlineState> for device_query::OnlineState {
    fn from(value: api::list_tenant_devices_request::OnlineState) -> Self {
        match value {
            api::list_tenant_devices_request::OnlineState::All => device_query::OnlineState::All,
            api::list_tenant_devices_request::OnlineState::Online => {
                device_query::OnlineState::Online
            }
            api::list_tenant_devices_request::OnlineState::Offline => {
                device_query::OnlineState::Offline
            }
            api::list_tenant_devices_request::OnlineState::NeverSeen => {
                device_query::OnlineState::NeverSeen
            }
            api::list_tenant_devices_request::OnlineState::Disabled => {
                device_query::OnlineState::Disabled
            }
        }
    }
}

impl From<api::list_tenant_devices_request::OrderBy> for device_query::OrderBy {
    fn from(value: api::list_tenant_devices_request::OrderBy) -> Self {
        match value {
            api::list_tenant_devices_request::OrderBy::Name => device_query::OrderBy::Name,
            api::list_tenant_devices_request::OrderBy::DevEui => device_query::OrderBy::DevEui,
            api::list_tenant_devices_request::OrderBy::LastSeenAt => {
                device_query::OrderBy::LastSeenAt
            }
            api::list_tenant_devices_request::OrderBy::DeviceProfileName => {
                device_query::OrderBy::DeviceProfileName
            }
            api::list_tenant_devices_request::OrderBy::ApplicationName => {
                device_query::OrderBy::ApplicationName
            }
            api::list_tenant_devices_request::OrderBy::CreatedAt => {
                device_query::OrderBy::CreatedAt
            }
            api::list_tenant_devices_request::OrderBy::UpdatedAt => {
                device_query::OrderBy::UpdatedAt
            }
        }
    }
}
