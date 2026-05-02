use std::str::FromStr;

use async_trait::async_trait;
use bigdecimal::ToPrimitive;
use chirpstack_api::api::alinkwise_service_server::AlinkwiseService;
use chirpstack_api::{api, tonic};
use tonic::{Request, Response, Status};
use uuid::Uuid;

use crate::alinkwise::device_query;
use crate::api::auth::validator;
use crate::api::error::ToStatus;
use crate::api::helpers::{ToProto, datetime_to_prost_timestamp};

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

        let count = device_query::get_count(&filters).await.map_err(|e| e.status())?;
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
}

fn optional_uuid(value: &str) -> Result<Option<Uuid>, Status> {
    if value.is_empty() {
        return Ok(None);
    }

    Ok(Some(Uuid::from_str(value).map_err(|e| e.status())?))
}

fn to_tenant_device_list_item(item: &device_query::TenantDeviceListItem) -> api::TenantDeviceListItem {
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
        last_seen_at: item
            .last_seen_at
            .as_ref()
            .map(datetime_to_prost_timestamp),
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
