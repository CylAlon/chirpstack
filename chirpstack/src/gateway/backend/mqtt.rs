use std::collections::HashMap;
use std::sync::{LazyLock, RwLock};
use std::time::Duration;

use anyhow::Result;
use async_trait::async_trait;
use chrono::Utc;
use handlebars::Handlebars;
use prometheus_client::encoding::EncodeLabelSet;
use prometheus_client::metrics::counter::Counter;
use prometheus_client::metrics::family::Family;
use prost::Message;
use rand::RngExt;
use rumqttc::Transport;
use rumqttc::tokio_rustls::rustls;
use rumqttc::v5::mqttbytes::v5::{ConnectReturnCode, Publish};
use rumqttc::v5::{AsyncClient, Event, Incoming, MqttOptions, mqttbytes::QoS};
use serde::Serialize;
use tokio::sync::mpsc;
use tokio::time::sleep;
use tracing::{error, info, trace};

use super::GatewayBackend;
use crate::config::GatewayBackendMqtt;
use crate::helpers::tls22::{get_root_certs, load_cert, load_key};
use crate::monitoring::prometheus;
use crate::{downlink, uplink};
use lrwn::region::CommonName;

#[derive(Clone, Hash, PartialEq, Eq, EncodeLabelSet, Debug)]
struct EventLabels {
    event: String,
}

#[derive(Clone, Hash, PartialEq, Eq, EncodeLabelSet, Debug)]
struct CommandLabels {
    command: String,
}

static EVENT_COUNTER: LazyLock<Family<EventLabels, Counter>> = LazyLock::new(|| {
    let counter = Family::<EventLabels, Counter>::default();
    prometheus::register(
        "gateway_backend_mqtt_events",
        "Number of events received",
        counter.clone(),
    );
    counter
});
static COMMAND_COUNTER: LazyLock<Family<CommandLabels, Counter>> = LazyLock::new(|| {
    let counter = Family::<CommandLabels, Counter>::default();
    prometheus::register(
        "gateway_backend_mqtt_commands",
        "Number of commands sent",
        counter.clone(),
    );
    counter
});
static GATEWAY_JSON: LazyLock<RwLock<HashMap<String, bool>>> =
    LazyLock::new(|| RwLock::new(HashMap::new()));
static GATEWAY_V3_JSON: LazyLock<RwLock<HashMap<String, bool>>> =
    LazyLock::new(|| RwLock::new(HashMap::new()));
static V3_DOWNLINK_TOKEN_MAP: LazyLock<RwLock<HashMap<u32, u32>>> =
    LazyLock::new(|| RwLock::new(HashMap::new()));

pub struct MqttBackend<'a> {
    client: AsyncClient,
    templates: handlebars::Handlebars<'a>,
    qos: QoS,
    v4_migrate: bool,
    region_config_id: String,
}

#[derive(Serialize)]
struct CommandTopicContext {
    pub gateway_id: String,
    pub command: String,
}

impl<'a> MqttBackend<'a> {
    pub async fn new(
        region_config_id: &str,
        region_common_name: CommonName,
        conf: &GatewayBackendMqtt,
    ) -> Result<MqttBackend<'a>> {
        // topic templates
        let mut templates = Handlebars::new();
        templates.register_template_string(
            "command_topic",
            if conf.command_topic.is_empty() {
                let command_topic = "gateway/{{ gateway_id }}/command/{{ command }}".to_string();
                if conf.topic_prefix.is_empty() {
                    command_topic
                } else {
                    format!("{}/{}", conf.topic_prefix, command_topic)
                }
            } else {
                conf.command_topic.clone()
            },
        )?;

        // get client id, this will generate a random client_id when no client_id has been
        // configured.
        let client_id = if conf.client_id.is_empty() {
            let mut rnd = rand::rng();
            let client_id: u64 = rnd.random();
            format!("{:x}", client_id)
        } else {
            conf.client_id.clone()
        };

        // Get QoS
        let qos = match conf.qos {
            0 => QoS::AtMostOnce,
            1 => QoS::AtLeastOnce,
            2 => QoS::ExactlyOnce,
            _ => return Err(anyhow!("Invalid QoS: {}", conf.qos)),
        };

        // Create connect channel
        // We need to re-subscribe on (re)connect to be sure we have a subscription. Even
        // in case of a persistent MQTT session, there is no guarantee that the MQTT persisted the
        // session and that a re-connect would recover the subscription.
        let (connect_tx, mut connect_rx) = mpsc::channel(10);

        // Create client
        let mut mqtt_opts =
            MqttOptions::parse_url(format!("{}?client_id={}", conf.server, client_id))?;
        mqtt_opts.set_clean_start(conf.clean_session);
        mqtt_opts.set_keep_alive(conf.keep_alive_interval);
        if !conf.username.is_empty() || !conf.password.is_empty() {
            mqtt_opts.set_credentials(&conf.username, &conf.password);
        }

        if !conf.ca_cert.is_empty() || !conf.tls_cert.is_empty() || !conf.tls_key.is_empty() {
            info!(
                "Configuring client with TLS certificate, ca_cert: {}, tls_cert: {}, tls_key: {}",
                conf.ca_cert, conf.tls_cert, conf.tls_key
            );

            let root_certs = get_root_certs(if conf.ca_cert.is_empty() {
                None
            } else {
                Some(conf.ca_cert.clone())
            })?;

            let client_conf = if conf.tls_cert.is_empty() && conf.tls_key.is_empty() {
                rustls::ClientConfig::builder()
                    .with_root_certificates(root_certs.clone())
                    .with_no_client_auth()
            } else {
                rustls::ClientConfig::builder()
                    .with_root_certificates(root_certs.clone())
                    .with_client_auth_cert(
                        load_cert(&conf.tls_cert).await?,
                        load_key(&conf.tls_key).await?,
                    )?
            };

            mqtt_opts.set_transport(Transport::tls_with_config(client_conf.into()));
        }

        let (client, mut eventloop) = AsyncClient::new(mqtt_opts, conf.channel_capacity);

        let b = MqttBackend {
            client,
            qos,
            templates,
            v4_migrate: conf.v4_migrate,
            region_config_id: region_config_id.to_string(),
        };

        // connect
        info!(region_id = %region_config_id, server_uri = %conf.server, clean_session = conf.clean_session, client_id = %client_id, "Connecting to MQTT broker");

        // (Re)subscribe loop
        tokio::spawn({
            let client = b.client.clone();
            let qos = b.qos;
            let region_config_id = region_config_id.to_string();
            let event_topic = if conf.event_topic.is_empty() {
                let event_topic = "gateway/+/event/+".to_string();
                if conf.topic_prefix.is_empty() {
                    event_topic
                } else {
                    format!("{}/{}", conf.topic_prefix, event_topic)
                }
            } else {
                conf.event_topic.clone()
            };
            let share_name = conf.share_name.clone();

            async move {
                while let Some(shared_sub_support) = connect_rx.recv().await {
                    let event_topic = if shared_sub_support {
                        format!("$share/{}/{}", share_name, event_topic)
                    } else {
                        event_topic.clone()
                    };

                    info!(region_id = %region_config_id, event_topic = %event_topic, "Subscribing to gateway event topic");
                    if let Err(e) = client.subscribe(&event_topic, qos).await {
                        error!(region_id = %region_config_id, event_topic = %event_topic, error = %e, "MQTT subscribe error");
                    }
                }
            }
        });

        // Eventloop
        tokio::spawn({
            let region_config_id = region_config_id.to_string();
            let v4_migrate = conf.v4_migrate;

            async move {
                info!("Starting MQTT event loop");

                loop {
                    match eventloop.poll().await {
                        Ok(v) => {
                            trace!(event = ?v, "MQTT event");

                            match v {
                                Event::Incoming(Incoming::Publish(p)) => {
                                    message_callback(
                                        v4_migrate,
                                        &region_config_id,
                                        region_common_name,
                                        p,
                                    )
                                    .await
                                }
                                Event::Incoming(Incoming::ConnAck(v)) => {
                                    if v.code == ConnectReturnCode::Success {
                                        // Per specification:
                                        // A value of 1 means Shared Subscriptions are supported. If not present, then Shared Subscriptions are supported.
                                        let shared_sub_support = v
                                            .properties
                                            .map(|v| {
                                                v.shared_subscription_available
                                                    .map(|v| v == 1)
                                                    .unwrap_or(true)
                                            })
                                            .unwrap_or(true);

                                        if let Err(e) = connect_tx.try_send(shared_sub_support) {
                                            error!(error = %e, "Send to subscribe channel error");
                                        }
                                    } else {
                                        error!(code = ?v.code, "Connection error");
                                        sleep(Duration::from_secs(1)).await
                                    }
                                }
                                _ => {}
                            }
                        }
                        Err(e) => {
                            error!(error = %e, "MQTT error");
                            sleep(Duration::from_secs(1)).await
                        }
                    }
                }
            }
        });

        // return backend
        Ok(b)
    }

    fn get_command_topic(&self, gateway_id: &str, command: &str) -> Result<String> {
        Ok(self.templates.render(
            "command_topic",
            &CommandTopicContext {
                gateway_id: gateway_id.to_string(),
                command: command.to_string(),
            },
        )?)
    }
}

#[async_trait]
impl GatewayBackend for MqttBackend<'_> {
    async fn send_downlink(&self, df: &chirpstack_api::gw::DownlinkFrame) -> Result<()> {
        COMMAND_COUNTER
            .get_or_create(&CommandLabels {
                command: "down".to_string(),
            })
            .inc();
        let topic = self.get_command_topic(&df.gateway_id, "down")?;
        let mut df = df.clone();

        let json = gateway_is_json(&df.gateway_id);
        let v3_json = gateway_is_v3_json(&df.gateway_id);

        if self.v4_migrate && v3_json {
            df.v4_migrate();
        }

        let b = match json {
            true if v3_json => {
                register_v3_downlink_token(df.downlink_id, df.downlink_id);
                normalize_v3_downlink_json_payload(&serde_json::to_vec(&df)?)?
            }
            true => serde_json::to_vec(&df)?,
            false => df.encode_to_vec(),
        };

        info!(region_id = %self.region_config_id, gateway_id = %df.gateway_id, topic = %topic, json = json, "Sending downlink frame");
        self.client.publish(topic, self.qos, false, b).await?;
        trace!("Message published");

        Ok(())
    }

    async fn send_configuration(
        &self,
        gw_conf: &chirpstack_api::gw::GatewayConfiguration,
    ) -> Result<()> {
        COMMAND_COUNTER
            .get_or_create(&CommandLabels {
                command: "config".to_string(),
            })
            .inc();
        let topic = self.get_command_topic(&gw_conf.gateway_id, "config")?;
        let json = gateway_is_json(&gw_conf.gateway_id);
        let v3_json = gateway_is_v3_json(&gw_conf.gateway_id);
        let b = match json {
            true if v3_json => {
                normalize_v3_gateway_config_json_payload(&serde_json::to_vec(&gw_conf)?)?
            }
            true => serde_json::to_vec(&gw_conf)?,
            false => gw_conf.encode_to_vec(),
        };

        info!(region_id = %self.region_config_id, gateway_id = %gw_conf.gateway_id, topic = %topic, json = json, "Sending gateway configuration");
        self.client.publish(topic, self.qos, false, b).await?;
        trace!("Message published");

        Ok(())
    }
}

async fn message_callback(
    v4_migrate: bool,
    region_config_id: &str,
    region_common_name: CommonName,
    p: Publish,
) {
    let topic = String::from_utf8_lossy(&p.topic);

    let err = || -> Result<()> {
        let json = payload_is_json(&p.payload);
        let v3_json = payload_is_v3_json(&p.payload);

        info!(
            region_id = region_config_id,
            topic = %topic,
            qos = ?p.qos,
            json = json,
            "Message received from gateway"
        );

        if topic.ends_with("/up") {
            EVENT_COUNTER
                .get_or_create(&EventLabels {
                    event: "up".to_string(),
                })
                .inc();
            let mut event = match json {
                true if v3_json => {
                    serde_json::from_slice(&normalize_v3_json_payload("up", &p.payload)?)?
                }
                true => serde_json::from_slice(&p.payload)?,
                false => chirpstack_api::gw::UplinkFrame::decode(p.payload.as_ref())?,
            };

            if v4_migrate && v3_json {
                event.v4_migrate();
            }

            if let Some(rx_info) = &mut event.rx_info {
                set_gateway_json(&rx_info.gateway_id, json);
                set_gateway_v3_json(&rx_info.gateway_id, v3_json);
                rx_info.ns_time = Some(Utc::now().into());
            }

            tokio::spawn(uplink::deduplicate_uplink(
                region_common_name,
                region_config_id.to_string(),
                event,
            ));
        } else if topic.ends_with("/stats") {
            EVENT_COUNTER
                .get_or_create(&EventLabels {
                    event: "stats".to_string(),
                })
                .inc();
            let mut event = match json {
                true if v3_json => {
                    serde_json::from_slice(&normalize_v3_json_payload("stats", &p.payload)?)?
                }
                true => serde_json::from_slice(&p.payload)?,
                false => chirpstack_api::gw::GatewayStats::decode(p.payload.as_ref())?,
            };

            if v4_migrate && v3_json {
                event.v4_migrate();
            }

            event
                .metadata
                .insert("region_config_id".to_string(), region_config_id.to_string());
            event.metadata.insert(
                "region_common_name".to_string(),
                region_common_name.to_string(),
            );
            set_gateway_json(&event.gateway_id, json);
            set_gateway_v3_json(&event.gateway_id, v3_json);
            tokio::spawn(uplink::stats::Stats::handle(event));
        } else if topic.ends_with("/ack") {
            EVENT_COUNTER
                .get_or_create(&EventLabels {
                    event: "ack".to_string(),
                })
                .inc();
            let mut event = match json {
                true if v3_json => {
                    serde_json::from_slice(&normalize_v3_json_payload("ack", &p.payload)?)?
                }
                true => serde_json::from_slice(&p.payload)?,
                false => chirpstack_api::gw::DownlinkTxAck::decode(p.payload.as_ref())?,
            };

            if v4_migrate && v3_json {
                event.v4_migrate();
            }

            set_gateway_json(&event.gateway_id, json);
            set_gateway_v3_json(&event.gateway_id, v3_json);
            tokio::spawn(downlink::tx_ack::TxAck::handle(event));
        } else if topic.ends_with("/mesh") {
            EVENT_COUNTER
                .get_or_create(&EventLabels {
                    event: "mesh".to_string(),
                })
                .inc();
            let event = match json {
                true => serde_json::from_slice(&p.payload)?,
                false => chirpstack_api::gw::MeshEvent::decode(p.payload.as_ref())?,
            };

            tokio::spawn(uplink::mesh::Mesh::handle(event));
        } else {
            return Err(anyhow!("Unknown event type"));
        }

        Ok(())
    }()
    .err();

    if err.is_some() {
        error!(
            region_id = %region_config_id,
            topic = %topic,
            qos = ?p.qos,
            "Processing gateway event error: {}",
            err.as_ref().unwrap()
        );
    }
}

fn gateway_is_json(gateway_id: &str) -> bool {
    let gw_json_r = GATEWAY_JSON.read().unwrap();
    gw_json_r.get(gateway_id).cloned().unwrap_or(false)
}

fn gateway_is_v3_json(gateway_id: &str) -> bool {
    let gw_v3_json_r = GATEWAY_V3_JSON.read().unwrap();
    gw_v3_json_r.get(gateway_id).cloned().unwrap_or(false)
}

fn set_gateway_json(gateway_id: &str, is_json: bool) {
    let mut gw_json_w = GATEWAY_JSON.write().unwrap();
    gw_json_w.insert(gateway_id.to_string(), is_json);
}

fn set_gateway_v3_json(gateway_id: &str, is_v3_json: bool) {
    let mut gw_v3_json_w = GATEWAY_V3_JSON.write().unwrap();
    gw_v3_json_w.insert(gateway_id.to_string(), is_v3_json);
}

fn register_v3_downlink_token(token: u32, downlink_id: u32) {
    let mut token_map = V3_DOWNLINK_TOKEN_MAP.write().unwrap();
    token_map.insert(token & 0xffff, downlink_id);
}

fn get_v3_downlink_id_by_token(token: u32) -> Option<u32> {
    let mut token_map = V3_DOWNLINK_TOKEN_MAP.write().unwrap();
    token_map.remove(&(token & 0xffff))
}

fn payload_is_json(b: &[u8]) -> bool {
    let payload = String::from_utf8_lossy(b);
    payload.contains("gatewayId") || payload.contains("gatewayID")
}

fn payload_is_v3_json(b: &[u8]) -> bool {
    let payload = String::from_utf8_lossy(b);
    payload.contains("gatewayID")
        || payload.contains("\"txInfo\"")
        || payload.contains("\"rxInfo\"")
}

fn normalize_v3_json_payload(event_type: &str, b: &[u8]) -> Result<Vec<u8>> {
    let mut v: serde_json::Value = serde_json::from_slice(b)?;

    match event_type {
        "up" => {
            rename_field(&mut v, "txInfo", "txInfoLegacy");
            rename_field(&mut v, "rxInfo", "rxInfoLegacy");

            if let Some(tx_info) = v.get_mut("txInfoLegacy") {
                rename_field(tx_info, "loRaModulationInfo", "loraModulationInfo");

                if let Some(lora_info) = tx_info.get_mut("loraModulationInfo") {
                    rename_field(lora_info, "codeRate", "codeRateLegacy");
                }
            }

            if let Some(rx_info) = v.get_mut("rxInfoLegacy") {
                rename_field(rx_info, "gatewayID", "gatewayId");
                rename_field(rx_info, "loRaSNR", "loraSnr");
                rename_field(rx_info, "uplinkID", "uplinkId");
            }
        }
        "stats" => {
            rename_field(&mut v, "gatewayID", "gatewayIdLegacy");
            rename_field(&mut v, "rxPacketsReceivedOK", "rxPacketsReceivedOk");
        }
        "ack" => {
            rename_field(&mut v, "gatewayID", "gatewayIdLegacy");
            if let Some(token) = v.get("token").and_then(|v| v.as_u64())
                && let Some(downlink_id) = get_v3_downlink_id_by_token(token as u32)
                && let Some(obj) = v.as_object_mut()
            {
                obj.insert(
                    "downlinkId".to_string(),
                    serde_json::Value::Number(downlink_id.into()),
                );
                obj.insert(
                    "items".to_string(),
                    serde_json::Value::Array(vec![serde_json::json!({
                        "status": "OK"
                    })]),
                );
            }
        }
        _ => {}
    }

    Ok(serde_json::to_vec(&v)?)
}

fn normalize_v3_downlink_json_payload(b: &[u8]) -> Result<Vec<u8>> {
    let mut v: serde_json::Value = serde_json::from_slice(b)?;

    rename_field(&mut v, "gatewayIdLegacy", "gatewayID");
    let gateway_id = v.get("gatewayID").cloned();
    if let Some(gateway_id) = &gateway_id
        && let Some(obj) = v.as_object_mut()
    {
        obj.insert("gateway_id".to_string(), gateway_id.clone());
    }

    if let Some(downlink_id) = v.get("downlinkId").cloned()
        && let Some(obj) = v.as_object_mut()
    {
        obj.insert("token".to_string(), downlink_id);
    }

    if let Some(obj) = v.as_object_mut() {
        obj.remove("gatewayId");
        obj.remove("downlinkId");
        obj.remove("downlinkIdLegacy");
    }

    if let Some(first_item) = v
        .get("items")
        .and_then(|v| v.as_array())
        .and_then(|items| items.first())
        .cloned()
    {
        if let Some(phy_payload) = first_item.get("phyPayload").cloned()
            && let Some(obj) = v.as_object_mut()
        {
            obj.insert("phyPayload".to_string(), phy_payload);
        }

        if let Some(tx_info) = first_item.get("txInfoLegacy").cloned()
            && let Some(obj) = v.as_object_mut()
        {
            obj.insert("txInfo".to_string(), tx_info);
        }

        if let Some(obj) = v.as_object_mut() {
            obj.remove("items");
        }
    }

    if let Some(items) = v.get_mut("items").and_then(|v| v.as_array_mut()) {
        for item in items {
            if let Some(obj) = item.as_object_mut() {
                obj.remove("txInfo");
            }

            rename_field(item, "txInfoLegacy", "txInfo");

            if let Some(tx_info) = item.get_mut("txInfo") {
                if let Some(gateway_id) = &gateway_id
                    && let Some(obj) = tx_info.as_object_mut()
                {
                    obj.insert("gatewayID".to_string(), gateway_id.clone());
                    obj.insert("gateway_id".to_string(), gateway_id.clone());
                }

                let has_lora_modulation_info = tx_info.get("loraModulationInfo").is_some();
                if has_lora_modulation_info && let Some(obj) = tx_info.as_object_mut() {
                    obj.insert(
                        "modulation".to_string(),
                        serde_json::Value::String("LORA".to_string()),
                    );
                }

                if let Some(lora_info) = tx_info.get_mut("loraModulationInfo") {
                    rename_field(lora_info, "codeRateLegacy", "codeRate");
                }

                if tx_info.get("immediatelyTimingInfo").is_some()
                    && tx_info.get("timing").is_none()
                    && let Some(obj) = tx_info.as_object_mut()
                {
                    obj.insert(
                        "timing".to_string(),
                        serde_json::Value::String("IMMEDIATELY".to_string()),
                    );
                }
            }
        }
    }

    if let Some(tx_info) = v.get_mut("txInfo") {
        normalize_v3_downlink_tx_info(tx_info, &gateway_id);
    }

    Ok(serde_json::to_vec(&v)?)
}

fn normalize_v3_downlink_tx_info(
    tx_info: &mut serde_json::Value,
    gateway_id: &Option<serde_json::Value>,
) {
    if let Some(gateway_id) = gateway_id
        && let Some(obj) = tx_info.as_object_mut()
    {
        obj.insert("gatewayID".to_string(), gateway_id.clone());
        obj.insert("gateway_id".to_string(), gateway_id.clone());
    }

    rename_field(tx_info, "loraModulationInfo", "loRaModulationInfo");

    let has_lora_modulation_info = tx_info.get("loRaModulationInfo").is_some();
    if has_lora_modulation_info && let Some(obj) = tx_info.as_object_mut() {
        obj.insert(
            "modulation".to_string(),
            serde_json::Value::String("LORA".to_string()),
        );
    }

    if let Some(lora_info) = tx_info.get_mut("loRaModulationInfo") {
        rename_field(lora_info, "codeRateLegacy", "codeRate");
    }

    if tx_info.get("immediatelyTimingInfo").is_some()
        && tx_info.get("timing").is_none()
        && let Some(obj) = tx_info.as_object_mut()
    {
        obj.insert(
            "timing".to_string(),
            serde_json::Value::String("IMMEDIATELY".to_string()),
        );
    }
}

fn normalize_v3_gateway_config_json_payload(b: &[u8]) -> Result<Vec<u8>> {
    let mut v: serde_json::Value = serde_json::from_slice(b)?;

    rename_field(&mut v, "gatewayIdLegacy", "gatewayID");

    if let Some(obj) = v.as_object_mut() {
        obj.remove("gatewayId");
    }

    Ok(serde_json::to_vec(&v)?)
}

fn rename_field(v: &mut serde_json::Value, old: &str, new: &str) {
    if let Some(obj) = v.as_object_mut()
        && !obj.contains_key(new)
        && let Some(value) = obj.remove(old)
    {
        obj.insert(new.to_string(), value);
    }
}
