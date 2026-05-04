use anyhow::{Context, Result};
use tokio_postgres::{NoTls, Row};

use crate::config;

const HISTORY_TABLES: &[&str] = &[
    "event_up",
    "event_join",
    "event_ack",
    "event_tx_ack",
    "event_log",
    "event_status",
    "event_location",
    "event_integration",
];

#[derive(Debug, Clone)]
pub struct Filters {
    pub dev_eui: String,
    pub event_type: String,
    pub search: String,
    pub f_port: Option<u32>,
}

#[derive(Debug, Clone)]
pub struct DeviceUplinkHistoryItem {
    pub deduplication_id: String,
    pub event_type: String,
    pub time: String,
    pub dev_addr: String,
    pub dr: u32,
    pub f_cnt: String,
    pub f_port: u32,
    pub confirmed: bool,
    pub data_hex: String,
    pub object_json: String,
    pub rx_info_json: String,
    pub tx_info_json: String,
    pub tags_json: String,
    pub summary_json: String,
}

pub async fn get_count(filters: &Filters) -> Result<i64> {
    let (client, connection) =
        tokio_postgres::connect(&config::get().integration.postgresql.dsn, NoTls)
            .await
            .context("Connect to integration PostgreSQL")?;
    tokio::spawn(async move {
        let _ = connection.await;
    });
    let search = search_pattern(&filters.search);
    let f_port = filters.f_port.map(|v| v as i32).unwrap_or(-1);
    let sql = format!(
        r#"
        select count(*)::bigint
        from ({}) as event
        where (
            $2 = ''
            or event_type ilike $2
            or search_text ilike $2
        )
          and ($3 < 0 or f_port = $3)
          and ($4 = '' or event_type = $4)
        "#,
        history_union_sql()
    );
    let row = client
        .query_one(
            &sql,
            &[&filters.dev_eui, &search, &f_port, &filters.event_type],
        )
        .await
        .context("Count device uplink history")?;

    Ok(row.get(0))
}

pub async fn list(
    limit: i64,
    offset: i64,
    filters: &Filters,
) -> Result<Vec<DeviceUplinkHistoryItem>> {
    let (client, connection) =
        tokio_postgres::connect(&config::get().integration.postgresql.dsn, NoTls)
            .await
            .context("Connect to integration PostgreSQL")?;
    tokio::spawn(async move {
        let _ = connection.await;
    });
    let search = search_pattern(&filters.search);
    let f_port = filters.f_port.map(|v| v as i32).unwrap_or(-1);
    let limit = limit.clamp(1, 100);
    let offset = offset.max(0);
    let sql = format!(
        r#"
        select
            deduplication_id,
            event_type,
            to_char(time at time zone 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"'),
            dev_addr,
            dr,
            f_cnt,
            f_port,
            confirmed,
            data_hex,
            object_json,
            rx_info_json,
            tx_info_json,
            tags_json,
            summary_json
        from ({}) as event
        where (
            $2 = ''
            or event_type ilike $2
            or search_text ilike $2
        )
          and ($3 < 0 or f_port = $3)
          and ($4 = '' or event_type = $4)
        order by time desc
        limit $5 offset $6
        "#,
        history_union_sql()
    );
    let rows = client
        .query(
            &sql,
            &[
                &filters.dev_eui,
                &search,
                &f_port,
                &filters.event_type,
                &limit,
                &offset,
            ],
        )
        .await
        .context("List device uplink history")?;

    Ok(rows.iter().map(row_to_item).collect())
}

pub async fn delete_for_device(dev_eui: &str) -> Result<u64> {
    let (client, connection) =
        tokio_postgres::connect(&config::get().integration.postgresql.dsn, NoTls)
            .await
            .context("Connect to integration PostgreSQL")?;
    tokio::spawn(async move {
        let _ = connection.await;
    });

    let mut deleted = 0;
    for table in HISTORY_TABLES {
        let sql = format!(
            "delete from {} where lower(dev_eui::text) = lower($1)",
            table
        );
        deleted += client
            .execute(&sql, &[&dev_eui])
            .await
            .with_context(|| format!("Delete device history from {}", table))?;
    }

    Ok(deleted)
}

pub async fn delete_expired(retention_days: u32) -> Result<u64> {
    if retention_days == 0 {
        return Ok(0);
    }

    let (client, connection) =
        tokio_postgres::connect(&config::get().integration.postgresql.dsn, NoTls)
            .await
            .context("Connect to integration PostgreSQL")?;
    tokio::spawn(async move {
        let _ = connection.await;
    });

    let retention_days = retention_days as i32;
    let mut deleted = 0;
    for table in HISTORY_TABLES {
        let sql = format!(
            "delete from {} where time < now() - make_interval(days => $1)",
            table
        );
        deleted += client
            .execute(&sql, &[&retention_days])
            .await
            .with_context(|| format!("Delete expired history from {}", table))?;
    }

    Ok(deleted)
}

fn row_to_item(row: &Row) -> DeviceUplinkHistoryItem {
    DeviceUplinkHistoryItem {
        deduplication_id: row.get(0),
        event_type: row.get(1),
        time: row.get(2),
        dev_addr: row.get(3),
        dr: row.get::<_, i32>(4).max(0) as u32,
        f_cnt: row.get(5),
        f_port: row.get::<_, i32>(6).max(0) as u32,
        confirmed: row.get(7),
        data_hex: row.get(8),
        object_json: row.get(9),
        rx_info_json: row.get(10),
        tx_info_json: row.get(11),
        tags_json: row.get(12),
        summary_json: row.get(13),
    }
}

fn search_pattern(search: &str) -> String {
    let search = search.trim();
    if search.is_empty() {
        String::new()
    } else {
        format!("%{}%", search)
    }
}

fn history_union_sql() -> &'static str {
    r#"
        select
            deduplication_id::text as deduplication_id,
            'up'::text as event_type,
            time as time,
            dev_addr::text as dev_addr,
            dr::int as dr,
            f_cnt::text as f_cnt,
            f_port::int as f_port,
            confirmed as confirmed,
            encode(data, 'hex') as data_hex,
            object::text as object_json,
            rx_info::text as rx_info_json,
            tx_info::text as tx_info_json,
            tags::text as tags_json,
            jsonb_build_object(
                'devAddr', dev_addr::text,
                'fCnt', f_cnt,
                'fPort', f_port,
                'dr', dr,
                'confirmed', confirmed,
                'dataHex', encode(data, 'hex')
            )::text as summary_json,
            concat_ws(' ', dev_addr::text, f_cnt::text, f_port::text, dr::text, encode(data, 'hex'), object::text, rx_info::text) as search_text
        from event_up
        where dev_eui = $1

        union all

        select
            deduplication_id::text,
            'join'::text,
            time,
            dev_addr::text,
            0,
            '',
            -1,
            false,
            '',
            '{}'::jsonb::text,
            '[]'::jsonb::text,
            '{}'::jsonb::text,
            tags::text,
            jsonb_build_object('devAddr', dev_addr::text)::text,
            concat_ws(' ', dev_addr::text, tags::text)
        from event_join
        where dev_eui = $1

        union all

        select
            queue_item_id::text,
            'ack'::text,
            time,
            '',
            0,
            f_cnt_down::text,
            -1,
            acknowledged,
            '',
            '{}'::jsonb::text,
            '[]'::jsonb::text,
            '{}'::jsonb::text,
            tags::text,
            jsonb_build_object('queueItemId', queue_item_id::text, 'deduplicationId', deduplication_id::text, 'acknowledged', acknowledged, 'fCntDown', f_cnt_down)::text,
            concat_ws(' ', queue_item_id::text, deduplication_id::text, acknowledged::text, f_cnt_down::text, tags::text)
        from event_ack
        where dev_eui = $1

        union all

        select
            queue_item_id::text,
            'txack'::text,
            time,
            '',
            0,
            f_cnt_down::text,
            -1,
            false,
            '',
            '{}'::jsonb::text,
            '[]'::jsonb::text,
            tx_info::text,
            tags::text,
            jsonb_build_object('queueItemId', queue_item_id::text, 'downlinkId', downlink_id, 'fCntDown', f_cnt_down, 'gatewayId', gateway_id::text)::text,
            concat_ws(' ', queue_item_id::text, downlink_id::text, f_cnt_down::text, gateway_id::text, tx_info::text, tags::text)
        from event_tx_ack
        where dev_eui = $1

        union all

        select
            id::text,
            'log'::text,
            time,
            '',
            0,
            '',
            -1,
            false,
            '',
            context::text,
            '[]'::jsonb::text,
            '{}'::jsonb::text,
            tags::text,
            jsonb_build_object('level', level, 'code', code, 'description', description)::text,
            concat_ws(' ', id::text, level, code, description, context::text, tags::text)
        from event_log
        where dev_eui = $1

        union all

        select
            deduplication_id::text,
            'status'::text,
            time,
            '',
            0,
            '',
            -1,
            false,
            '',
            jsonb_build_object(
                'margin', margin,
                'externalPowerSource', external_power_source,
                'batteryLevelUnavailable', battery_level_unavailable,
                'batteryLevel', battery_level
            )::text,
            '[]'::jsonb::text,
            '{}'::jsonb::text,
            tags::text,
            jsonb_build_object('margin', margin, 'batteryLevel', battery_level, 'externalPowerSource', external_power_source)::text,
            concat_ws(' ', margin::text, external_power_source::text, battery_level_unavailable::text, battery_level::text, tags::text)
        from event_status
        where dev_eui = $1

        union all

        select
            deduplication_id::text,
            'location'::text,
            time,
            '',
            0,
            '',
            -1,
            false,
            '',
            jsonb_build_object(
                'latitude', latitude,
                'longitude', longitude,
                'altitude', altitude,
                'source', source,
                'accuracy', accuracy
            )::text,
            '[]'::jsonb::text,
            '{}'::jsonb::text,
            tags::text,
            jsonb_build_object('latitude', latitude, 'longitude', longitude, 'altitude', altitude, 'source', source, 'accuracy', accuracy)::text,
            concat_ws(' ', latitude::text, longitude::text, altitude::text, source, accuracy::text, tags::text)
        from event_location
        where dev_eui = $1

        union all

        select
            deduplication_id::text,
            'integration'::text,
            time,
            '',
            0,
            '',
            -1,
            false,
            '',
            object::text,
            '[]'::jsonb::text,
            '{}'::jsonb::text,
            tags::text,
            jsonb_build_object(
                'integrationName', integration_name,
                'eventType', event_type,
                'object', object
            )::text,
            concat_ws(' ', integration_name, event_type, object::text, tags::text)
        from event_integration
        where dev_eui = $1
    "#
}
