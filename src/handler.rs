use crate::config::{Processor, Path};
use crate::error::ServiceError;
use actix_web::{web, HttpResponse};
use cloudevents::{event::Data, Event};
use influxdb::{InfluxDbWriteable, Timestamp, Type, WriteQuery};
use std::collections::HashMap;
use serde_json::Value;
use chrono::Utc;
use cloudevents::AttributesReader;

// Implement your function's logic here
pub async fn handle(
    event: Event,
    processor: web::Data<Processor>,
) -> Result<HttpResponse, actix_web::Error> {
    log::debug!("Received Event: {:?}", event);

    let data: Option<&Data> = event.data();

    let timestamp = event.time().cloned().unwrap_or_else(Utc::now);
    let timestamp = Timestamp::from(timestamp);

    let query = timestamp.into_query(processor.table.clone());

    // process values with payload only

    let json = parse_payload(data)?;
    let (query, num) = add_values(query, &processor, &json)?;

    // create full events JSON for tags

    let event_json = serde_json::to_value(event)?;
    let (query, _) = add_tags(query, &processor, &event_json)?;

    // execute query

    if num > 0 {
        let result = processor.client.query(&query).await;

        // process result

        log::debug!("Result: {:?}", result);

        match result {
            Ok(_) => Ok(HttpResponse::Accepted().finish()),
            Err(e) => Ok(HttpResponse::InternalServerError().body(e.to_string())),
        }
    } else {
        Ok(HttpResponse::NoContent().finish())
    }
}

fn add_to_query<F>(
    mut query: WriteQuery,
    processor: &HashMap<String, Path>,
    json: &Value,
    f: F,
) -> Result<(WriteQuery, usize), ServiceError>
where
    F: Fn(WriteQuery, &String, Type) -> WriteQuery,
{
    let mut num = 0;

    let mut f = |query, field, value| {
        num += 1;
        f(query, field, value)
    };

    for (ref field, ref path) in processor {
        let sel = path
            .compiled
            .select(&json)
            .map_err(|err| ServiceError::SelectorError {
                details: err.to_string(),
            })?;

        query = match sel.as_slice() {
            // no value, don't add
            [] => Ok(query),
            // single value, process
            [v] => Ok(f(query, field, path.r#type.convert(v, path)?)),
            // multiple values, error
            [..] => Err(ServiceError::SelectorError {
                details: format!("Selector found more than one value: {}", sel.len()),
            }),
        }?;
    }

    Ok((query, num))
}

fn add_values(
    query: WriteQuery,
    processor: &Processor,
    json: &Value,
) -> Result<(WriteQuery, usize), ServiceError> {
    add_to_query(query, &processor.fields, json, |query, field, value| {
        query.add_field(field, value)
    })
}

fn add_tags(
    query: WriteQuery,
    processor: &Processor,
    json: &Value,
) -> Result<(WriteQuery, usize), ServiceError> {
    add_to_query(query, &processor.tags, json, |query, field, value| {
        query.add_tag(field, value)
    })
}

fn parse_payload(data: Option<&Data>) -> Result<Value, ServiceError> {
    match data {
        Some(Data::Json(value)) => Ok(value.clone()),
        Some(Data::String(s)) => {
            serde_json::from_str::<Value>(&s).map_err(|err| ServiceError::PayloadParseError {
                details: err.to_string(),
            })
        }

        Some(Data::Binary(b)) => {
            serde_json::from_slice::<Value>(&b).map_err(|err| ServiceError::PayloadParseError {
                details: err.to_string(),
            })
        }
        _ => Err(ServiceError::PayloadParseError {
            details: "Unknown event payload".to_string(),
        }),
    }
}
