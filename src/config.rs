use actix_web::web;

use std::convert::{TryFrom, TryInto};
use std::env::VarError;

use crate::error::ServiceError;
use envconfig::Envconfig;
use influxdb::{Client, Type};
use serde_json::Value;
use std::collections::HashMap;

// cfg.service(web::resource("/test")
//     .route(web::get().to(|| HttpResponse::Ok()))
//     .route(web::head().to(|| HttpResponse::MethodNotAllowed()))
// );
pub fn config(cfg: &mut web::ServiceConfig) {
    log::info!("Configuring service");

    match init() {
        Ok((processor, max_json_payload_size)) => {
            cfg.data(processor.clone())
                .data(web::JsonConfig::default().limit(max_json_payload_size));
        }
        Err(err) => {
            log::error!("Error configuring service {:}", err);
        }
    }
}

fn init() -> anyhow::Result<(Processor, usize)> {
    env_logger::init();

    let influx = InfluxDb::init_from_env()?;
    let client = Client::new(influx.uri, influx.db).with_auth(influx.user, influx.password);

    let config = Config::init_from_env()?;
    let max_json_payload_size = config.max_json_payload_size;

    let mut fields = HashMap::new();
    let mut tags = HashMap::new();

    for (key, value) in std::env::vars() {
        if let Some(field) = key.strip_prefix("FIELD_") {
            log::debug!("Adding field - {} -> {}", field, value);
            let compiled = jsonpath_lib::Compiled::compile(&value)
                .map_err(|err| anyhow::anyhow!("Failed to parse JSON path: {}", err))?;

            // find expected type for the field
            let expected_type = std::env::var(format!("TYPE_FIELD_{}", field)).try_into()?;
            fields.insert(
                field.to_lowercase(),
                Path {
                    path: value,
                    compiled,
                    r#type: expected_type,
                },
            );
        } else if let Some(tag) = key.strip_prefix("TAG_") {
            log::debug!("Adding tag - {} -> {}", tag, value);
            let compiled = jsonpath_lib::Compiled::compile(&value)
                .map_err(|err| anyhow::anyhow!("Failed to parse JSON path: {}", err))?;
            tags.insert(
                tag.to_lowercase(),
                Path {
                    path: value,
                    compiled,
                    r#type: ExpectedType::None,
                },
            );
        }
    }

    let processor = Processor {
        client,
        table: influx.table,
        fields,
        tags,
    };
    Ok((processor, max_json_payload_size))
}

#[derive(Envconfig, Clone, Debug)]
struct InfluxDb {
    #[envconfig(from = "INFLUXDB_URI")]
    pub uri: String,
    #[envconfig(from = "INFLUXDB_DATABASE")]
    pub db: String,
    #[envconfig(from = "INFLUXDB_USERNAME")]
    pub user: String,
    #[envconfig(from = "INFLUXDB_PASSWORD")]
    pub password: String,
    #[envconfig(from = "INFLUXDB_TABLE")]
    pub table: String,
}

#[derive(Envconfig, Clone, Debug)]
struct Config {
    #[envconfig(from = "MAX_JSON_PAYLOAD_SIZE", default = "65536")]
    pub max_json_payload_size: usize,
    #[envconfig(from = "BIND_ADDR", default = "127.0.0.1:8080")]
    pub bind_addr: String,
}

#[derive(Debug, Clone)]
pub struct Path {
    pub path: String,
    pub compiled: jsonpath_lib::Compiled,
    pub r#type: ExpectedType,
}

#[derive(Debug, Clone)]
pub enum ExpectedType {
    Boolean,
    Float,
    SignedInteger,
    UnsignedInteger,
    Text,
    None,
}

impl ExpectedType {
    fn accept(&self, value: Option<Type>) -> Result<Type, ServiceError> {
        value.ok_or_else(|| ServiceError::PayloadParseError {
            details: format!(""),
        })
    }

    pub fn convert(&self, value: &Value, path: &Path) -> Result<Type, ServiceError> {
        match self {
            ExpectedType::Boolean => self.accept(value.as_bool().map(Type::Boolean)),
            ExpectedType::Text => {
                self.accept(value.as_str().map(ToString::to_string).map(Type::Text))
            }
            ExpectedType::UnsignedInteger => self.accept(value.as_u64().map(Type::UnsignedInteger)),
            ExpectedType::SignedInteger => self.accept(value.as_i64().map(Type::SignedInteger)),
            ExpectedType::Float => self.accept(value.as_f64().map(Type::Float)),
            ExpectedType::None => match value {
                Value::String(s) => Ok(Type::Text(s.clone())),
                Value::Bool(b) => Ok(Type::Boolean(*b)),
                Value::Number(n) => n
                    .as_f64()
                    .map(Type::Float)
                    .or_else(|| n.as_i64().map(Type::SignedInteger))
                    .or_else(|| n.as_u64().map(Type::UnsignedInteger))
                    .ok_or_else(|| ServiceError::PayloadParseError {
                        details: format!(
                            "Unknown numeric type - path: {}, value: {:?}",
                            path.path, n
                        ),
                    }),
                _ => Err(ServiceError::PayloadParseError {
                    details: format!(
                        "Invalid value type selected - path: {}, value: {:?}",
                        path.path, value
                    ),
                }),
            },
        }
    }
}

impl TryFrom<String> for ExpectedType {
    type Error = anyhow::Error;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        match value.to_lowercase().as_str() {
            "bool" | "boolean" => Ok(ExpectedType::Boolean),
            "float" | "number" => Ok(ExpectedType::Float),
            "int" | "integer" => Ok(ExpectedType::SignedInteger),
            "uint" | "unsigned" => Ok(ExpectedType::UnsignedInteger),
            "string" | "text" => Ok(ExpectedType::Text),
            "" | "none" => Ok(ExpectedType::None),
            _ => anyhow::bail!("Unknown type: {}", value),
        }
    }
}

impl TryFrom<Result<String, VarError>> for ExpectedType {
    type Error = anyhow::Error;

    fn try_from(value: Result<String, VarError>) -> Result<Self, Self::Error> {
        value
            .map(Option::Some)
            .or_else(|err| match err {
                VarError::NotPresent => Ok(None),
                err => Err(err),
            })?
            .map_or_else(|| Ok(ExpectedType::None), TryInto::try_into)
    }
}

#[derive(Debug, Clone)]
pub struct Processor {
    pub client: Client,
    pub table: String,
    pub fields: HashMap<String, Path>,
    pub tags: HashMap<String, Path>,
}
