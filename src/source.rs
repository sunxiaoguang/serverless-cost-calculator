use std::cmp::{max, min};
use std::fs::File;
use std::io;
use std::io::{BufReader, Write};
use std::ops::Sub;

use crate::output::OutputFormat;
use anyhow::{anyhow, Result};
use chrono::{DateTime, Duration, Utc};
use regex::Regex;
use serde::{Deserialize, Serialize};
use sqlx::{FromRow, MySql, Pool};

const TARGET_REGION_SIZE: u64 = 256 * 1024 * 1024;
const MINUTES_PER_HOUR: u64 = 60;

#[derive(Debug, Default, Deserialize, Serialize)]
pub struct WorkloadSourceConfiguration {
    #[serde(default = "default_host")]
    pub host: String,
    #[serde(default = "default_port")]
    pub port: u16,
    #[serde(default = "default_user")]
    pub user: String,
    #[serde(default)]
    pub password: String,
    pub database: String,
}

fn default_host() -> String {
    "localhost".into()
}
fn default_port() -> u16 {
    3306
}

fn default_user() -> String {
    "root".into()
}

impl WorkloadSourceConfiguration {
    pub fn new(
        host: impl Into<String>,
        port: u16,
        user: impl Into<String>,
        password: impl Into<String>,
        database: impl Into<String>,
    ) -> Self {
        Self {
            host: host.into(),
            port,
            user: user.into(),
            password: password.into(),
            database: database.into(),
        }
    }

    pub fn load(file: String) -> Result<Vec<Self>> {
        let file = file.to_lowercase();
        let reader = BufReader::new(File::open(&file)?);
        if file.ends_with(".json") {
            Ok(serde_json::from_reader(reader)?)
        } else if file.ends_with(".yaml") || file.ends_with(".yml") {
            Ok(serde_yaml::from_reader(reader)?)
        } else {
            Err(anyhow!(
                "Unknown batch configuration file format. Only json and yaml are supported"
            ))
        }
    }
    fn connection_string(&self) -> String {
        format!(
            "mysql://{}:{}@{}:{}/{}",
            self.user, self.password, self.host, self.port, self.database
        )
    }
}

#[derive(Debug, Default, Serialize)]
pub struct RequestDescription {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub requests_per_hour: Option<u64>,
    pub bytes_per_hour: u64,
}

#[derive(Debug, Default, Serialize)]
pub struct StorageDescription {
    pub data_in_bytes: u64,
    pub index_in_bytes: u64,
}

#[derive(Default, Debug, Serialize)]
pub struct WorkloadDescription {
    pub read: RequestDescription,
    pub write: RequestDescription,
    pub egress: RequestDescription,
    pub storage: StorageDescription,
}

impl WorkloadDescription {
    fn check_summary_duration(output: OutputFormat, duration_in_minutes: u64) {
        if duration_in_minutes < MINUTES_PER_HOUR {
            output.error(&format!("The statement summary, covering only {} minute(s), is less than an hour's workload. It is highly recommended to collect at least a day's worth of data before running the estimation to prevent distortion.", duration_in_minutes));
        } else if duration_in_minutes < MINUTES_PER_HOUR * 24 {
            output.warn(&format!("The statement summary, covering only {} hour(s), is less than a full day's workload and may not reflect the full business. Consider running the tool after collecting data for a longer period to ensure accuracy.", duration_in_minutes / MINUTES_PER_HOUR));
        }
    }
    fn mysql(
        output: OutputFormat,
        tables: TablesInformation,
        summary: MySQLStatementsSummary,
    ) -> Self {
        let duration_in_minutes =
            max(summary.end_time.sub(summary.start_time).num_minutes(), 1) as u64;
        Self::check_summary_duration(output, duration_in_minutes);
        let total_storage_in_bytes = max(
            tables.total_index_in_bytes.unwrap_or(0) + tables.total_data_in_bytes.unwrap_or(0),
            1,
        );
        let average_row_size_in_bytes =
            total_storage_in_bytes / max(tables.total_rows.unwrap_or(0), 1);
        let estimated_number_of_regions = total_storage_in_bytes / TARGET_REGION_SIZE;

        let read_bytes_per_hour =
            MINUTES_PER_HOUR * average_row_size_in_bytes * summary.read_rows / duration_in_minutes;
        let read_queries_per_hour = max(
            MINUTES_PER_HOUR * summary.read_queries / duration_in_minutes,
            1,
        );
        let read_bytes_per_request = read_bytes_per_hour / read_queries_per_hour;
        let read_regions_per_query = max(
            read_bytes_per_request * estimated_number_of_regions / total_storage_in_bytes,
            1,
        );

        let write_bytes_per_hour =
            MINUTES_PER_HOUR * average_row_size_in_bytes * summary.write_rows / duration_in_minutes;
        let write_queries_per_hour = max(
            MINUTES_PER_HOUR * summary.write_queries / duration_in_minutes,
            1,
        );
        let write_bytes_per_query = write_bytes_per_hour / write_queries_per_hour;
        let write_regions_per_query = max(
            write_bytes_per_query * estimated_number_of_regions / total_storage_in_bytes,
            1,
        );

        WorkloadDescription {
            read: RequestDescription {
                requests_per_hour: (read_queries_per_hour * read_regions_per_query).into(),
                bytes_per_hour: read_bytes_per_hour,
            },
            write: RequestDescription {
                requests_per_hour: (write_queries_per_hour * write_regions_per_query).into(),
                bytes_per_hour: write_bytes_per_hour,
            },
            egress: RequestDescription {
                bytes_per_hour: MINUTES_PER_HOUR * average_row_size_in_bytes * summary.sent_rows
                    / duration_in_minutes,
                ..Default::default()
            },
            storage: StorageDescription {
                data_in_bytes: tables.total_data_in_bytes.unwrap_or(0),
                index_in_bytes: tables.total_index_in_bytes.unwrap_or(0),
            },
        }
    }

    fn tidb(
        output: OutputFormat,
        tables: TablesInformation,
        summary: Option<TiDBStatementsSummary>,
        metrics: TiDBSystemMetrics,
    ) -> Self {
        let (write_bytes_per_hour, sent_bytes_per_hour) = match summary {
            Some(summary) => {
                let duration_in_minutes =
                    max(summary.end_time.sub(summary.start_time).num_minutes(), 1) as u64;
                Self::check_summary_duration(output, duration_in_minutes);
                let average_row_size_in_bytes = (tables.total_index_in_bytes.unwrap_or(0)
                    + tables.total_data_in_bytes.unwrap_or(0))
                    / max(1, tables.total_rows.unwrap_or(0));
                (
                    MINUTES_PER_HOUR * summary.write_bytes / duration_in_minutes,
                    MINUTES_PER_HOUR * summary.sent_rows * average_row_size_in_bytes
                        / duration_in_minutes,
                )
            }
            None => {
                output.warn("The 'Statement Summary Tables' are disabled; when they are available, estimations can be more accurate.");
                output.warn("For detailed instruction, visit https://docs.pingcap.com/tidb/stable/statement-summary-tables#parameter-configuration");
                (metrics.write_bytes_per_hour, 0)
            }
        };
        WorkloadDescription {
            read: RequestDescription {
                requests_per_hour: metrics.read_requests_per_hour.into(),
                bytes_per_hour: metrics.read_bytes_per_hour,
            },
            write: RequestDescription {
                requests_per_hour: metrics.write_requests_per_hour.into(),
                bytes_per_hour: write_bytes_per_hour,
            },
            egress: RequestDescription {
                bytes_per_hour: sent_bytes_per_hour,
                ..Default::default()
            },
            storage: StorageDescription {
                data_in_bytes: tables.total_data_in_bytes.unwrap_or(0),
                index_in_bytes: tables.total_index_in_bytes.unwrap_or(0),
            },
        }
    }
}

async fn run_analyze(output: OutputFormat, pool: &Pool<MySql>) -> Result<()> {
    let tables: Vec<String> = sqlx::query_as("SHOW FULL TABLES WHERE Table_type = 'BASE TABLE'")
        .fetch_all(pool)
        .await?
        .into_iter()
        .map(|v: (String, String)| v.0)
        .collect();
    for table in tables {
        output.warn(&format!("Analyzing table `{}`. Press CTRL+C to terminate if you notice unexpected performance impacts on the production system.", table));
        sqlx::query(&format!("ANALYZE TABLE `{}`", table))
            .execute(pool)
            .await?;
    }
    Ok(())
}

async fn confirm_and_run_analyze(output: OutputFormat, pool: &Pool<MySql>) -> Result<()> {
    loop {
        output.warn("Running ANALYZE on the production system may affect ongoing queries. Do you want to proceed? (yes/no): ");
        io::stdout().flush().unwrap_or(());
        let mut confirmation = String::new();
        io::stdin().read_line(&mut confirmation)?;
        match confirmation.trim().to_lowercase().as_str() {
            "yes" => break,
            "no" => return Ok(()),
            _ => continue,
        }
    }
    run_analyze(output, pool).await
}

pub async fn load_workload_description(
    output: OutputFormat,
    config: WorkloadSourceConfiguration,
    analyze_before_start: bool,
) -> Result<Option<WorkloadDescription>> {
    let pool = sqlx::MySqlPool::connect(&config.connection_string()).await?;

    if analyze_before_start {
        confirm_and_run_analyze(output, &pool).await?
    }

    let tables = read_tables_information(&pool, &config.database).await?;
    if is_tidb(&pool).await? {
        if is_tidb_serverless(&pool).await? {
            Ok(None)
        } else {
            Ok(Some(WorkloadDescription::tidb(
                output,
                tables,
                read_tidb_statements_summary(&pool, &config.database).await?,
                read_tidb_system_metrics(&pool).await?,
            )))
        }
    } else if is_mysql_performance_schema_enabled(&pool).await? {
        Ok(Some(WorkloadDescription::mysql(
            output,
            tables,
            read_mysql_statements_summary(&pool, &config.database).await?,
        )))
    } else if is_mariadb(&pool).await? {
        Err(anyhow!("Please enable the 'Performance Schema' on your MariaDB server and keep it active for at least a full business day to ensure comprehensive workload coverage. For instructions, see this guide: https://mariadb.com/kb/en/performance-schema-overview/#activating-the-performance-schema"))
    } else {
        Err(anyhow!("Please enable the 'Performance Schema' on your MySQL server and keep it active for at least a full business day to ensure comprehensive workload coverage. For instructions, see this guide: https://dev.mysql.com/doc/refman/5.7/en/performance-schema-startup-configuration.html"))
    }
}

#[derive(Debug, FromRow)]
struct TablesInformation {
    total_rows: Option<u64>,
    total_data_in_bytes: Option<u64>,
    total_index_in_bytes: Option<u64>,
}

async fn check_variable_value(pool: &Pool<MySql>, variable: &str, value: &str) -> Result<bool> {
    Ok(
        sqlx::query_as(&format!("SHOW VARIABLES LIKE '{}'", variable))
            .fetch_optional(pool)
            .await?
            .map(|v: (String, String)| v.1 == value)
            .unwrap_or(false),
    )
}

async fn read_tables_information(pool: &Pool<MySql>, database: &str) -> Result<TablesInformation> {
    Ok(sqlx::query_as("SELECT CAST(SUM(TABLE_ROWS) AS UNSIGNED) AS total_rows, CAST(SUM(DATA_LENGTH) AS UNSIGNED) AS total_data_in_bytes, CAST(SUM(INDEX_LENGTH) AS UNSIGNED) AS total_index_in_bytes FROM information_schema.TABLES WHERE TABLE_SCHEMA=?")
        .bind(database).fetch_one(pool).await?)
}

#[derive(Default, Debug)]
struct MySQLStatementsSummary {
    read_queries: u64,
    read_rows: u64,
    sent_rows: u64,
    write_queries: u64,
    write_rows: u64,
    start_time: DateTime<Utc>,
    end_time: DateTime<Utc>,
}

#[derive(FromRow, Debug)]
struct MySQLStatementSummary {
    #[sqlx(rename = "DIGEST_TEXT")]
    sql: String,
    #[sqlx(rename = "COUNT_STAR")]
    count: u64,
    #[sqlx(rename = "SUM_ROWS_AFFECTED")]
    affected_rows: u64,
    /* the term used IN MySQL official client is affect*/
    #[sqlx(rename = "SUM_ROWS_SENT")]
    sent_rows: u64,
    #[sqlx(rename = "SUM_ROWS_EXAMINED")]
    read_rows: u64,
    #[sqlx(rename = "FIRST_SEEN")]
    first_seen: DateTime<Utc>,
    #[sqlx(rename = "LAST_SEEN")]
    last_seen: DateTime<Utc>,
}

async fn is_mysql_performance_schema_enabled(pool: &Pool<MySql>) -> Result<bool> {
    check_variable_value(pool, "performance_schema", "ON").await
}

async fn read_mysql_statements_summary(
    pool: &Pool<MySql>,
    database: &str,
) -> Result<MySQLStatementsSummary> {
    let statements_summary: Vec<MySQLStatementSummary> =
        sqlx::query_as("SELECT DIGEST_TEXT, COUNT_STAR, SUM_ROWS_AFFECTED, SUM_ROWS_SENT, SUM_ROWS_EXAMINED, FIRST_SEEN, LAST_SEEN FROM performance_schema.events_statements_summary_by_digest WHERE SCHEMA_NAME=? AND LAST_SEEN >= DATE_SUB(NOW(), INTERVAL 7 DAY)")
            .bind(database).fetch_all(pool).await?;
    let now = Utc::now();
    let seven_days_ago = now.sub(Duration::days(7));
    if statements_summary.is_empty() {
        return Ok(MySQLStatementsSummary {
            end_time: now,
            start_time: seven_days_ago,
            ..Default::default()
        });
    }
    let is_write_pattern = Regex::new("^INSERT |^DELETE |^UPDATE ")?;
    Ok(statements_summary.into_iter().fold(
        MySQLStatementsSummary {
            start_time: now,
            end_time: seven_days_ago,
            ..Default::default()
        },
        |mut acc, statement| -> MySQLStatementsSummary {
            acc.end_time = max(statement.last_seen, acc.end_time);
            acc.start_time = min(statement.first_seen, acc.start_time);
            acc.read_rows += statement.read_rows;
            acc.sent_rows += statement.sent_rows;
            acc.write_rows += statement.affected_rows;
            if is_write_pattern.find(&statement.sql).is_some() {
                acc.write_queries += statement.count;
            } else {
                acc.read_queries += statement.count;
            }
            acc
        },
    ))
}

#[derive(Debug, Default)]
struct TiDBStatementsSummary {
    read_queries: u64,
    read_rows: u64,
    sent_rows: u64,
    write_queries: u64,
    write_bytes: u64,
    start_time: DateTime<Utc>,
    end_time: DateTime<Utc>,
}

#[derive(FromRow, Debug)]
struct TiDBStatementSummary {
    #[sqlx(rename = "STMT_TYPE")]
    statement_type: String,
    #[sqlx(rename = "EXEC_COUNT")]
    count: u64,
    #[sqlx(rename = "AVG_RESULT_ROWS")]
    avg_result_rows: u64,
    #[sqlx(rename = "AVG_PROCESSED_KEYS")]
    avg_processed_keys: u64,
    #[sqlx(rename = "AVG_WRITE_SIZE")]
    avg_write_bytes: u64,
    #[sqlx(rename = "FIRST_SEEN")]
    first_seen: DateTime<Utc>,
    #[sqlx(rename = "LAST_SEEN")]
    last_seen: DateTime<Utc>,
}

#[derive(Debug, Default)]
struct TiDBSystemMetrics {
    write_bytes_per_hour: u64,
    write_requests_per_hour: u64,
    read_bytes_per_hour: u64,
    read_requests_per_hour: u64,
}

async fn is_tidb_stmt_summary_enabled(pool: &Pool<MySql>) -> Result<bool> {
    check_variable_value(pool, "tidb_enable_stmt_summary", "ON").await
}

async fn read_tidb_system_metrics(pool: &Pool<MySql>) -> Result<TiDBSystemMetrics> {
    let mut interval = 7;

    loop {
        let (start, end): (String, String) = sqlx::query_as(
            "SELECT CAST(DATE_SUB(NOW(), INTERVAL ? DAY) AS CHAR), CAST(NOW() AS CHAR)",
        )
        .bind(interval)
        .fetch_one(pool)
        .await?;
        let sql = format!(
            "SELECT 'write_bytes' AS type, CAST(SUM(`value`) AS UNSIGNED) AS `value` FROM metrics_schema.tidb_kv_write_total_size WHERE time BETWEEN '{}' AND '{}' UNION\n\
                 SELECT 'write_requests' AS type, CAST(SUM(`value`) AS UNSIGNED) AS `value` FROM metrics_schema.tidb_kv_request_total_count WHERE type IN ('Prewrite', 'Commit') AND time BETWEEN '{}' AND '{}' UNION\n\
                 SELECT 'read_bytes' AS type, CAST(SUM(`value`) AS UNSIGNED) AS `value` FROM metrics_schema.tikv_cop_total_rocksdb_perf_statistics WHERE metric IN ('get_read_bytes', 'iter_red_bytes') AND req IN ('index', 'select') AND time BETWEEN '{}' AND '{}' UNION\n\
                 SELECT 'read_requests' AS type, CAST(SUM(`value`) AS UNSIGNED) AS `value` FROM metrics_schema.tidb_kv_request_total_count WHERE type not IN ('Prewrite', 'Commit') AND time BETWEEN '{}' AND '{}'"
            , start, end, start, end, start, end, start, end);
        let metrics: Result<Vec<(String, Option<u64>)>> = sqlx::query_as(&sql)
            .fetch_all(pool)
            .await
            .map_err(Into::into);
        if let Ok(metrics) = metrics {
            let hours = interval * 24;
            return Ok(metrics.into_iter().fold(
                Default::default(),
                |mut acc, metric| -> TiDBSystemMetrics {
                    match metric.0.as_str() {
                        "write_bytes" => acc.write_bytes_per_hour = metric.1.unwrap_or(0) / hours,
                        "write_requests" => {
                            acc.write_requests_per_hour = metric.1.unwrap_or(0) / hours
                        }
                        "read_bytes" => acc.read_bytes_per_hour = metric.1.unwrap_or(0) / hours,
                        "read_requests" => {
                            acc.read_requests_per_hour = metric.1.unwrap_or(0) / hours
                        }
                        _ => {}
                    }
                    acc
                },
            ));
        }
        if interval == 1 {
            return Err(anyhow!("Failed to read metrics schema, please check your prometheus setup AND make sure it is working AS expected"));
        }
        interval -= 1;
    }
}

async fn read_tidb_statements_summary(
    pool: &Pool<MySql>,
    database: &str,
) -> Result<Option<TiDBStatementsSummary>> {
    if !is_tidb_stmt_summary_enabled(pool).await? {
        return Ok(None);
    }
    let statements_summary: Vec<TiDBStatementSummary> =
        sqlx::query_as(
            "SELECT STMT_TYPE, DIGEST_TEXT, EXEC_COUNT, AVG_AFFECTED_ROWS, CAST(AVG_RESULT_ROWS AS UNSIGNED) AS AVG_RESULT_ROWS, AVG_PROCESSED_KEYS, CAST(AVG_WRITE_SIZE AS UNSIGNED) AS AVG_WRITE_SIZE, FIRST_SEEN, LAST_SEEN FROM information_schema.CLUSTER_STATEMENTS_SUMMARY WHERE SCHEMA_NAME=? AND LAST_SEEN >= DATE_SUB(NOW(), INTERVAL 7 DAY) UNION ALL SELECT STMT_TYPE, DIGEST_TEXT, EXEC_COUNT, AVG_AFFECTED_ROWS, CAST(AVG_RESULT_ROWS AS UNSIGNED) AS AVG_RESULT_ROWS, AVG_PROCESSED_KEYS, CAST(AVG_WRITE_SIZE AS UNSIGNED) AS AVG_WRITE_SIZE, FIRST_SEEN, LAST_SEEN FROM information_schema.CLUSTER_STATEMENTS_SUMMARY_HISTORY WHERE SCHEMA_NAME=? AND LAST_SEEN >= DATE_SUB(NOW(), INTERVAL 7 DAY)"
        )
            .bind(database).bind(database).fetch_all(pool).await?;
    let now = Utc::now();
    let seven_days_ago = now.sub(Duration::days(7));
    if statements_summary.is_empty() {
        return Ok(Some(TiDBStatementsSummary {
            end_time: now,
            start_time: seven_days_ago,
            ..Default::default()
        }));
    }
    Ok(Some(statements_summary.into_iter().fold(
        TiDBStatementsSummary {
            start_time: now,
            end_time: seven_days_ago,
            ..Default::default()
        },
        |mut acc, statement| -> TiDBStatementsSummary {
            acc.end_time = max(statement.last_seen, acc.end_time);
            acc.start_time = min(statement.first_seen, acc.start_time);
            acc.read_rows += statement.avg_processed_keys * statement.count;
            acc.sent_rows += statement.avg_result_rows * statement.count;
            acc.write_bytes += statement.avg_write_bytes * statement.count;
            if matches!(
                statement.statement_type.as_str(),
                "Delete" | "Update" | "Insert" | "Replace"
            ) {
                acc.write_queries += statement.count;
            } else {
                acc.read_queries += statement.count;
            }
            acc
        },
    )))
}

async fn check_version_signature(pool: &Pool<MySql>, pattern: &str) -> Result<bool> {
    let version: (String,) = sqlx::query_as("SELECT version()").fetch_one(pool).await?;
    Ok(Regex::new(pattern)?.find(&version.0).is_some())
}

async fn is_tidb(pool: &Pool<MySql>) -> Result<bool> {
    check_version_signature(pool, "^\\d+\\.\\d+\\.\\d+-(?i)TiDB(?-i)-.*").await
}

async fn is_tidb_serverless(pool: &Pool<MySql>) -> Result<bool> {
    check_version_signature(
        pool,
        "^\\d+\\.\\d+\\.\\d+-(?i)TiDB(?-i)-v\\d+\\.\\d+\\.\\d+-(?i)serverless(?-i).*",
    )
    .await
}

async fn is_mariadb(pool: &Pool<MySql>) -> Result<bool> {
    check_version_signature(pool, "^\\d+\\.\\d+\\.\\d+-(?i)MariaDB(?-i)-.*").await
}
