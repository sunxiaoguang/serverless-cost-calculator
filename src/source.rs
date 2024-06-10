use std::cmp::{max, min};
use std::ops::Sub;

use anyhow::{anyhow, Result};
use chrono::{DateTime, Duration, Utc};
use colored::*;
use regex::Regex;
use sqlx::{FromRow, MySql, Pool};

const TARGET_REGION_SIZE: u64 = 256 * 1024 * 1024;
const MINUTES_PER_HOUR: u64 = 60;

#[derive(Default, Debug)]
pub struct WorkloadDescription {
    pub read_requests_per_hour: u64,
    pub read_bytes_per_hour: u64,
    pub write_requests_per_hour: u64,
    pub write_bytes_per_hour: u64,
    pub sent_bytes_per_hour: u64,
    pub total_data_in_bytes: u64,
    pub total_index_in_bytes: u64,
}

impl WorkloadDescription {
    fn check_summary_duration(duration_in_minutes: u64) {
        if duration_in_minutes < MINUTES_PER_HOUR {
            println!("{}", format!("The statement summary, covering only {} minute(s), is less than an hour's workload. It is highly recommended to collect at least a day's worth of data before running the estimation to prevent distortion.", duration_in_minutes).bold().red());
        } else if duration_in_minutes < MINUTES_PER_HOUR * 24 {
            println!("{}", format!("The statement summary, covering only {} hour(s), is less than a full day's workload and may not reflect the full business. Consider running the tool after collecting data for a longer period to ensure accuracy.", duration_in_minutes / MINUTES_PER_HOUR).bold().yellow());
        }
    }
    fn mysql(tables: TablesInformation, summary: MySQLStatementsSummary) -> Self {
        let duration_in_minutes =
            max(summary.end_time.sub(summary.start_time).num_minutes(), 1) as u64;
        Self::check_summary_duration(duration_in_minutes);
        let total_storage_in_bytes = max(
            tables.total_index_in_bytes.unwrap_or(0) + tables.total_data_in_bytes.unwrap_or(0),
            1,
        );
        let average_row_size_in_bytes =
            total_storage_in_bytes / max(tables.total_rows.unwrap_or(0), 1);
        let estimated_number_of_regions = total_storage_in_bytes / TARGET_REGION_SIZE;

        let read_bytes_per_hour =
            MINUTES_PER_HOUR * average_row_size_in_bytes * summary.read_rows / duration_in_minutes;
        let read_requests_per_hour = max(
            MINUTES_PER_HOUR * summary.read_requests / duration_in_minutes,
            1,
        );
        let read_bytes_per_request = read_bytes_per_hour / read_requests_per_hour;
        let read_regions_per_request = max(
            read_bytes_per_request * estimated_number_of_regions / total_storage_in_bytes,
            1,
        );

        let write_bytes_per_hour =
            MINUTES_PER_HOUR * average_row_size_in_bytes * summary.write_rows / duration_in_minutes;
        let write_requests_per_hour = max(
            MINUTES_PER_HOUR * summary.write_queries / duration_in_minutes,
            1,
        );
        let write_bytes_per_request = write_bytes_per_hour / write_requests_per_hour;
        let write_regions_per_request = max(
            write_bytes_per_request * estimated_number_of_regions / total_storage_in_bytes,
            1,
        );

        WorkloadDescription {
            read_requests_per_hour: read_requests_per_hour * read_regions_per_request,
            read_bytes_per_hour,
            write_requests_per_hour: write_requests_per_hour * write_regions_per_request,
            write_bytes_per_hour,
            sent_bytes_per_hour: MINUTES_PER_HOUR * average_row_size_in_bytes * summary.sent_rows
                / duration_in_minutes,
            total_data_in_bytes: tables.total_data_in_bytes.unwrap_or(0),
            total_index_in_bytes: tables.total_index_in_bytes.unwrap_or(0),
        }
    }

    fn tidb(
        tables: TablesInformation,
        summary: Option<TiDBStatementsSummary>,
        metrics: TiDBSystemMetrics,
    ) -> Self {
        let (write_bytes_per_hour, sent_bytes_per_hour) = match summary {
            Some(summary) => {
                let duration_in_minutes =
                    max(summary.end_time.sub(summary.start_time).num_minutes(), 1) as u64;
                Self::check_summary_duration(duration_in_minutes);
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
                println!("{}", "The 'Statement Summary Tables' are disabled; when they are available, estimations can be more accurate.".bold().yellow());
                println!("{}", "For detailed instruction, visit https://docs.pingcap.com/tidb/stable/statement-summary-tables#parameter-configuration".bold().yellow());
                (metrics.write_bytes_per_hour, 0)
            }
        };
        WorkloadDescription {
            read_requests_per_hour: metrics.read_requests_per_hour,
            read_bytes_per_hour: metrics.read_bytes_per_hour,
            write_requests_per_hour: metrics.write_requests_per_hour,
            write_bytes_per_hour,
            sent_bytes_per_hour,
            total_data_in_bytes: tables.total_data_in_bytes.unwrap_or(0),
            total_index_in_bytes: tables.total_index_in_bytes.unwrap_or(0),
        }
    }
}

pub async fn load_workload_description(
    host: String,
    port: u16,
    user: String,
    password: String,
    database: String,
) -> Result<Option<WorkloadDescription>> {
    let connection_string = format!(
        "mysql://{}:{}@{}:{}/{}",
        user, password, host, port, database
    );
    let pool = sqlx::MySqlPool::connect(&connection_string).await?;

    let tables = read_tables_information(&pool, &database).await?;
    if is_tidb(&pool).await? {
        if is_tidb_serverless(&pool).await? {
            Ok(None)
        } else {
            Ok(Some(WorkloadDescription::tidb(
                tables,
                read_tidb_statements_summary(&pool, &database).await?,
                read_tidb_system_metrics(&pool).await?,
            )))
        }
    } else if is_mysql_performance_schema_enabled(&pool).await? {
        Ok(Some(WorkloadDescription::mysql(
            tables,
            read_mysql_statements_summary(&pool, &database).await?,
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
    read_requests: u64,
    read_rows: u64,
    sent_rows: u64,
    write_queries: u64,
    write_rows: u64,
    start_time: DateTime<Utc>,
    end_time: DateTime<Utc>,
}

#[derive(FromRow)]
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
                acc.read_requests += statement.count;
            }
            acc
        },
    ))
}

#[derive(Debug, Default)]
struct TiDBStatementsSummary {
    read_requests: u64,
    read_rows: u64,
    sent_rows: u64,
    write_queries: u64,
    write_bytes: u64,
    start_time: DateTime<Utc>,
    end_time: DateTime<Utc>,
}

#[derive(FromRow)]
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
                acc.read_requests += statement.count;
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
