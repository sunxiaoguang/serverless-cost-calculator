mod calculator;
mod output;
mod source;

use crate::output::OutputFormat;
use clap::{ArgAction, Parser};

#[derive(Parser)]
#[command(
    name = "TiDB Serverless Cost Calculator",
    version,
    arg_required_else_help(true),
    disable_help_flag(true),
    about = "Estimate the cost of TiDB Serverless for your existing MySQL-compatible databases."
)]
struct CalculatorOptions {
    #[arg(
        id = "host",
        short = 'h',
        long = "host",
        env = "DB_HOST",
        default_value = "localhost",
        help = "Sets the host for the MySQL server",
        num_args(1)
    )]
    host: String,
    #[arg(
        id = "port",
        short = 'P',
        long = "port",
        env = "DB_PORT",
        default_value_t = 3306,
        help = "Sets the port for the MySQL server",
        num_args(1)
    )]
    port: u16,
    #[arg(
        id = "user",
        short = 'u',
        long = "user",
        env = "DB_USERNAME",
        default_value = "root",
        help = "Sets the username for the MySQL server",
        num_args(1)
    )]
    user: String,
    #[arg(
        id = "password",
        short = 'p',
        long = "password",
        env = "DB_PASSWORD",
        default_value = "",
        help = "Sets the password for the MySQL server",
        num_args(1)
    )]
    password: String,
    #[arg(
        id = "database",
        short = 'D',
        long = "database",
        env = "DB_DATABASE",
        help = "Sets the database for the MySQL server",
        num_args(1),
        required(true)
    )]
    database: String,
    #[arg(
        id = "region",
        short = 'r',
        long = "region",
        env = "SERVERLESS_REGION",
        default_value = "us-east-1",
        help = "AWS Region of the TiDB Serverless cluster",
        num_args(1)
    )]
    region: String,
    #[arg(
        id = "analyze",
        short = 'a',
        long = "analyze",
        env = "DB_ANALYZE",
        action = ArgAction::SetTrue,
        default_value_t = false,
        help = "Run ANALYZE before reading system tables depending on statistics data",
    )]
    analyze: bool,
    #[arg(
        id = "output",
        short = 'o',
        long = "output",
        env = "OUTPUT",
        default_value = "human",
        help = "Output format. One of: json|yaml|human"
    )]
    output: OutputFormat,
}
#[tokio::main]
async fn main() {
    let options = CalculatorOptions::parse();
    let output = options.output;

    output.welcome(&options);
    let workload = match source::load_workload_description(
        output,
        &options.host,
        options.port,
        &options.user,
        &options.password,
        &options.database,
        options.analyze,
    )
    .await
    {
        Err(e) => {
            return output.fatal(&format!("The workload failed to load: {}", e));
        }
        Ok(Some(workload)) => workload,
        Ok(None) => {
            return output.info("You are already using TiDB Serverless. Please check your billing in the TiDB Cloud Console for charges. For more information, visit https://docs.pingcap.com/tidbcloud/tidb-cloud-billing");
        }
    };
    match calculator::estimate(&options.region, &workload) {
        Err(e) => {
            return output.fatal(&format!("The cost estimation failed: {}", e));
        }
        Ok(estimation) => {
            output.report(workload, estimation);
        }
    }
}
