mod calculator;
mod source;
use clap::{ArgAction, Parser};
use colored::*;
use prettytable::{row, Table};
use readable::num::*;
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
}
#[tokio::main]
async fn main() {
    let options = CalculatorOptions::parse();

    println!(
        "Connecting to the MySQL compatible database at '{}' as the user '{}' using the database '{}'",
        format!("{}:{}", options.host, options.port).bold().green(),
        options.user.bold().green(),
        options.database.bold().green()
    );

    let workload = match source::load_workload_description(
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
            println!(
                "{}",
                format!("The workload failed to load: {}", e).bold().red()
            );
            return;
        }
        Ok(Some(workload)) => workload,
        Ok(None) => {
            println!("{}", "You are already using TiDB Serverless. Please check your billing in the TiDB Cloud Console for charges. For more information, visit https://docs.pingcap.com/tidbcloud/tidb-cloud-billing".bold().green());
            return;
        }
    };
    match calculator::estimate(&options.region, workload) {
        Err(e) => {
            println!("{}", format!("The cost estimation failed: {}", e).red());
            return;
        }
        Ok(estimation) => {
            let total = if estimation.storage_cost + estimation.request_units_cost
                <= estimation.free_credit
            {
                "$0.00".to_string()
            } else {
                format!(
                    "${}",
                    Float::from_2(
                        estimation.storage_cost + estimation.request_units_cost
                            - estimation.free_credit
                    )
                )
            };
            println!(
                "The estimated monthly cost for your workload is {}",
                total.bold().green()
            );
            let mut table = Table::new();
            table.set_titles(row![bFg -> "SKU", bFgr -> "Cost"]);
            table.add_row(row![bFg -> "Request Units", bFgr -> format!("${}", Float::from_2(estimation.request_units_cost))]);
            table.add_row(row![bFg -> "Row-based Storage", bFgr -> format!("${}", Float::from_2(estimation.storage_cost))]);
            table.add_row(row![bFg -> "Free Credits", bFgr -> format!("-${}", Float::from_2(estimation.free_credit))]);
            table.add_row(row![bFg -> "Total", bFgr -> total]);
            table.printstd();
        }
    }

    println!("\n{}", "Notes:".bold().green());
    println!("{}", "* Request units are estimated based on statistical data from the past, up to seven days. Be cautious: severe fluctuations in recent workload, such as ingesting a large volume of data, can skew the final estimation.".bold().green());
    println!("{}", "* The storage size is estimated from statistical data, which differs from the actual data size.".bold().green());
    println!("{}", "* TiDB Serverless encodes data differently from MySQL, resulting in slightly different storage consumption.".bold().green());
    println!("{}", "* The TiDB Serverless storage size meter does not account for data compression or replicas.".bold().green());
    println!("{}", "* For detailed pricing information, visit https://www.pingcap.com/tidb-serverless-pricing-details".bold().green());
    println!("{}", "* For additional questions, refer to the FAQs on https://docs.pingcap.com/tidbcloud/serverless-faqs".bold().green());
}
