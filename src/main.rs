mod calculator;
mod source;
use clap::{value_parser, Arg, Command};
use colored::*;
use prettytable::{row, Table};
use readable::num::*;

#[tokio::main]
async fn main() {
    let matches = Command::new("TiDB Serverless Cost Calculator")
        .version("0.1.0")
        .about("Estimate the cost of TiDB Serverless for your existing MySQL-compatible databases.")
        .arg_required_else_help(true)
        .disable_help_flag(true)
        .arg(
            Arg::new("host")
                .short('h')
                .long("host")
                .value_name("HOST")
                .default_value("localhost")
                .help("Sets the host for the MySQL server")
                .num_args(1),
        )
        .arg(
            Arg::new("port")
                .short('P')
                .long("port")
                .value_name("PORT")
                .default_value("3306")
                .value_parser(value_parser!(u16))
                .help("Sets the port for the MySQL server")
                .num_args(1)
                .default_value("3306"), // Default MySQL port
        )
        .arg(
            Arg::new("user")
                .short('u')
                .long("user")
                .value_name("USER")
                .default_value("root")
                .num_args(1)
                .help("Sets the username for the MySQL server"),
        )
        .arg(
            Arg::new("password")
                .short('p')
                .long("password")
                .value_name("PASSWORD")
                .default_value("")
                .num_args(1)
                .help("Sets the password for the MySQL server"),
        )
        .arg(
            Arg::new("database")
                .short('d')
                .long("database")
                .num_args(1)
                .value_name("DATABASE")
                .help("Sets the database to connect to")
                .required(true),
        )
        .arg(
            Arg::new("region")
                .short('r')
                .long("region")
                .value_name("REGION")
                .default_value("us-east-1")
                .num_args(1)
                .help("AWS Region of the new TiDB Serverless cluster"),
        )
        .get_matches();

    // Extract the values from the command line arguments
    let host = matches
        .get_one::<String>("host")
        .expect("`host` is required")
        .to_owned();
    let port: u16 = *matches.get_one("port").expect("`port` is required");
    let user = matches
        .get_one::<String>("user")
        .expect("`user` is required")
        .to_owned();
    let password = matches.get_one::<String>("password").unwrap().to_owned();
    let database = matches
        .get_one::<String>("database")
        .expect("`database` is required")
        .to_owned();
    let region = matches
        .get_one::<String>("region")
        .expect("`region` is required")
        .to_owned();

    println!(
        "Connecting to the MySQL compatible database at '{}' as the user '{}' using the database '{}'",
        format!("{}:{}", host, port).bold().green(),
        user.bold().green(),
        database.bold().green()
    );

    let workload =
        match source::load_workload_description(host, port, user, password, database).await {
            Err(e) => {
                println!("{}", format!("The workload failed to load: {}", e).red());
                return;
            }
            Ok(workload) => workload,
        };
    match calculator::estimate(region.as_str(), workload) {
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
            table.add_row(row![bFg -> "Row-based Storage", bFgr -> format!("${}", Float::from_2(estimation.storage_cost))]);
            table.add_row(row![bFg -> "Request Units", bFgr -> format!("${}", Float::from_2(estimation.request_units_cost))]);
            table.add_row(row![bFg -> "Free Credits", bFgr -> format!("-${}", Float::from_2(estimation.free_credit))]);
            table.add_row(row![bFg -> "Total", bFgr -> total]);
            table.printstd();
        }
    }
}
