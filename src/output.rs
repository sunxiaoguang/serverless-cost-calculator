use crate::calculator::WorkloadEstimation;
use crate::source::WorkloadDescription;
use crate::CalculatorOptions;
use colored::Colorize;
use prettytable::{row, Table};
use readable::num::Float;
use serde::Serialize;
use std::process::exit;

#[derive(Serialize)]
struct WorkloadReport {
    workload: WorkloadDescription,
    estimation: WorkloadEstimation,
}

#[derive(clap::ValueEnum, Clone, Copy, Default, Debug, Serialize)]
pub enum OutputFormat {
    #[default]
    Human,
    Json,
    Yaml,
}

impl OutputFormat {
    pub fn welcome(&self, options: &CalculatorOptions) {
        match &self {
            OutputFormat::Json => return,
            OutputFormat::Yaml => return,
            _ => (),
        }
        println!(
            "Connecting to the MySQL compatible database at '{}' as the user '{}' using the database '{}'",
            format!("{}:{}", options.host, options.port).bold().green(),
            options.user.bold().green(),
            options.database.bold().green(),
        );
    }

    pub fn fatal(&self, error: &str) {
        self.error(error);
        exit(1);
    }

    pub fn error(&self, error: &str) {
        println!("{}", error.bold().red());
    }

    pub fn warn(&self, warn: &str) {
        println!("{}", warn.bold().yellow());
    }

    pub fn info(&self, info: &str) {
        if let OutputFormat::Human = *self {
            println!("{}", info.bold().green());
        }
    }

    pub fn report(&self, workloads: Vec<WorkloadDescription>, estimation: Vec<WorkloadEstimation>) {
        if let OutputFormat::Human = *self {
            return Self::output_human(estimation);
        }
        let reports: Vec<WorkloadReport> = workloads
            .into_iter()
            .zip(estimation)
            .map(|pair| WorkloadReport {
                workload: pair.0,
                estimation: pair.1,
            })
            .collect();

        println!(
            "{}",
            match *self {
                OutputFormat::Json => serde_json::to_string_pretty(&reports).unwrap(),
                OutputFormat::Yaml => serde_yaml::to_string(&reports).unwrap(),
                _ => unreachable!(),
            }
        );
    }

    fn output_human_step(index: Option<usize>, estimation: &WorkloadEstimation) {
        if let Some(index) = index {
            println!("Cluster: {}", format!("{}", index).bold().green());
        }
        let total =
            if estimation.storage_cost + estimation.request_units_cost <= estimation.free_credit {
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

    fn output_human(estimation: Vec<WorkloadEstimation>) {
        let single_workload = estimation.len() == 1;
        for pair in estimation.iter().enumerate() {
            Self::output_human_step(if single_workload { None } else { Some(pair.0) }, pair.1)
        }

        println!("\n{}", "Notes:".bold().green());
        println!("{}", "* Request units are estimated based on statistical data from the past, up to seven days. Be cautious: severe fluctuations in recent workload, such as ingesting a large volume of data, can skew the final estimation.".bold().green());
        println!("{}", "* The storage size is estimated from statistical data, which differs from the actual data size.".bold().green());
        println!("{}", "* TiDB Serverless encodes data differently from MySQL, resulting in slightly different storage consumption.".bold().green());
        println!("{}", "* The TiDB Serverless storage size meter does not account for data compression or replicas.".bold().green());
        println!("{}", "* For detailed pricing information, visit https://www.pingcap.com/tidb-serverless-pricing-details".bold().green());
        println!("{}", "* For additional questions, refer to the FAQs on https://docs.pingcap.com/tidbcloud/serverless-faqs".bold().green());
    }
}
