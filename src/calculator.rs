use anyhow::{anyhow, Result};
use serde::Serialize;

use crate::source::WorkloadDescription;

const KILO: u64 = 1024;
const MEGA: u64 = KILO * 1024;
const HOURS_PER_MONTH: u64 = 730;

pub struct WorkloadUsage {
    row_based_storage_in_mib: u64,
    network_egress_in_mib: u64,
    request_units_in_million: u64,
}

#[derive(Default, Debug, Serialize)]
pub struct WorkloadEstimation {
    pub storage_cost: f64,
    pub request_units_cost: f64,
    pub free_credit: f64,
}

fn calculate(
    row_based_price: f64,
    ru_price: f64,
    free_credit: f64,
    usages: Vec<WorkloadUsage>,
) -> Vec<WorkloadEstimation> {
    usages
        .into_iter()
        .map(|usage| WorkloadEstimation {
            storage_cost: usage.row_based_storage_in_mib as f64 * row_based_price / 1024f64,
            request_units_cost: ((usage.network_egress_in_mib as f64 / 1024f64)
                + usage.request_units_in_million as f64)
                * ru_price,
            free_credit,
        })
        .collect()
}

fn estimate_usages(workloads: &[WorkloadDescription]) -> Vec<WorkloadUsage> {
    workloads
        .iter()
        .map(|workload| {
            let read_request_units_per_hour = (workload.read.requests_per_hour.unwrap_or(0) / 8)
                + (workload.read.bytes_per_hour / (64 * KILO));
            let write_request_units_per_hour = (workload.write.requests_per_hour.unwrap_or(0)
                + (workload.write.bytes_per_hour / KILO))
                * 3;
            let request_units_per_hour = read_request_units_per_hour + write_request_units_per_hour;
            WorkloadUsage {
                row_based_storage_in_mib: (workload.storage.data_in_bytes
                    + workload.storage.index_in_bytes)
                    / MEGA,
                network_egress_in_mib: workload.egress.bytes_per_hour * HOURS_PER_MONTH / MEGA,
                request_units_in_million: request_units_per_hour * HOURS_PER_MONTH / MEGA,
            }
        })
        .collect()
}

pub fn estimate(
    region: &str,
    workloads: &[WorkloadDescription],
) -> Result<Vec<WorkloadEstimation>> {
    let usages = estimate_usages(workloads);
    match region {
        "us-east-1" => Ok(calculate(0.2, 0.1, 6.0, usages)),
        "us-west-2" => Ok(calculate(0.2, 0.1, 6.0, usages)),
        "eu-central-1" => Ok(calculate(0.24, 0.12, 7.2, usages)),
        "ap-southeast-1" => Ok(calculate(0.24, 0.12, 7.2, usages)),
        "ap-northeast-1" => Ok(calculate(0.24, 0.12, 7.2, usages)),
        _ => Err(anyhow!("The region '{}' is invalid", region)),
    }
}
