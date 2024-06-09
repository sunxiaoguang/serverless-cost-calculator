use anyhow::{anyhow, Result};

use crate::source::WorkloadDescription;

const KILO: u64 = 1024;
const MEGA: u64 = KILO * 1024;
const HOURS_PER_MONTH: u64 = 730;

pub struct WorkloadUsage {
    row_based_storage_in_mib: u64,
    network_egress_in_mib: u64,
    request_units_in_million: u64,
}

pub struct WorkloadEstimation {
    pub storage_cost: f64,
    pub request_units_cost: f64,
    pub free_credit: f64,
}

fn calculate(
    row_based_price: f64,
    ru_price: f64,
    free_credit: f64,
    usage: WorkloadUsage,
) -> WorkloadEstimation {
    WorkloadEstimation {
        storage_cost: usage.row_based_storage_in_mib as f64 * row_based_price / 1024f64,
        request_units_cost: ((usage.network_egress_in_mib as f64 / 1024f64)
            + usage.request_units_in_million as f64)
            * ru_price,
        free_credit,
    }
}

fn estimate_usage(workload: WorkloadDescription) -> WorkloadUsage {
    let read_request_units_per_hour =
        workload.read_requests_per_hour / 8 + workload.read_bytes_per_hour / (64 * KILO);
    let write_request_units_per_hour =
        (workload.write_requests_per_hour + workload.write_bytes_per_hour / KILO) * 3;
    let request_units_per_hour = read_request_units_per_hour + write_request_units_per_hour;
    WorkloadUsage {
        row_based_storage_in_mib: (workload.total_data_in_bytes + workload.total_index_in_bytes)
            / MEGA,
        network_egress_in_mib: workload.sent_bytes_per_hour / MEGA,
        request_units_in_million: request_units_per_hour * HOURS_PER_MONTH / MEGA,
    }
}

pub fn estimate(region: &str, workload: WorkloadDescription) -> Result<WorkloadEstimation> {
    let usage = estimate_usage(workload);
    match region {
        "us-east-1" => Ok(calculate(0.2, 0.1, 6.0, usage)),
        "us-west-2" => Ok(calculate(0.2, 0.1, 6.0, usage)),
        "eu-central-1" => Ok(calculate(0.24, 0.12, 7.2, usage)),
        "ap-southeast-1" => Ok(calculate(0.24, 0.12, 7.2, usage)),
        "ap-northeast-1" => Ok(calculate(0.24, 0.12, 7.2, usage)),
        _ => Err(anyhow!("The region '{}' is invalid", region)),
    }
}
