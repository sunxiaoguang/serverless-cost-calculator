# TiDB Serverless Cost Calculator

[![GitHub stars](https://img.shields.io/github/stars/sunxiaoguang/serverless-cost-calculator)](https://github.com/sunxiaoguang/serverless-cost-calculator)
[![GitHub issues](https://img.shields.io/github/issues/sunxiaoguang/serverless-cost-calculator)](https://github.com/sunxiaoguang/serverless-cost-calculator/issues)
[![GitHub license](https://img.shields.io/github/license/sunxiaoguang/serverless-cost-calculator)](https://github.com/sunxiaoguang/serverless-cost-calculator/blob/main/LICENSE)

Estimate the cost of TiDB Serverless for your existing MySQL-compatible databases.

## Overview

The `serverless-cost-calculator` is an application that estimates the monthly cost of running MySQL-compatible databases on TiDB Serverless, analyzing your current workload to provide a projected expense.

## Prerequisites

- Rust (https://www.rust-lang.org/tools/install)
- Cargo (Rust's package manager, included with Rust)
- MySQL-compatible database server (MySQL or TiDB)

## Building the Tool

To build the `serverless-cost-calculator`, clone the repository and build it using Cargo:

```sh
git clone https://github.com/sunxiaoguang/serverless-cost-calculator.git
cd serverless-cost-calculator
cargo build --release
```

## Usage

After building the tool, you can run it using the following command:

```sh
./target/release/serverless-cost-calculator --database <DATABASE> --region <REGION>
```

Where:
- `<DATABASE>` is the name of the database you want to estimate.
- `<REGION>` is the AWS region for the TiDB Serverless cluster.

You can also specify the host, port, user, and password for your MySQL server using the respective flags.

### Example

```sh
./target/release/serverless-cost-calculator --database mydb --host localhost --port 3306 --user root --password abcxyz --region us-east-1
```

## Output

The tool will output an estimated monthly cost for your workload, broken down by request units and storage costs, and will display any relevant notes or warnings.

## Contributing

Contributions are welcome! For more information on how to contribute, please refer to our [CONTRIBUTING.md](CONTRIBUTING.md).

## License

This project is licensed under the [Apache-2.0 License](LICENSE).
