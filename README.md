# TiDB Serverless Cost Calculator

[![GitHub stars](https://img.shields.io/github/stars/sunxiaoguang/serverless-cost-calculator)](https://github.com/sunxiaoguang/serverless-cost-calculator)
[![GitHub issues](https://img.shields.io/github/issues/sunxiaoguang/serverless-cost-calculator)](https://github.com/sunxiaoguang/serverless-cost-calculator/issues)
[![GitHub license](https://img.shields.io/github/license/sunxiaoguang/serverless-cost-calculator)](https://github.com/sunxiaoguang/serverless-cost-calculator/blob/main/LICENSE)

Estimate the cost of TiDB Serverless for your existing MySQL-compatible databases.

## Overview

The `serverless-cost-calculator` is an application that estimates the monthly cost of running existing workloads on MySQL-compatible databases using TiDB Serverless. It analyzes your current workload to provide a projected expense. As the tool is still in the early stages of development, its estimations may differ substantially in real-world scenarios. If you encounter any problems or unexpected estimations, please feel free to report them.

## Prerequisites

- Rust (https://www.rust-lang.org/tools/install)
- Cargo (Rust's package manager, included with Rust)
- MySQL-compatible database server (MySQL or TiDB)

## Building from source

You can use cargo to build everything:

```sh
cargo install serverless-cost-calculator
```

## Usage

After building the tool, you can run it using the following command:

```sh
serverless-cost-calculator --database <DATABASE> --region <REGION>
```

Where:
- `<DATABASE>` is the name of the database you want to estimate.
- `<REGION>` is the AWS region for the TiDB Serverless cluster.

You can also specify the host, port, user, password and analyze for your MySQL server using the respective flags.

### Example

```sh
serverless-cost-calculator --database mydb --host localhost --port 3306 --user root --password abcxyz --region us-east-1 --analyze
```

## Output

The tool will output an estimated monthly cost for your workload, broken down by request units and storage costs, and will display any relevant notes or warnings.

## Contributing

Contributions are welcome! For more information on how to contribute, please refer to our [CONTRIBUTING.md](CONTRIBUTING.md).

## License

This project is licensed under the [Apache-2.0 License](LICENSE).
