
## Overview
`parquet_decoding` is a Rust implementation focused on efficient decoding and parsing of Apache Parquet metadata and data files. The project aims to provide high-performance tools for reading Parquet files, including deserialization of metadata and access to column statistics.

This library can be leveraged for building data processing applications that require working with Parquet format, such as analytics platforms and data pipelines.

Inspired by the new release of the [Apache Arrow Parquet Rust crate](https://arrow.apache.org/blog/2025/10/23/rust-parquet-metadata/), which introduced a custom Thrift parser that decodes Parquet metadata 3x–9x faster than earlier versions, I implemented a benchmark comparing Moonlink’s custom parser [Moonlink parquet_utils.rs](https://github.com/Mooncake-Labs/moonlink/blob/main/src/moonlink_backend/src/parquet_utils.rs)  with the built-in decode_metadata function of Parquet (versions 56.x and 57.0.0).
The experiment showed that the built-in decode_metadata function in Parquet version 57.0.0 indeed outperformed both version 56.0.0 and Moonlink’s custom Thrift parser implementation in terms of speed, confirming the significant performance improvements made in the latest release.

## Performance Comparision
![alt text](parquet_decoding_benchmark_plot/result/speedup_comparison.png)


## Detailed Performance Comparision:

![alt text](parquet_decoding/parquet_decoding_benchmark_plotting/README.MD)

## Project Set-up step by step
There are 2 sub-project
1. benchmark processing which are written by RUST
![re-produce](parquet_decoding/parquet_decoding_benchmark_processing/README.MD)

2. benchmark plotting by python
![re-produce](parquet_decoding/parquet_decoding_benchmark_plotting/README.MD)




