use arrow_array::{Float32Array, Int32Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};
use parquet_56::file::metadata::ParquetMetaDataReader as Parquet56DataReader;
use parquet_56::format::FileMetaData as Parquet56FileMetaData;
use parquet_57::arrow::arrow_writer::ArrowWriter;
use parquet_57::errors::{ParquetError, Result};
use parquet_57::file::metadata::ParquetMetaDataReader as Parquet57DataReader;
use std::{
    fs::File,
    io::{Read, Seek, SeekFrom},
    sync::Arc,
    thread,
    time::{Duration, Instant},
};
use tempfile::tempdir;

const FOOTER_SIZE: u64 = 8;
const PARQUET_MAGIC: &[u8; 4] = b"PAR1";

fn time_method<F, T>(mut f: F) -> (T, Duration)
where
    F: FnMut() -> T,
{
    let mut total_duration = Duration::ZERO;
    let mut result: Option<T> = None;

    for _ in 0..10 {
        let start = Instant::now();
        let res = f();
        let elapsed = start.elapsed();
        total_duration += elapsed;
        result = Some(res);
    }

    (result.unwrap(), total_duration / 10)
}

fn get_parquet_serialized_metadata(filepath: &str) -> Result<Vec<u8>> {
    let mut file = File::open(filepath)?;

    let file_len = file.metadata()?.len();
    if file_len < FOOTER_SIZE {
        return Err(ParquetError::General(
            "File too small to be a valid parquet file".into(),
        ));
    }

    file.seek(SeekFrom::End(-(FOOTER_SIZE as i64)))?;
    let mut footer = [0u8; FOOTER_SIZE as usize];
    file.read_exact(&mut footer)?;

    if &footer[4..] != PARQUET_MAGIC {
        return Err(ParquetError::General("Invalid parquet magic bytes".into()));
    }

    let metadata_len = u32::from_le_bytes([footer[0], footer[1], footer[2], footer[3]]) as u64;

    if metadata_len + FOOTER_SIZE > file_len {
        return Err(ParquetError::General(
            "Parquet metadata length is invalid".into(),
        ));
    }

    let metadata_start = file_len - FOOTER_SIZE - metadata_len;
    file.seek(SeekFrom::Start(metadata_start))?;

    let mut buf = vec![0u8; metadata_len as usize];
    file.read_exact(&mut buf)?;

    Ok(buf)
}

fn deserialize_parquet_metadata(bytes: &[u8]) -> Parquet56FileMetaData {
    use parquet_56::thrift::TSerializable;
    use thrift::protocol::TCompactInputProtocol;
    use thrift::transport::TBufferChannel;

    let mut chan = TBufferChannel::with_capacity(bytes.len(), 0);
    chan.set_readable_bytes(bytes);
    let mut proto = TCompactInputProtocol::new(chan);
    Parquet56FileMetaData::read_from_in_protocol(&mut proto).unwrap()
}

fn generate_dummy_record_batch(schema: &Schema) -> RecordBatch {
    let columns = schema
        .fields()
        .iter()
        .map(|field| match field.data_type() {
            DataType::Float32 => {
                let array = Float32Array::from(vec![Some(42.0); 20]);
                Arc::new(array) as Arc<dyn arrow_array::Array>
            }
            DataType::Utf8 => {
                let array = StringArray::from(vec![Some("dummy"); 20]);
                Arc::new(array) as Arc<dyn arrow_array::Array>
            }
            DataType::Int32 => {
                let array = Int32Array::from(vec![Some(42); 20]);
                Arc::new(array) as Arc<dyn arrow_array::Array>
            }
            _ => panic!("Unsupported datatype in generate_dummy_record_batch"),
        })
        .collect::<Vec<_>>();

    RecordBatch::try_new(Arc::new(schema.clone()), columns).unwrap()
}

fn generate_column_schema(fields_num: u32, data_type: DataType) -> Schema {
    let fields: Vec<Field> = (0..fields_num)
        .map(|i| Field::new(&format!("col_{i}"), data_type.clone(), true))
        .collect();

    Schema::new(fields)
}

fn test_write_read_parquet_col(
    fields_num: u32,
    data_type: DataType,
) -> Result<(Duration, Duration, Duration)> {
    let schema = generate_column_schema(fields_num, data_type);
    let batch = generate_dummy_record_batch(&schema);

    let tmp_dir = tempdir().unwrap();
    let parquet_path = tmp_dir.path().join("test_100_col.parquet");

    // Write parquet file
    {
        let file = File::create(&parquet_path).unwrap();
        let mut writer = ArrowWriter::try_new(file, Arc::new(schema.clone()), None).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();
    }

    let buf = get_parquet_serialized_metadata(parquet_path.to_str().unwrap()).unwrap();

    // Approach 1: Deserialize with thrift
    let (file_md_1, avg_time_1) = time_method(|| deserialize_parquet_metadata(&buf));
    println!("Approach 1: using custom thrift to parse metadata");
    println!("Approach 1: num_rows = {:?}", file_md_1.num_rows);
    println!(
        "Approach 1: number of columns in first row group = {:?}",
        file_md_1.row_groups[0].columns.len()
    );
    println!(
        "Approach 1: average time elapsed over 10 runs: {:?}",
        avg_time_1
    );
    thread::sleep(Duration::from_secs(5));

    // Approach 2: parquet_56 decode_metadata
    let (file_md_2, avg_time_2) = time_method(|| {
        Parquet56DataReader::decode_metadata(&buf).expect("Failed to decode metadata")
    });
    println!(
        "Approach 2: using decode_metadata built-in function in ParquetDataReader version 56.0.0 to parse metadata"
    );
    println!(
        "Approach 2: num_rows = {:?}",
        file_md_2.file_metadata().num_rows()
    );
    println!(
        "Approach 2: num_row_groups = {:?}",
        file_md_2.num_row_groups()
    );
    println!(
        "Approach 2: average time elapsed over 10 runs: {:?}",
        avg_time_2
    );
    thread::sleep(Duration::from_secs(5));

    // Approach 3: parquet_57 decode_metadata
    let (file_md_3, avg_time_3) = time_method(|| {
        Parquet57DataReader::decode_metadata(&buf).expect("Failed to decode metadata")
    });
    println!(
        "Approach 3: using decode_metadata built-in function in ParquetDataReader version 57.0.0 to parse metadata"
    );
    println!(
        "Approach 3: num_rows = {:?}",
        file_md_3.file_metadata().num_rows()
    );
    println!(
        "Approach 3: num_row_groups = {:?}",
        file_md_3.num_row_groups()
    );
    println!(
        "Approach 3: average time elapsed over 10 runs: {:?}",
        avg_time_3
    );

    thread::sleep(Duration::from_secs(5));
    Ok((avg_time_1, avg_time_2, avg_time_3))
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let field_counts = [100, 1000, 10_000, 100_000];

    let mut wtr = csv::Writer::from_path("src/result/parquet_metadata_benchmark.csv")?;
    wtr.write_record(&[
        "fields_num",
        "data_type",
        "avg_time_moonlink_thrift_custom_s",
        "avg_time_parquet56_s",
        "avg_time_parquet57_s",
    ])?;

    let data_types = [DataType::Int32, DataType::Float32, DataType::Utf8];

    for &fields_num in &field_counts {
        for data_type in data_types.iter() {
            println!(
                "Testing with {} columns and data type {:?}...",
                fields_num, data_type
            );
            let (avg_time_1, avg_time_2, avg_time_3) =
                test_write_read_parquet_col(fields_num, data_type.clone())?;
            wtr.write_record(&[
                fields_num.to_string(),
                format!("{:?}", data_type),
                avg_time_1.as_secs_f64().to_string(),
                avg_time_2.as_secs_f64().to_string(),
                avg_time_3.as_secs_f64().to_string(),
            ])?;
            wtr.flush()?;
            println!(
                "Completed for {} columns {:?}: Thrift {:.3}s, Parquet56 {:.3}s, Parquet57 {:.3}s",
                fields_num,
                data_type,
                avg_time_1.as_secs_f64(),
                avg_time_2.as_secs_f64(),
                avg_time_3.as_secs_f64()
            );
            thread::sleep(Duration::from_secs(0));
        }
    }

    println!("Benchmark complete. Results exported to parquet_metadata_benchmark.csv");
    Ok(())
}
