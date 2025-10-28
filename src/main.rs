
#[cfg(test)]
use std::io::Seek;
use std::fs::File;
/// Parquet file footer size.
const FOOTER_SIZE: u64 = 8;
/// Parquet file magic bytes ("PAR1").
const PARQUET_MAGIC: &[u8; 4] = b"PAR1";

use parquet::errors::{Result, ParquetError};
use std::io::{Seek, SeekFrom, Read};


/// Get serialized uncompressed parquet metadata from the given local filepath.
/// TODO(hjiang): Currently it only supports local filepath.
fn get_parquet_serialized_metadata(filepath: &str) -> Result<Vec<u8>> {
    let mut file = File::open(filepath)?; // unwrap file or propagate error

    let file_len = file.metadata()?.len();
    if file_len < FOOTER_SIZE {
        return Err(ParquetError::General("File too small to be a valid parquet file".into()));
    }

    file.seek(SeekFrom::End(-(FOOTER_SIZE as i64)))?;
    let mut footer = [0u8; FOOTER_SIZE as usize];
    file.read_exact(&mut footer)?;

    if &footer[4..] != PARQUET_MAGIC {
        return Err(ParquetError::General("Invalid parquet magic bytes".into()));
    }

    let metadata_len = u32::from_le_bytes([footer[0], footer[1], footer[2], footer[3]]) as u64;

    if metadata_len + FOOTER_SIZE > file_len {
        return Err(ParquetError::General("Parquet metadata length is invalid".into()));
    }

    let metadata_start = file_len - FOOTER_SIZE - metadata_len;
    file.seek(SeekFrom::Start(metadata_start))?;

    let mut buf = vec![0u8; metadata_len as usize];
    file.read_exact(&mut buf)?;

    Ok(buf)
}
// #[cfg(test)]
// pub(crate) fn deserialize_parquet_metadata(bytes: &[u8]) -> FileMetaData {
//     use parquet::thrift::TSerializable;
//     use thrift::protocol::TCompactInputProtocol;
//     use thrift::transport::TBufferChannel;

//     let mut chan = TBufferChannel::with_capacity(bytes.len(), /*write_capacity=*/ 0);
//     chan.set_readable_bytes(bytes);
//     let mut proto = TCompactInputProtocol::new(chan);
//     FileMetaData::read_from_in_protocol(&mut proto).unwrap()
// }
use parquet::file::metadata::{ParquetMetaDataReader};
// fn deserialize_parquet_metadata_thuong(bytes: &[u8]) -> Result<ParquetMetadata> {
    
//     metadata
// }


use std::fs::File as StdFile;

use arrow_array::{Int32Array, RecordBatch};
use arrow_schema::{DataType, Field, Schema};
use parquet::arrow::arrow_writer::ArrowWriter;
use parquet::format::{FileMetaData, Statistics};
use tempfile::tempdir;
use std::sync::Arc;

// Util function to get min and max.
fn stats_min_max_i32(stats: &Statistics) -> Option<(i32, i32)> {
    let min_bytes = stats.min_value.as_ref().or(stats.min.as_ref())?;
    let max_bytes = stats.max_value.as_ref().or(stats.max.as_ref())?;

    if min_bytes.len() != 4 || max_bytes.len() != 4 {
        return None;
    }
    let min = i32::from_le_bytes([min_bytes[0], min_bytes[1], min_bytes[2], min_bytes[3]]);
    let max = i32::from_le_bytes([max_bytes[0], max_bytes[1], max_bytes[2], max_bytes[3]]);
    Some((min, max))
}


// Generate schema with 100 Int32 nullable columns.


fn test_write_read_parquet_100_col() {
    use std::time::Instant;
    let start = Instant::now();
    let schema = generate_100_column_schema();
    let batch = generate_dummy_record_batch(&schema);

    let tmp_dir = tempdir().unwrap();
    let parquet_path = format!("{}/test_100_col.parquet", tmp_dir.path().to_str().unwrap());

    // Write parquet file
    {
        let file = StdFile::create(&parquet_path).unwrap();
        let mut writer = ArrowWriter::try_new(file, Arc::new(schema.clone()), None).unwrap();
        writer.write(&batch).unwrap();
        let _ = writer.close().unwrap();
    }

    // Read serialized metadata bytes from parquet file
    let buf = get_parquet_serialized_metadata(&parquet_path).unwrap();

    // Deserialize parquet metadata from bytes
    let file_md = ParquetMetaDataReader::decode_metadata(&buf[..]).expect("Failed to decode metadata");

    // Access num_rows from file_metadata
    println!("num rows: {:?}", file_md.file_metadata().num_rows());

    // Access row_groups which returns a slice
    println!("num_row_groups: {:?}", file_md.num_row_groups());
    println!("row_groups: {:?}", file_md.row_groups()[0].num_columns());

    // Basic validation
    // assert_eq!(file_md.num_rows, 1);
    // assert_eq!(file_md.row_groups.len(), 1);
    // let rg = &file_md.row_groups[0];
    // assert_eq!(rg.columns.len(), 10);
    let duration = start.elapsed();
    println!("Time elapsed: {:?}", duration);
}



use tokio;

fn main() {
    test_write_read_parquet_100_col();
}

fn generate_100_column_schema() -> Schema {
    let fields: Vec<Field> = (0..1000000)
        .map(|i| Field::new(&format!("col_{i}"), DataType::Int32, true))
        .collect();
    Schema::new(fields)
}

// Generate dummy RecordBatch with one row for given schema.
fn generate_dummy_record_batch(schema: &Schema) -> RecordBatch {
    let columns = schema.fields().iter()
        .map(|_| {
            Arc::new(Int32Array::from(vec![Some(42)])) as Arc<dyn arrow_array::Array>
        })
        .collect::<Vec<_>>();
    RecordBatch::try_new(Arc::new(schema.clone()), columns).unwrap()
}