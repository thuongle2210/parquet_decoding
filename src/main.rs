use std::{
    fs::File,
    io::{Read, Seek, SeekFrom},
    sync::Arc,
    thread,
    time::{Duration, Instant},
 };
 
 use arrow_array::{Int32Array, RecordBatch};
 use arrow_schema::{DataType, Field, Schema};
 use parquet_56::file::metadata::ParquetMetaDataReader as Parquet56DataReader;
 use parquet_56::format::FileMetaData as Parquet56FileMetaData;
 use parquet_57::file::metadata::ParquetMetaDataReader as Parquet57DataReader;
 use parquet_57::arrow::arrow_writer::ArrowWriter;
 use parquet_57::errors::{ParquetError, Result};
 use tempfile::tempdir;
 
 const FOOTER_SIZE: u64 = 8;
 const PARQUET_MAGIC: &[u8; 4] = b"PAR1";
 
 fn get_parquet_serialized_metadata(filepath: &str) -> Result<Vec<u8>> {
    let mut file = File::open(filepath)?;
 
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
 
 fn deserialize_parquet_metadata(bytes: &[u8]) -> Parquet56FileMetaData {
    use parquet_56::thrift::TSerializable;
    use thrift::protocol::TCompactInputProtocol;
    use thrift::transport::TBufferChannel;
 
    let mut chan = TBufferChannel::with_capacity(bytes.len(), 0);
    chan.set_readable_bytes(bytes);
    let mut proto = TCompactInputProtocol::new(chan);
    Parquet56FileMetaData::read_from_in_protocol(&mut proto).unwrap()
 }
 
 fn generate_column_schema(fields_num: u32) -> Schema {
    let fields: Vec<Field> = (0..fields_num)
        .map(|i| Field::new(&format!("col_{i}"), DataType::Int32, true))
        .collect();
 
    Schema::new(fields)
 }
 
 fn generate_dummy_record_batch(schema: &Schema) -> RecordBatch {
    let columns = schema
        .fields()
        .iter()
        .map(|_| Arc::new(Int32Array::from(vec![Some(42)])) as Arc<dyn arrow_array::Array>)
        .collect::<Vec<_>>();
    RecordBatch::try_new(Arc::new(schema.clone()), columns).unwrap()
 }
 
 fn test_write_read_parquet_100_col() {
    let schema = generate_column_schema(300_000);let batch = generate_dummy_record_batch(&schema);
 
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
    let start = Instant::now();
    let file_md_1 = deserialize_parquet_metadata(&buf);
    println!("Approach 1: using custom thrift to parse metadata");
    println!("Approach 1: num_rows = {:?}", file_md_1.num_rows);
    println!(
        "Approach 1: number of columns in first row group = {:?}",
        file_md_1.row_groups[0].columns.len()
    );
    println!("Time elapsed: {:?}", start.elapsed());
    thread::sleep(Duration::from_secs(5));
 
    // Approach 2: parquet_56 decode_metadata
    let start = Instant::now();
    let file_md_2 =
        Parquet56DataReader::decode_metadata(&buf).expect("Failed to decode metadata");
    println!("Approach 2: using decode_metadata built-in function in ParquetDataReader version 56.0.0 to parse metadata");
    println!("Approach 2: num_rows = {:?}", file_md_2.file_metadata().num_rows());
    println!("Approach 2: num_row_groups = {:?}", file_md_2.num_row_groups());
    println!("Time elapsed: {:?}", start.elapsed());
    thread::sleep(Duration::from_secs(5));
 
    // Approach 3: parquet_57 decode_metadata
    let start = Instant::now();
    let file_md_3 =
        Parquet57DataReader::decode_metadata(&buf).expect("Failed to decode metadata");
    println!("Approach 3: using decode_metadata built-in function in ParquetDataReader version 57.0.0 to parse metadata");
    println!("Approach 3: num_rows = {:?}", file_md_3.file_metadata().num_rows());
    println!("Approach 3: num_row_groups = {:?}", file_md_3.num_row_groups());
    println!("Time elapsed: {:?}", start.elapsed());
    thread::sleep(Duration::from_secs(5));
 }
 
 fn main() {
    test_write_read_parquet_100_col();
 }