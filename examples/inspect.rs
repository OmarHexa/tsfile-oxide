// Inspect a .tsfile: print bloom filter + table schema summary, list
// devices, per-device measurements with their data type, chunk count,
// aggregate point count, and time range. Closes with overall totals.
//
// Usage: cargo run --example inspect -- <path-to-tsfile>
//         (defaults to examples/benchmark.tsfile)

use std::env;
use std::path::PathBuf;
use std::time::Instant;

use tsfile_oxide::io::io_reader::TsFileIOReader;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<String> = env::args().collect();
    let path: PathBuf = args
        .get(1)
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("examples/benchmark.tsfile"));

    let file_size = std::fs::metadata(&path)?.len();
    let mb = file_size as f64 / 1024.0 / 1024.0;
    println!("File: {}", path.display());
    println!("Size: {file_size} bytes ({mb:.2} MB)");
    println!();

    let open_start = Instant::now();
    let mut io = TsFileIOReader::open(&path)?;
    let open_ms = open_start.elapsed().as_millis();
    println!("Opened in {open_ms} ms");

    match io.ts_file_meta.bloom_filter.as_ref() {
        Some(bytes) => println!("Bloom filter: {} bytes", bytes.len()),
        None => println!("Bloom filter: (none)"),
    }

    let table_schemas = &io.ts_file_meta.table_schema_map;
    println!("Table schemas: {}", table_schemas.len());
    for ts in table_schemas {
        println!("  - {}", ts.table_name);
    }
    println!();

    let index_start = Instant::now();
    let devices = io.all_devices()?;
    let index_ms = index_start.elapsed().as_millis();
    println!(
        "Loaded index for {} device(s) in {index_ms} ms",
        devices.len()
    );
    println!();

    let mut total_measurements = 0usize;
    let mut total_chunks = 0usize;
    let mut total_points: u64 = 0;
    let mut global_min_time = i64::MAX;
    let mut global_max_time = i64::MIN;
    let mut per_type_chunks: std::collections::BTreeMap<String, usize> =
        std::collections::BTreeMap::new();

    for device in &devices {
        println!("Device: {device}");
        let ts_map = io.get_timeseries_indexes(device)?.clone();

        // Sort by measurement name for stable output; empty-named entry
        // (aligned time column) sorts first.
        let mut entries: Vec<_> = ts_map.iter().collect();
        entries.sort_by_key(|(name, _)| (*name).clone());

        for (measurement, ts_index) in entries {
            let stat = &ts_index.statistic;
            let count = stat.count();
            let start = stat.start_time();
            let end = stat.end_time();
            let n_chunks = ts_index.chunk_meta_list.len();
            total_measurements += 1;
            total_chunks += n_chunks;
            total_points += count;
            if count > 0 {
                global_min_time = global_min_time.min(start);
                global_max_time = global_max_time.max(end);
            }
            *per_type_chunks
                .entry(format!("{:?}", ts_index.data_type))
                .or_insert(0) += n_chunks;

            let label = if measurement.is_empty() {
                "<aligned-time>"
            } else {
                measurement
            };
            println!(
                "  {label:<28} type={:?}  chunks={n_chunks:>3}  points={count:>10}  time=[{start}, {end}]",
                ts_index.data_type,
            );
        }
    }
    println!();

    println!("Totals");
    println!("  Devices:      {}", devices.len());
    println!("  Measurements: {total_measurements}");
    println!("  Chunks:       {total_chunks}");
    println!("  Points:       {total_points}");
    if global_min_time <= global_max_time {
        println!("  Time range:   [{global_min_time}, {global_max_time}]");
    }
    println!("  Chunks by data type:");
    for (dt, n) in &per_type_chunks {
        println!("    {dt:<10} {n}");
    }

    Ok(())
}
