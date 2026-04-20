// Write a small sample .tsfile using the crate's own writer.
// Used to exercise examples/inspect.rs without needing a C++-produced file.
//
// Usage: cargo run --example make_sample --release -- <out.tsfile>
//         (defaults to /tmp/sample.tsfile)

use std::env;
use std::path::PathBuf;
use std::sync::Arc;

use tsfile_oxide::config::Config;
use tsfile_oxide::device_id::DeviceId;
use tsfile_oxide::schema::MeasurementSchema;
use tsfile_oxide::tablet::Tablet;
use tsfile_oxide::types::{CompressionType, TSDataType, TSEncoding};
use tsfile_oxide::writer::tsfile_writer::TsFileWriter;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<String> = env::args().collect();
    let out: PathBuf = args
        .get(1)
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("/tmp/sample.tsfile"));

    let cfg = Arc::new(Config::default());
    let mut w = TsFileWriter::new(&out, cfg)?;

    // Two devices, each with two non-aligned int64 measurements.
    for dev_name in &["root.sg.d1", "root.sg.d2"] {
        let device = DeviceId::parse(dev_name).unwrap();
        let m_temp = MeasurementSchema::new(
            "temperature".into(),
            TSDataType::Int64,
            TSEncoding::Ts2Diff,
            CompressionType::Snappy,
        );
        let m_hum = MeasurementSchema::new(
            "humidity".into(),
            TSDataType::Double,
            TSEncoding::Gorilla,
            CompressionType::Lz4,
        );

        // Write 200 rows per measurement in two separate tablets (one
        // measurement each) with a flush between so each ends up in its
        // own chunk.
        let n = 200usize;
        let mut t_temp = Tablet::new(device.to_string(), vec![m_temp], n);
        for i in 0..n {
            t_temp.add_timestamp(i, i as i64)?;
            t_temp.add_value_i64(i, 0, 20 + (i as i64 % 10))?;
        }
        w.write_tablet(&t_temp)?;
        w.flush()?;

        let mut t_hum = Tablet::new(device.to_string(), vec![m_hum], n);
        for i in 0..n {
            t_hum.add_timestamp(i, i as i64)?;
            t_hum.add_value_f64(i, 0, 0.5 + (i as f64) * 0.01)?;
        }
        w.write_tablet(&t_hum)?;
    }

    w.close()?;

    let size = std::fs::metadata(&out)?.len();
    println!("Wrote {} ({} bytes)", out.display(), size);
    Ok(())
}
