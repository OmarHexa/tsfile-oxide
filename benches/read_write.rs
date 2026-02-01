use criterion::{criterion_group, criterion_main, Criterion};

fn read_write_benchmarks(_c: &mut Criterion) {
    // TODO: Add full pipeline read/write throughput benchmarks
}

criterion_group!(benches, read_write_benchmarks);
criterion_main!(benches);
