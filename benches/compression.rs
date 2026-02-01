use criterion::{criterion_group, criterion_main, Criterion};

fn compression_benchmarks(_c: &mut Criterion) {
    // TODO: Add compress/decompress throughput benchmarks
}

criterion_group!(benches, compression_benchmarks);
criterion_main!(benches);
