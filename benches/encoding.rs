use criterion::{criterion_group, criterion_main, Criterion};

fn encoding_benchmarks(_c: &mut Criterion) {
    // TODO: Add encoder/decoder throughput benchmarks per algorithm
}

criterion_group!(benches, encoding_benchmarks);
criterion_main!(benches);
