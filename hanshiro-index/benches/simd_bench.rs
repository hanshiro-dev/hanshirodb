//! SIMD Distance Calculation Benchmarks
//!
//! Target: >15M distance calcs/sec for 768-dim vectors on single core
//!
//! Run with: cargo bench --package hanshiro-index

use criterion::{black_box, criterion_group, criterion_main, Criterion, Throughput};
use rand::Rng;

fn random_vec(dim: usize) -> Vec<f32> {
    let mut rng = rand::thread_rng();
    (0..dim).map(|_| rng.gen_range(-1.0..1.0)).collect()
}

fn random_vec_i8(dim: usize) -> Vec<i8> {
    let mut rng = rand::thread_rng();
    (0..dim).map(|_| rng.gen_range(-127..127)).collect()
}

fn bench_dot_product(c: &mut Criterion) {
    let dims = [128, 384, 768, 1536];
    
    let mut group = c.benchmark_group("dot_product");
    
    for dim in dims {
        let a = random_vec(dim);
        let b = random_vec(dim);
        
        group.throughput(Throughput::Elements(1));
        group.bench_function(format!("dim_{}", dim), |bencher| {
            bencher.iter(|| {
                black_box(hanshiro_index::simd::dot_product(
                    black_box(&a),
                    black_box(&b),
                ))
            })
        });
    }
    
    group.finish();
}

fn bench_l2_distance(c: &mut Criterion) {
    let dims = [128, 384, 768, 1536];
    
    let mut group = c.benchmark_group("l2_distance");
    
    for dim in dims {
        let a = random_vec(dim);
        let b = random_vec(dim);
        
        group.throughput(Throughput::Elements(1));
        group.bench_function(format!("dim_{}", dim), |bencher| {
            bencher.iter(|| {
                black_box(hanshiro_index::simd::l2_distance_squared(
                    black_box(&a),
                    black_box(&b),
                ))
            })
        });
    }
    
    group.finish();
}

fn bench_cosine_similarity(c: &mut Criterion) {
    let dims = [128, 384, 768, 1536];
    
    let mut group = c.benchmark_group("cosine_similarity");
    
    for dim in dims {
        let a = random_vec(dim);
        let b = random_vec(dim);
        
        group.throughput(Throughput::Elements(1));
        group.bench_function(format!("dim_{}", dim), |bencher| {
            bencher.iter(|| {
                black_box(hanshiro_index::simd::cosine_similarity(
                    black_box(&a),
                    black_box(&b),
                ))
            })
        });
    }
    
    group.finish();
}

fn bench_quantized_dot_product(c: &mut Criterion) {
    let dims = [128, 384, 768, 1536];
    
    let mut group = c.benchmark_group("dot_product_sq8");
    
    for dim in dims {
        let a = random_vec_i8(dim);
        let b = random_vec_i8(dim);
        
        group.throughput(Throughput::Elements(1));
        group.bench_function(format!("dim_{}", dim), |bencher| {
            bencher.iter(|| {
                black_box(hanshiro_index::simd::dot_product_sq8(
                    black_box(&a),
                    black_box(&b),
                ))
            })
        });
    }
    
    group.finish();
}

fn bench_throughput_768(c: &mut Criterion) {
    // Specific benchmark for 768-dim to verify >15M/sec target
    let dim = 768;
    let a = random_vec(dim);
    let b = random_vec(dim);
    
    let mut group = c.benchmark_group("throughput_768dim");
    group.throughput(Throughput::Elements(1_000_000)); // Report as M ops
    
    group.bench_function("dot_product_1M", |bencher| {
        bencher.iter(|| {
            for _ in 0..1_000_000 {
                black_box(hanshiro_index::simd::dot_product(
                    black_box(&a),
                    black_box(&b),
                ));
            }
        })
    });
    
    group.bench_function("cosine_similarity_1M", |bencher| {
        bencher.iter(|| {
            for _ in 0..1_000_000 {
                black_box(hanshiro_index::simd::cosine_similarity(
                    black_box(&a),
                    black_box(&b),
                ));
            }
        })
    });
    
    // Quantized version
    let a_q = hanshiro_index::simd::quantize_sq8(&a);
    let b_q = hanshiro_index::simd::quantize_sq8(&b);
    
    group.bench_function("dot_product_sq8_1M", |bencher| {
        bencher.iter(|| {
            for _ in 0..1_000_000 {
                black_box(hanshiro_index::simd::dot_product_sq8(
                    black_box(&a_q),
                    black_box(&b_q),
                ));
            }
        })
    });
    
    group.finish();
}

fn bench_flat_index_search(c: &mut Criterion) {
    use hanshiro_index::flat::FlatIndex;
    use hanshiro_index::traits::{IndexConfig, VectorIndex};
    use hanshiro_index::simd::DistanceMetric;
    
    let mut group = c.benchmark_group("flat_index_search");
    
    for n in [1000, 10000] {
        let config = IndexConfig {
            dimension: 768,
            metric: DistanceMetric::Cosine,
            quantized: false,
        };
        let index = FlatIndex::with_capacity(config, n);
        
        // Populate
        for i in 0..n {
            index.insert(i as u64, &random_vec(768)).unwrap();
        }
        
        let query = random_vec(768);
        
        group.throughput(Throughput::Elements(n as u64));
        group.bench_function(format!("n_{}_k_10", n), |bencher| {
            bencher.iter(|| {
                black_box(index.search(black_box(&query), 10))
            })
        });
    }
    
    group.finish();
}

criterion_group!(
    benches,
    bench_dot_product,
    bench_l2_distance,
    bench_cosine_similarity,
    bench_quantized_dot_product,
    bench_throughput_768,
    bench_flat_index_search,
);

criterion_main!(benches);
