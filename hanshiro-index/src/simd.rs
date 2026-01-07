//! # SIMD-Accelerated Distance Functions
//!
//! High-performance vector distance calculations using CPU intrinsics.
//! Target: >15M distance calcs/sec for 768-dim vectors on a single core.
//!
//! ## Supported Operations
//! - Cosine similarity (normalized dot product)
//! - L2 distance (Euclidean)
//! - Dot product
//!
//! ## Implementation Strategy
//! 1. Detect CPU features at runtime
//! 2. Dispatch to fastest available: AVX-512 > AVX2 > SSE > Scalar

#[cfg(target_arch = "x86_64")]
use std::arch::x86_64::*;

#[cfg(target_arch = "aarch64")]
use std::arch::aarch64::*;

/// Distance metric types
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DistanceMetric {
    Cosine,
    L2,
    DotProduct,
}

/// Compute dot product of two vectors using best available SIMD
#[inline]
pub fn dot_product(a: &[f32], b: &[f32]) -> f32 {
    debug_assert_eq!(a.len(), b.len());
    
    #[cfg(target_arch = "x86_64")]
    {
        if is_x86_feature_detected!("avx2") && is_x86_feature_detected!("fma") {
            return unsafe { dot_product_avx2_fma(a, b) };
        }
        if is_x86_feature_detected!("avx") {
            return unsafe { dot_product_avx(a, b) };
        }
        if is_x86_feature_detected!("sse") {
            return unsafe { dot_product_sse(a, b) };
        }
    }
    
    #[cfg(target_arch = "aarch64")]
    {
        return unsafe { dot_product_neon(a, b) };
    }
    
    #[allow(unreachable_code)]
    dot_product_scalar(a, b)
}

/// Compute L2 (Euclidean) distance squared
#[inline]
pub fn l2_distance_squared(a: &[f32], b: &[f32]) -> f32 {
    debug_assert_eq!(a.len(), b.len());
    
    #[cfg(target_arch = "x86_64")]
    {
        if is_x86_feature_detected!("avx2") && is_x86_feature_detected!("fma") {
            return unsafe { l2_squared_avx2_fma(a, b) };
        }
        if is_x86_feature_detected!("avx") {
            return unsafe { l2_squared_avx(a, b) };
        }
    }
    
    #[cfg(target_arch = "aarch64")]
    {
        return unsafe { l2_squared_neon(a, b) };
    }
    
    #[allow(unreachable_code)]
    l2_squared_scalar(a, b)
}

/// Compute L2 distance (with sqrt)
#[inline]
pub fn l2_distance(a: &[f32], b: &[f32]) -> f32 {
    l2_distance_squared(a, b).sqrt()
}

/// Compute cosine similarity: dot(a,b) / (||a|| * ||b||)
/// For normalized vectors, this is just the dot product
#[inline]
pub fn cosine_similarity(a: &[f32], b: &[f32]) -> f32 {
    debug_assert_eq!(a.len(), b.len());
    
    #[cfg(target_arch = "x86_64")]
    {
        if is_x86_feature_detected!("avx2") && is_x86_feature_detected!("fma") {
            return unsafe { cosine_similarity_avx2_fma(a, b) };
        }
    }
    
    #[cfg(target_arch = "aarch64")]
    {
        return unsafe { cosine_similarity_neon(a, b) };
    }
    
    #[allow(unreachable_code)]
    cosine_similarity_scalar(a, b)
}

/// Compute cosine distance: 1 - cosine_similarity
#[inline]
pub fn cosine_distance(a: &[f32], b: &[f32]) -> f32 {
    1.0 - cosine_similarity(a, b)
}

/// Compute vector norm (magnitude)
#[inline]
pub fn norm(v: &[f32]) -> f32 {
    dot_product(v, v).sqrt()
}

/// Normalize vector in-place
#[inline]
pub fn normalize(v: &mut [f32]) {
    let n = norm(v);
    if n > 0.0 {
        let inv_n = 1.0 / n;
        for x in v.iter_mut() {
            *x *= inv_n;
        }
    }
}

/// Normalize vector, returning new vec
#[inline]
pub fn normalized(v: &[f32]) -> Vec<f32> {
    let mut result = v.to_vec();
    normalize(&mut result);
    result
}

// ============================================================================
// Scalar implementations (fallback)
// ============================================================================

#[inline]
fn dot_product_scalar(a: &[f32], b: &[f32]) -> f32 {
    a.iter().zip(b.iter()).map(|(x, y)| x * y).sum()
}

#[inline]
fn l2_squared_scalar(a: &[f32], b: &[f32]) -> f32 {
    a.iter().zip(b.iter()).map(|(x, y)| {
        let d = x - y;
        d * d
    }).sum()
}

#[inline]
fn cosine_similarity_scalar(a: &[f32], b: &[f32]) -> f32 {
    let mut dot = 0.0f32;
    let mut norm_a = 0.0f32;
    let mut norm_b = 0.0f32;
    
    for (x, y) in a.iter().zip(b.iter()) {
        dot += x * y;
        norm_a += x * x;
        norm_b += y * y;
    }
    
    let denom = (norm_a * norm_b).sqrt();
    if denom > 0.0 {
        dot / denom
    } else {
        0.0
    }
}

// ============================================================================
// AVX2 + FMA implementations (256-bit, 8 floats at a time)
// ============================================================================

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx2", enable = "fma")]
unsafe fn dot_product_avx2_fma(a: &[f32], b: &[f32]) -> f32 {
    let n = a.len();
    let chunks = n / 8;
    let remainder = n % 8;
    
    let mut sum = _mm256_setzero_ps();
    
    let a_ptr = a.as_ptr();
    let b_ptr = b.as_ptr();
    
    for i in 0..chunks {
        let offset = i * 8;
        let va = _mm256_loadu_ps(a_ptr.add(offset));
        let vb = _mm256_loadu_ps(b_ptr.add(offset));
        sum = _mm256_fmadd_ps(va, vb, sum);
    }
    
    // Horizontal sum
    let mut result = hsum256_ps(sum);
    
    // Handle remainder
    let base = chunks * 8;
    for i in 0..remainder {
        result += a[base + i] * b[base + i];
    }
    
    result
}

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx2", enable = "fma")]
unsafe fn l2_squared_avx2_fma(a: &[f32], b: &[f32]) -> f32 {
    let n = a.len();
    let chunks = n / 8;
    let remainder = n % 8;
    
    let mut sum = _mm256_setzero_ps();
    
    let a_ptr = a.as_ptr();
    let b_ptr = b.as_ptr();
    
    for i in 0..chunks {
        let offset = i * 8;
        let va = _mm256_loadu_ps(a_ptr.add(offset));
        let vb = _mm256_loadu_ps(b_ptr.add(offset));
        let diff = _mm256_sub_ps(va, vb);
        sum = _mm256_fmadd_ps(diff, diff, sum);
    }
    
    let mut result = hsum256_ps(sum);
    
    let base = chunks * 8;
    for i in 0..remainder {
        let d = a[base + i] - b[base + i];
        result += d * d;
    }
    
    result
}

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx2", enable = "fma")]
unsafe fn cosine_similarity_avx2_fma(a: &[f32], b: &[f32]) -> f32 {
    let n = a.len();
    let chunks = n / 8;
    let remainder = n % 8;
    
    let mut dot_sum = _mm256_setzero_ps();
    let mut norm_a_sum = _mm256_setzero_ps();
    let mut norm_b_sum = _mm256_setzero_ps();
    
    let a_ptr = a.as_ptr();
    let b_ptr = b.as_ptr();
    
    for i in 0..chunks {
        let offset = i * 8;
        let va = _mm256_loadu_ps(a_ptr.add(offset));
        let vb = _mm256_loadu_ps(b_ptr.add(offset));
        
        dot_sum = _mm256_fmadd_ps(va, vb, dot_sum);
        norm_a_sum = _mm256_fmadd_ps(va, va, norm_a_sum);
        norm_b_sum = _mm256_fmadd_ps(vb, vb, norm_b_sum);
    }
    
    let mut dot = hsum256_ps(dot_sum);
    let mut norm_a = hsum256_ps(norm_a_sum);
    let mut norm_b = hsum256_ps(norm_b_sum);
    
    let base = chunks * 8;
    for i in 0..remainder {
        let x = a[base + i];
        let y = b[base + i];
        dot += x * y;
        norm_a += x * x;
        norm_b += y * y;
    }
    
    let denom = (norm_a * norm_b).sqrt();
    if denom > 0.0 { dot / denom } else { 0.0 }
}

// ============================================================================
// AVX implementations (256-bit, no FMA)
// ============================================================================

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx")]
unsafe fn dot_product_avx(a: &[f32], b: &[f32]) -> f32 {
    let n = a.len();
    let chunks = n / 8;
    let remainder = n % 8;
    
    let mut sum = _mm256_setzero_ps();
    
    let a_ptr = a.as_ptr();
    let b_ptr = b.as_ptr();
    
    for i in 0..chunks {
        let offset = i * 8;
        let va = _mm256_loadu_ps(a_ptr.add(offset));
        let vb = _mm256_loadu_ps(b_ptr.add(offset));
        let prod = _mm256_mul_ps(va, vb);
        sum = _mm256_add_ps(sum, prod);
    }
    
    let mut result = hsum256_ps(sum);
    
    let base = chunks * 8;
    for i in 0..remainder {
        result += a[base + i] * b[base + i];
    }
    
    result
}

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx")]
unsafe fn l2_squared_avx(a: &[f32], b: &[f32]) -> f32 {
    let n = a.len();
    let chunks = n / 8;
    let remainder = n % 8;
    
    let mut sum = _mm256_setzero_ps();
    
    let a_ptr = a.as_ptr();
    let b_ptr = b.as_ptr();
    
    for i in 0..chunks {
        let offset = i * 8;
        let va = _mm256_loadu_ps(a_ptr.add(offset));
        let vb = _mm256_loadu_ps(b_ptr.add(offset));
        let diff = _mm256_sub_ps(va, vb);
        let sq = _mm256_mul_ps(diff, diff);
        sum = _mm256_add_ps(sum, sq);
    }
    
    let mut result = hsum256_ps(sum);
    
    let base = chunks * 8;
    for i in 0..remainder {
        let d = a[base + i] - b[base + i];
        result += d * d;
    }
    
    result
}

// ============================================================================
// SSE implementations (128-bit, 4 floats at a time)
// ============================================================================

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "sse")]
unsafe fn dot_product_sse(a: &[f32], b: &[f32]) -> f32 {
    let n = a.len();
    let chunks = n / 4;
    let remainder = n % 4;
    
    let mut sum = _mm_setzero_ps();
    
    let a_ptr = a.as_ptr();
    let b_ptr = b.as_ptr();
    
    for i in 0..chunks {
        let offset = i * 4;
        let va = _mm_loadu_ps(a_ptr.add(offset));
        let vb = _mm_loadu_ps(b_ptr.add(offset));
        let prod = _mm_mul_ps(va, vb);
        sum = _mm_add_ps(sum, prod);
    }
    
    let mut result = hsum128_ps(sum);
    
    let base = chunks * 4;
    for i in 0..remainder {
        result += a[base + i] * b[base + i];
    }
    
    result
}

// ============================================================================
// Helper functions for horizontal sums
// ============================================================================

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx")]
#[inline]
unsafe fn hsum256_ps(v: __m256) -> f32 {
    // Sum the high and low 128-bit lanes
    let high = _mm256_extractf128_ps(v, 1);
    let low = _mm256_castps256_ps128(v);
    let sum128 = _mm_add_ps(high, low);
    hsum128_ps(sum128)
}

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "sse")]
#[inline]
unsafe fn hsum128_ps(v: __m128) -> f32 {
    // Horizontal sum of 4 floats
    let shuf = _mm_movehdup_ps(v);        // [1,1,3,3]
    let sums = _mm_add_ps(v, shuf);       // [0+1,1+1,2+3,3+3]
    let shuf = _mm_movehl_ps(sums, sums); // [2+3,3+3,2+3,3+3]
    let sums = _mm_add_ss(sums, shuf);    // [0+1+2+3,...]
    _mm_cvtss_f32(sums)
}

// ============================================================================
// ARM NEON implementations (128-bit, 4 floats at a time)
// ============================================================================

#[cfg(target_arch = "aarch64")]
#[inline]
unsafe fn dot_product_neon(a: &[f32], b: &[f32]) -> f32 {
    let n = a.len();
    let chunks = n / 4;
    let remainder = n % 4;
    
    let mut sum = vdupq_n_f32(0.0);
    
    let a_ptr = a.as_ptr();
    let b_ptr = b.as_ptr();
    
    for i in 0..chunks {
        let offset = i * 4;
        let va = vld1q_f32(a_ptr.add(offset));
        let vb = vld1q_f32(b_ptr.add(offset));
        sum = vfmaq_f32(sum, va, vb);
    }
    
    let mut result = vaddvq_f32(sum);
    
    let base = chunks * 4;
    for i in 0..remainder {
        result += a[base + i] * b[base + i];
    }
    
    result
}

#[cfg(target_arch = "aarch64")]
#[inline]
unsafe fn l2_squared_neon(a: &[f32], b: &[f32]) -> f32 {
    let n = a.len();
    let chunks = n / 4;
    let remainder = n % 4;
    
    let mut sum = vdupq_n_f32(0.0);
    
    let a_ptr = a.as_ptr();
    let b_ptr = b.as_ptr();
    
    for i in 0..chunks {
        let offset = i * 4;
        let va = vld1q_f32(a_ptr.add(offset));
        let vb = vld1q_f32(b_ptr.add(offset));
        let diff = vsubq_f32(va, vb);
        sum = vfmaq_f32(sum, diff, diff);
    }
    
    let mut result = vaddvq_f32(sum);
    
    let base = chunks * 4;
    for i in 0..remainder {
        let d = a[base + i] - b[base + i];
        result += d * d;
    }
    
    result
}

#[cfg(target_arch = "aarch64")]
#[inline]
unsafe fn cosine_similarity_neon(a: &[f32], b: &[f32]) -> f32 {
    let n = a.len();
    let chunks = n / 4;
    let remainder = n % 4;
    
    let mut dot_sum = vdupq_n_f32(0.0);
    let mut norm_a_sum = vdupq_n_f32(0.0);
    let mut norm_b_sum = vdupq_n_f32(0.0);
    
    let a_ptr = a.as_ptr();
    let b_ptr = b.as_ptr();
    
    for i in 0..chunks {
        let offset = i * 4;
        let va = vld1q_f32(a_ptr.add(offset));
        let vb = vld1q_f32(b_ptr.add(offset));
        
        dot_sum = vfmaq_f32(dot_sum, va, vb);
        norm_a_sum = vfmaq_f32(norm_a_sum, va, va);
        norm_b_sum = vfmaq_f32(norm_b_sum, vb, vb);
    }
    
    let mut dot = vaddvq_f32(dot_sum);
    let mut norm_a = vaddvq_f32(norm_a_sum);
    let mut norm_b = vaddvq_f32(norm_b_sum);
    
    let base = chunks * 4;
    for i in 0..remainder {
        let x = a[base + i];
        let y = b[base + i];
        dot += x * y;
        norm_a += x * x;
        norm_b += y * y;
    }
    
    let denom = (norm_a * norm_b).sqrt();
    if denom > 0.0 { dot / denom } else { 0.0 }
}

// ============================================================================
// Quantized operations (SQ8 - Scalar Quantization to int8)
// ============================================================================

/// Quantize f32 vector to i8 (assumes values in [-1, 1] range)
pub fn quantize_sq8(v: &[f32]) -> Vec<i8> {
    v.iter().map(|&x| {
        let clamped = x.clamp(-1.0, 1.0);
        (clamped * 127.0).round() as i8
    }).collect()
}

/// Dequantize i8 vector back to f32
pub fn dequantize_sq8(v: &[i8]) -> Vec<f32> {
    v.iter().map(|&x| x as f32 / 127.0).collect()
}

/// Dot product of quantized vectors (returns approximate result)
#[inline]
pub fn dot_product_sq8(a: &[i8], b: &[i8]) -> i32 {
    debug_assert_eq!(a.len(), b.len());
    
    #[cfg(target_arch = "x86_64")]
    {
        if is_x86_feature_detected!("avx2") {
            return unsafe { dot_product_sq8_avx2(a, b) };
        }
    }
    
    #[cfg(target_arch = "aarch64")]
    {
        return unsafe { dot_product_sq8_neon(a, b) };
    }
    
    #[allow(unreachable_code)]
    dot_product_sq8_scalar(a, b)
}

#[inline]
fn dot_product_sq8_scalar(a: &[i8], b: &[i8]) -> i32 {
    a.iter().zip(b.iter()).map(|(&x, &y)| x as i32 * y as i32).sum()
}

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx2")]
unsafe fn dot_product_sq8_avx2(a: &[i8], b: &[i8]) -> i32 {
    let n = a.len();
    let chunks = n / 32;
    let remainder = n % 32;
    
    let mut sum = _mm256_setzero_si256();
    
    let a_ptr = a.as_ptr() as *const __m256i;
    let b_ptr = b.as_ptr() as *const __m256i;
    
    for i in 0..chunks {
        let va = _mm256_loadu_si256(a_ptr.add(i));
        let vb = _mm256_loadu_si256(b_ptr.add(i));
        
        // Use maddubs for unsigned*signed -> i16, then hadd to i32
        // Since we have signed*signed, we need a different approach
        // Convert to i16 first, then multiply
        let va_lo = _mm256_cvtepi8_epi16(_mm256_castsi256_si128(va));
        let vb_lo = _mm256_cvtepi8_epi16(_mm256_castsi256_si128(vb));
        let va_hi = _mm256_cvtepi8_epi16(_mm256_extracti128_si256(va, 1));
        let vb_hi = _mm256_cvtepi8_epi16(_mm256_extracti128_si256(vb, 1));
        
        let prod_lo = _mm256_madd_epi16(va_lo, vb_lo);
        let prod_hi = _mm256_madd_epi16(va_hi, vb_hi);
        
        sum = _mm256_add_epi32(sum, prod_lo);
        sum = _mm256_add_epi32(sum, prod_hi);
    }
    
    let mut result = hsum256_epi32(sum);
    
    let base = chunks * 32;
    for i in 0..remainder {
        result += a[base + i] as i32 * b[base + i] as i32;
    }
    
    result
}

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx2")]
#[inline]
unsafe fn hsum256_epi32(v: __m256i) -> i32 {
    let high = _mm256_extracti128_si256(v, 1);
    let low = _mm256_castsi256_si128(v);
    let sum128 = _mm_add_epi32(high, low);
    
    let shuf = _mm_shuffle_epi32(sum128, 0b11_10_11_10);
    let sums = _mm_add_epi32(sum128, shuf);
    let shuf = _mm_shuffle_epi32(sums, 0b00_00_00_01);
    let sums = _mm_add_epi32(sums, shuf);
    
    _mm_cvtsi128_si32(sums)
}

#[cfg(target_arch = "aarch64")]
#[inline]
unsafe fn dot_product_sq8_neon(a: &[i8], b: &[i8]) -> i32 {
    let n = a.len();
    let chunks = n / 16;
    let remainder = n % 16;
    
    let mut sum = vdupq_n_s32(0);
    
    let a_ptr = a.as_ptr();
    let b_ptr = b.as_ptr();
    
    for i in 0..chunks {
        let offset = i * 16;
        let va = vld1q_s8(a_ptr.add(offset));
        let vb = vld1q_s8(b_ptr.add(offset));
        
        // Widen to i16 and multiply
        let va_lo = vmovl_s8(vget_low_s8(va));
        let vb_lo = vmovl_s8(vget_low_s8(vb));
        let va_hi = vmovl_s8(vget_high_s8(va));
        let vb_hi = vmovl_s8(vget_high_s8(vb));
        
        let prod_lo = vmull_s16(vget_low_s16(va_lo), vget_low_s16(vb_lo));
        let prod_lo2 = vmull_s16(vget_high_s16(va_lo), vget_high_s16(vb_lo));
        let prod_hi = vmull_s16(vget_low_s16(va_hi), vget_low_s16(vb_hi));
        let prod_hi2 = vmull_s16(vget_high_s16(va_hi), vget_high_s16(vb_hi));
        
        sum = vaddq_s32(sum, prod_lo);
        sum = vaddq_s32(sum, prod_lo2);
        sum = vaddq_s32(sum, prod_hi);
        sum = vaddq_s32(sum, prod_hi2);
    }
    
    let mut result = vaddvq_s32(sum);
    
    let base = chunks * 16;
    for i in 0..remainder {
        result += a[base + i] as i32 * b[base + i] as i32;
    }
    
    result
}
