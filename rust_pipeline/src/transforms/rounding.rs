/// Round like Spark's `round(col, scale)` on a DoubleType column.
///
/// Spark converts the double to a decimal via its shortest string form
/// (`BigDecimal(d)`) and rounds HALF_UP, so `round(1.005, 2) == 1.01`.
/// Naive `(x * 100).round() / 100` gives 1.0 because 1.005 * 100 is
/// 100.49999…; this matches Spark instead.
pub fn spark_round(x: f64, scale: impl Into<i64>) -> f64 {
    if !x.is_finite() {
        return x;
    }
    let repr = format!("{}", x.abs());
    let (int_part, frac_part) = repr.split_once('.').unwrap_or((repr.as_str(), ""));
    let scale = scale.into();
    if scale >= frac_part.len() as i64 {
        return x;
    }
    let digits = format!("{int_part}{frac_part}");
    let keep = (int_part.len() as i64).saturating_add(scale);
    if keep < 0 {
        return 0.0;
    }
    let keep = keep as usize;
    let mut rounded = digits.as_bytes()[..keep].to_vec();
    if digits.as_bytes()[keep] >= b'5' {
        let mut carry = true;
        for digit in rounded.iter_mut().rev() {
            if *digit == b'9' {
                *digit = b'0';
            } else {
                *digit += 1;
                carry = false;
                break;
            }
        }
        if carry {
            rounded.insert(0, b'1');
        }
    }
    let digits = String::from_utf8(rounded).expect("decimal digits");
    if digits.is_empty() || digits.bytes().all(|d| d == b'0') {
        return 0.0;
    }
    let sign = if x.is_sign_negative() { "-" } else { "" };
    format!("{sign}{digits}e{}", -scale)
        .parse()
        .expect("rounded decimal")
}

/// Float64 × Int64 -> Float64, with row-wise scales and null propagation.
#[allow(dead_code)]
pub fn spark_round_udf() -> deltalake::datafusion::logical_expr::ScalarUDF {
    use deltalake::arrow::array::{Array, Float64Array, Int64Array};
    use deltalake::arrow::datatypes::DataType;
    use deltalake::datafusion::logical_expr::{create_udf, ColumnarValue, Volatility};
    use std::sync::Arc;

    create_udf(
        "spark_round",
        vec![DataType::Float64, DataType::Int64],
        DataType::Float64,
        Volatility::Immutable,
        Arc::new(|args| {
            let arrays = ColumnarValue::values_to_arrays(args)?;
            let values = arrays[0].as_any().downcast_ref::<Float64Array>().unwrap();
            let scales = arrays[1].as_any().downcast_ref::<Int64Array>().unwrap();
            let result: Float64Array = values
                .iter()
                .zip(scales.iter())
                .map(|(x, s)| x.zip(s).map(|(x, s)| spark_round(x, s)))
                .collect();
            Ok(ColumnarValue::Array(Arc::new(result)))
        }),
    )
}

#[allow(dead_code)]
pub fn register_spark_round(ctx: &deltalake::datafusion::prelude::SessionContext) {
    ctx.register_udf(spark_round_udf());
}

#[cfg(test)]
mod tests {
    use super::spark_round;

    #[test]
    fn signed_scales_extremes_and_special_values() {
        assert_eq!(spark_round(2.5, 0), 3.0);
        assert_eq!(spark_round(-2.5, 0), -3.0);
        assert_eq!(spark_round(0.125, 2), 0.13);
        assert_eq!(spark_round(150.0, -2), 200.0);
        assert_eq!(spark_round(-150.0, -2), -200.0);
        assert_eq!(spark_round(49.0, -2), 0.0);
        assert_eq!(spark_round(1e100, -99), 1e100);
        assert_eq!(spark_round(f64::MAX, 2), f64::MAX);
        assert_eq!(spark_round(1e-300, 301), 1e-300);
        assert_eq!(spark_round(1.0, i32::MIN), 0.0);
        assert_eq!(spark_round(1.25, i32::MAX), 1.25);
        assert_eq!(spark_round(f64::INFINITY, 0), f64::INFINITY);
        assert_eq!(spark_round(f64::NEG_INFINITY, 0), f64::NEG_INFINITY);
        assert!(spark_round(f64::NAN, 0).is_nan());
    }

    #[tokio::test]
    async fn udf_scalar_array_null_and_empty_inputs() {
        use deltalake::arrow::array::Float64Array;
        use deltalake::datafusion::prelude::SessionContext;
        let ctx = SessionContext::new();
        super::register_spark_round(&ctx);
        let batches = ctx.sql("SELECT spark_round(x, s) AS r FROM (VALUES (2.5, 0), (-2.5, 0), (0.125, 2), (150.0, -2), (NULL, 2), (1.0, NULL)) AS t(x,s)")
            .await.unwrap().collect().await.unwrap();
        let result = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        assert_eq!(
            result.iter().collect::<Vec<_>>(),
            vec![Some(3.0), Some(-3.0), Some(0.13), Some(200.0), None, None]
        );
        let batches = ctx
            .sql("SELECT spark_round(1.005, 2)")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        assert_eq!(
            batches[0]
                .column(0)
                .as_any()
                .downcast_ref::<Float64Array>()
                .unwrap()
                .value(0),
            1.01
        );
        let batches = ctx
            .sql("SELECT spark_round(CAST(NULL AS DOUBLE), 2) WHERE false")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        assert_eq!(batches.iter().map(|b| b.num_rows()).sum::<usize>(), 0);
        assert!(ctx.sql("SELECT spark_round('bad', 2)").await.is_err());
    }

    #[test]
    fn rounds_half_up_on_decimal_value_like_spark() {
        assert_eq!(spark_round(1.005, 2), 1.01);
        assert_eq!(spark_round(2.675, 2), 2.68);
        assert_eq!(spark_round(-1.005, 2), -1.01);
    }

    #[test]
    fn leaves_values_with_fewer_decimals_untouched() {
        assert_eq!(spark_round(37.5, 2), 37.5);
        assert_eq!(spark_round(100.0, 3), 100.0);
    }

    #[test]
    fn rounds_coordinates_to_two_places() {
        assert_eq!(spark_round(37.548512, 2), 37.55);
        assert_eq!(spark_round(-121.988649, 2), -121.99);
    }
}
