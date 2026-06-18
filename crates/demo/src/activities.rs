use crate::types::{ParseParams, ParseResult, SumParams, SumResult};
use activity::{Context, Definition, Error};

/// Pure parse: split on whitespace, parse each token as i64. Empty input is a
/// valid empty list (spec §4). A bad token is a fatal (non-retryable) error.
fn parse_ints(text: &str) -> Result<Vec<i64>, String> {
    text.split_whitespace()
        .map(|tok| {
            tok.parse::<i64>()
                .map_err(|_| format!("could not parse '{tok}' as an integer"))
        })
        .collect()
}

pub struct Parse;

#[async_trait::async_trait]
impl Definition for Parse {
    type Input = ParseParams;
    type Output = ParseResult;
    const TYPE: &'static str = "Parse";
    async fn run(&self, _ctx: Context, params: ParseParams) -> Result<ParseResult, Error> {
        let values = parse_ints(&params.text).map_err(Error::fatal)?;
        Ok(ParseResult { values })
    }
}

pub struct SumActivity;

#[async_trait::async_trait]
impl Definition for SumActivity {
    type Input = SumParams;
    type Output = SumResult;
    const TYPE: &'static str = "Sum";
    async fn run(&self, _ctx: Context, params: SumParams) -> Result<SumResult, Error> {
        Ok(SumResult {
            total: params.values.iter().sum(),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use activity::{Execution, Info};

    fn test_ctx() -> Context {
        Context::new(Info {
            execution: Execution {
                workflow_id: "w".into(),
                run_id: "r".into(),
            },
            activity_id: "0".into(),
            activity_type: "Sum".into(),
            attempt: 1,
        })
    }

    #[test]
    fn parses_space_separated_integers() {
        assert_eq!(parse_ints("1 2 3"), Ok(vec![1, 2, 3]));
    }

    #[test]
    fn empty_input_is_empty_list() {
        assert_eq!(parse_ints(""), Ok(vec![]));
        assert_eq!(parse_ints("   "), Ok(vec![]));
    }

    #[test]
    fn parses_negative_integers() {
        assert_eq!(parse_ints("-5 10"), Ok(vec![-5, 10]));
    }

    #[test]
    fn bad_token_is_an_error_naming_the_token() {
        let err = parse_ints("1 two 3").unwrap_err();
        assert!(err.contains("two"), "got: {err}");
    }

    #[tokio::test]
    async fn sum_activity_totals_values() {
        let out = SumActivity.run(test_ctx(), SumParams { values: vec![1, 2, 3] })
            .await
            .unwrap();
        assert_eq!(out, SumResult { total: 6 });
    }

    #[tokio::test]
    async fn sum_activity_empty_is_zero() {
        let out = SumActivity.run(test_ctx(), SumParams { values: vec![] })
            .await
            .unwrap();
        assert_eq!(out, SumResult { total: 0 });
    }
}
