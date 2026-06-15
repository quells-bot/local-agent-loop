use crate::activities::{Parse, SumActivity};
use crate::types::{
    ParentParams, ParentResult, ParseParams, SumChildParams, SumChildResult, SumParams,
};
use workflow::{Context, Definition, Error};

/// Child: sum a list of integers via the `SumActivity`.
pub struct SumChild;

#[async_trait::async_trait(?Send)]
impl Definition for SumChild {
    type Input = SumChildParams;
    type Output = SumChildResult;
    const TYPE: &'static str = "SumChild";
    async fn run(ctx: Context, params: SumChildParams) -> Result<SumChildResult, Error> {
        let summed = ctx
            .activity::<SumActivity>(SumParams { values: params.values })
            .await?;
        Ok(SumChildResult { total: summed.total })
    }
}

/// Parent: parse text → integers (parse failure fails the workflow via `?`),
/// then delegate the sum to the `SumChild` child workflow (spec §4).
pub struct Parent;

#[async_trait::async_trait(?Send)]
impl Definition for Parent {
    type Input = ParentParams;
    type Output = ParentResult;
    const TYPE: &'static str = "Parent";
    async fn run(ctx: Context, params: ParentParams) -> Result<ParentResult, Error> {
        let parsed = ctx
            .activity::<Parse>(ParseParams { text: params.text })
            .await?;
        let summed = ctx
            .child_workflow::<SumChild>(SumChildParams { values: parsed.values })
            .await?;
        Ok(ParentResult { total: summed.total })
    }
}
