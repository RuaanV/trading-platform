with latest_company as (
    select
        symbol,
        extracted_at,
        short_name,
        sector,
        industry,
        country,
        currency,
        market_cap,
        enterprise_value,
        trailing_pe,
        forward_pe,
        trailing_eps,
        forward_eps,
        beta,
        return_on_assets,
        return_on_equity,
        total_revenue,
        gross_profits,
        ebitda,
        long_business_summary
    from (
        select
            *,
            row_number() over (partition by symbol order by extracted_at desc) as row_num
        from {{ ref('stg_company_info') }}
    ) ranked_company
    where row_num = 1
),
latest_balance_sheet as (
    select
        symbol,
        extracted_at,
        metric,
        as_of_date,
        value
    from (
        select
            *,
            row_number() over (
                partition by symbol, metric
                order by extracted_at desc, as_of_date desc
            ) as row_num
        from {{ ref('stg_balance_sheet') }}
    ) ranked_balance_sheet
    where row_num = 1
),
balance_rollup as (
    select
        symbol,
        max(case when metric = 'Total Assets' then value end) as total_assets,
        max(case when metric = 'Total Debt' then value end) as total_debt,
        max(case when metric = 'Stockholders Equity' then value end) as stockholders_equity,
        max(case when metric = 'Cash Cash Equivalents And Short Term Investments' then value end)
            as cash_and_short_term_investments
    from latest_balance_sheet
    group by symbol
)
select
    c.symbol,
    c.extracted_at,
    c.short_name,
    c.sector,
    c.industry,
    c.country,
    c.currency,
    c.market_cap,
    c.enterprise_value,
    c.trailing_pe,
    c.forward_pe,
    c.trailing_eps,
    c.forward_eps,
    c.beta,
    c.return_on_assets,
    c.return_on_equity,
    c.total_revenue,
    c.gross_profits,
    c.ebitda,
    b.total_assets,
    b.total_debt,
    b.stockholders_equity,
    b.cash_and_short_term_investments
from latest_company c
left join balance_rollup b
    on c.symbol = b.symbol
