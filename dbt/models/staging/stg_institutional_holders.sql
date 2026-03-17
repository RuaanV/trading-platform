select
    symbol,
    extracted_at::timestamptz as extracted_at,
    holder,
    shares::numeric as shares,
    coalesce(date_reported::date, null) as date_reported,
    pct_out::numeric as pct_out,
    value::numeric as value
from {{ source('raw', 'institutional_holders') }}
