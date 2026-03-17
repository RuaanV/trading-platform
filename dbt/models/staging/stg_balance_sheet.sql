select
    symbol,
    extracted_at::timestamptz as extracted_at,
    metric,
    as_of_date::date as as_of_date,
    value::numeric as value
from {{ source('raw', 'balance_sheet') }}
