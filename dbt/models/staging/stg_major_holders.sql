select
    symbol,
    extracted_at::timestamptz as extracted_at,
    holder_metric,
    holder_value
from {{ source('raw', 'major_holders') }}
