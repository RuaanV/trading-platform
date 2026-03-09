-- Stub feature model joining price and fundamentals snapshots.
select
    p.symbol,
    p.as_of_date,
    p.close_price,
    p.volume
from {{ ref('stg_prices') }} as p
