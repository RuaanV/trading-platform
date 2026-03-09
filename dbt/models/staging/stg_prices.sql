-- Stub staging model for raw price data.
select
    symbol,
    as_of_date,
    close_price,
    volume
from raw.prices
