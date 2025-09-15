CREATE TABLE IF NOT EXISTS market_ticks_parquet
WITH (
  format='PARQUET',
  write_compression='SNAPPY',
  external_location='s3://<clean-bucket>/curated/market_ticks/',
  partitioned_by = ARRAY['asset','year','month','day']
) AS
SELECT market_id, last_price, min_ask, max_bid, volume,
       price_variation_24h, price_variation_7d, ingested_at,
       asset, year, month, day
FROM market_ticks
WHERE year = year(current_date)
  AND month = month(current_date)
  AND day = day_of_month(current_date);
