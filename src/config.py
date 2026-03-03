from datetime import datetime, timezone

currencies = ['ETH', 'BTC'] # https://docs.deribit.com/api-reference/market-data/public-get_currencies
db_address = '~/deribit.duckdb'
start_date = datetime(year=2022, month=10, day=1, hour=0, minute=0, tzinfo=timezone.utc)
latency = 0.2
