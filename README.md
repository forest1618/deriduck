# deriduck

A DuckDB-based database for Deribit.com historic market data. It handles ingestion, storage, and processing of futures, options, and spot order data.

# Requirements

    Python 3.8+
    DuckDB
    Pandas
    Requests

# Project Structure

    deriduck/
    ├── src/
    │   ├── aggregates.py   # OHLC and term structure calculations
    │   ├── api.py          # API connection
    │   ├── config.py       # Define your path to the DB and other things
    │   ├── connection.py   # Database connection and operations
    │   ├── definitions.py  # Schema definitions
    │   ├── ingestion.py    # Data collection
    │   └── schema.py       # Init tables, views, and macros
    └── main.py             # Entry point

# Setup and Usage

The project uses a single entry point.
1. Initialization: run the setup command to create the database file, tables, views, and macros:

        python3 main.py setup

2. Data update: Run the update command to fetch new data and recalculate aggregate tables:

        python3 main.py update

3. Only update aggregates: test deriduck by downloading a little bit of data with the 'update' command, stop it, and then aggregate it with:

        python3 main.py aggregate

# Data schema

1. Deribit data:

        instruments
        funding_rates
        future_trades
        fut_combo_trades
        option_trades
        spot_trades

2. Aggregated Tables:

        ohlc_1min_futures        1-minute intervals for future contracts.
        ohlc_1min_index          1-minute intervals for underlying indexes.
        futures_term_structure   APR-based term structure.
        options_term_structure   IV-based term structure categorized by delta buckets.

3. Views and Macros:

        funding_view             View  - Realized funding rates by minute.
        futures_by_volume        Macro - Tick based dollar bars.
        funding_volume_weighted  Macro - Join dollar bars with realized funding.
        ohlc_dollar_bars         Macro - OHLC based dollar bars.
