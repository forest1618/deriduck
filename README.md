# deriduck

A DuckDB-based data warehouse for Deribit.com market data. It handles ingestion, storage, and analytical processing of futures, options, and spot order books.

# Requirements

    Python 3.8+
    DuckDB
    Pandas
    Requests

# Project Structure

    deriduck/
    ├── src/
    │   ├── connection.py   # Database connection management
    │   ├── schema.py       # DDL: Tables, Views, and Macros
    │   ├── ingestion.py    # Data collection from Deribit API
    │   ├── aggregates.py   # OHLC and term structure calculations
    │   ├── api.py          # Local API server
    │   └── definitions.py  # Schema definitions
    └── main.py             # Unified CLI entry point

# Setup and Usage

The project uses a unified CLI to manage the data pipeline.
1. Initialization: run the setup command to create the database file, tables, views, and macros:

        python3 main.py setup

2. Data update: Run the update command to fetch new data and recalculate aggregate tables:

        python3 main.py update

3. Only update aggregates: Test deriduck by downloadin a little bit of data, stop it, then aggregate it with:

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

        ohlc_1min_futures: 1-minute intervals for future contracts.
        ohlc_1min_index: 1-minute intervals for underlying indexes.
        futures_term_structure: APR-based term structure.
        options_term_structure: IV-based term structure categorized by delta buckets.

3. Views and Macros:

        funding_view: View - Realized funding rates by minute.
        futures_by_volume: Macro - Tick based dollar bars.
        funding_volume_weighted: Macro - Join dollar bars with realized funding.
        ohlc_dollar_bars: Macro - OHLC based dollar bars.
