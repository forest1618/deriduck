# deriduck

A DuckDB-based database for Deribit.com historical market data. It handles ingestion, storage, and processing of futures, options, and spot order data.
It will take several hours to completely initialize the database. 
Contributions are welcome!

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
1. **Initialization**: run the setup command to create the database file, tables, views, and macros:

        python3 main.py setup

2. **Data update**: Run the update command to fetch new data and recalculate aggregate tables:

        python3 main.py update

> **Warning**: You can interrupt the `update` command using **Ctrl+C** only during the **data ingestion phase**. Do **NOT** interrupt the script during the **aggregation phase**, as this may lead to inconsistent data. The aggregation phase duration depends on your storage performance (usually a few minutes). The terminal will indicate which phase is currently active.

# Recommended usage

1. Clone the repository:
    ```bash
    git clone https://github.com/forest1618/deriduck.git
    cd deriduck
    ```

2. Activate an environment of your choice and install the requirements:
    ```bash
    pip install -r requirements.txt 
    ```

3. Initialize the database:
    ```bash
    python3 main.py setup
    ```

4. Download data and populate database:
    ```bash
    python3 main.py update
    ```

5. Once you completely initialize **deriduck**, query it using python:
    ```python
    import duckdb as dd
    import pandas as pd

    con = dd.connect('~/deribit.duckdb')
    con.execute("SET TimeZone='UTC'; LOAD stochastic") 
    
    df = con.execute("SELECT * FROM future_trades LIMIT 100").df()
    print(df)
    df = con.execute("SELECT * FROM ohlc_1min_index WHERE currency = 'BTC' LIMIT 100").df()
    print(df)
    ```

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
        spot_1min_ohlc           View  - Spot trades OHLC.
        futures_by_volume        Macro - Tick based dollar bars.
        funding_volume_weighted  Macro - Join dollar bars with realized funding.
        ohlc_dollar_bars         Macro - OHLC based dollar bars.


