# --- DERIBIT DATA SECTION --- #

# funding rates
tbl_funding_rates = 'funding_rates'
att_funding_rates = '''
        timestamp TIMESTAMPTZ,
        index_price DOUBLE,
        prev_index_price DOUBLE,
        interest_8h DOUBLE,
        interest_1h DOUBLE,
        instrument_name VARCHAR,
        currency VARCHAR
        '''

# futures trades
tbl_future_trades = 'future_trades'
att_future_trades = '''
        trade_id VARCHAR,
        timestamp TIMESTAMPTZ,
        instrument_name VARCHAR,
        price  DOUBLE,
        mark_price DOUBLE,
        amount INTEGER,
        index_price DOUBLE,
        direction VARCHAR,
        currency VARCHAR
        '''

# future combos
tbl_futures_combo = 'fut_combo_trades'
att_futures_combo = '''
        trade_id VARCHAR,
        timestamp TIMESTAMPTZ,
        instrument_name VARCHAR,
        price  DOUBLE,
        mark_price DOUBLE,
        amount INTEGER,
        index_price DOUBLE,
        direction VARCHAR,
        currency VARCHAR
        '''

# option trades
tbl_option_trades = 'option_trades'
att_option_trades = '''
        trade_id VARCHAR,
        timestamp TIMESTAMPTZ,
        instrument_name VARCHAR,
        price DOUBLE,
        iv DOUBLE,
        mark_price DOUBLE,
        amount DOUBLE,
        index_price DOUBLE,
        direction VARCHAR,
        currency VARCHAR
        '''

# spot trades
tbl_spot_trades = 'spot_trades'
att_spot_trades = '''
        trade_id VARCHAR,
        timestamp TIMESTAMPTZ,
        instrument_name VARCHAR, 
        price  DOUBLE,
        mark_price DOUBLE,
        amount DOUBLE,
        index_price DOUBLE,
        direction VARCHAR
'''

# instruments
tbl_instruments = 'instruments'
att_instruments = """
        instrument_name VARCHAR PRIMARY KEY,
        tick_size DOUBLE,
        taker_commission DOUBLE,
        settlement_period VARCHAR,
        settlement_currency VARCHAR,
        quote_currency VARCHAR,
        maker_commission DOUBLE,
        kind VARCHAR,
        expiration_timestamp TIMESTAMPTZ,
        creation_timestamp TIMESTAMPTZ,
        counter_currency VARCHAR,
        contract_size DOUBLE,
        base_currency VARCHAR
        """

# --- AGGREGATES SECTION --- #

# 1 min ohlc futures
tbl_1m_ohlc = 'ohlc_1min_futures'
att_1m_ohlc = """
        instrument_name VARCHAR,
        currency VARCHAR,
        bucket TIMESTAMPTZ,
        open DOUBLE,
        high DOUBLE,
        low DOUBLE,
        close DOUBLE,
        volume DOUBLE,
        n_orders UBIGINT,
        pct_buys_count DOUBLE,
        pct_buys_volume DOUBLE,
        vw_buy_price DOUBLE,
        vw_sell_price DOUBLE,
        vwap DOUBLE,
        vw_diff_index DOUBLE,
        median_price DOUBLE,
        """

# 1 min ohlc index
tbl_index_ohlc = 'ohlc_1min_index'
att_index_ohlc = """
        currency VARCHAR,
        bucket TIMESTAMPTZ,
        open DOUBLE,
        high DOUBLE,
        low DOUBLE,
        close DOUBLE,
        obs UBIGINT
        """

# 1 hour futures term structure
tbl_fut_ts = 'futures_term_structure'
att_fut_ts = """
        ts_hour TIMESTAMPTZ,
        currency VARCHAR,
        expiry_bucket VARCHAR,
        metric_type VARCHAR,
        metric_value DOUBLE,
        PRIMARY KEY (
                ts_hour, 
                currency, 
                expiry_bucket, 
                metric_type)
        """

# 1 hour options term structure
tbl_opt_ts = 'options_term_structure'
att_opt_ts = """
        ts_hour TIMESTAMPTZ,
        currency VARCHAR,
        delta_bucket VARCHAR,
        expiry_bucket VARCHAR,
        metric_type VARCHAR,
        metric_value DOUBLE,
        PRIMARY KEY (
                ts_hour, 
                currency, 
                delta_bucket, 
                expiry_bucket,
                metric_type)
        """
