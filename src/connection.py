import duckdb as db
from pandas import DataFrame
from datetime import datetime, timedelta

from src.config import *

def datetime_to_timestamp(datetime_obj): 
    return int(datetime.timestamp(datetime_obj)*1000)

class DBManager:
    def __init__(self, address: str):
        self.con = db.connect(address)
        self.con.execute("SET TimeZone='UTC'; LOAD stochastic") 

        self.min_date = start_date 

    def run_command(self, command: str):
        return self.con.execute(command)
    
    def print_extensions(self):
        out = self.run_command("""
            SELECT extension_name, installed, loaded, install_path 
            FROM duckdb_extensions()
            WHERE installed = true
            """).df()
        print(out)
        
    def print_all_tables(self):
        out = self.run_command("show all tables").df()
        print(out)

    def print_all_macros_and_views(self):
        q = """
            SELECT function_name, parameters
            FROM duckdb_functions() 
            WHERE comment = 'e'
            """
        out = self.run_command(q).df()
        print(out)

        q = """
            SELECT view_name
            FROM duckdb_views
            WHERE comment = 'e'
            """
        out = self.run_command(q).df()
        print(out)

    def detail_table(self, table_name: str):
        out = self.run_command(f"show {table_name}").df()
        print(out)

    def create_new_table(self, table_name: str, attributes: str):
        command = f'CREATE TABLE IF NOT EXISTS {table_name} ({attributes})'
        self.con.execute(command)

    def drop_table(self, table_name: str):
        self.con.execute(f'DROP TABLE {table_name}')

    def dedup_table(self, table_name: str, column: str):
        self.con.execute(f"""
                        CREATE OR REPLACE TABLE {table_name} AS 
                        SELECT * FROM ( 
                        SELECT 
                            *, ROW_NUMBER() OVER (PARTITION BY {column} ORDER BY rowid) AS rn 
                        FROM {table_name} )
                        WHERE rn = 1;
                        ALTER TABLE {table_name} DROP rn
                        """)
    
    def show_duplicates(self, table_name: str, by_column: str):
        out = self.con.execute(f"""
                        SELECT *
                        FROM {table_name}
                        WHERE {by_column} IN (
                            SELECT {by_column}
                            FROM {table_name}
                            GROUP BY {by_column}
                            HAVING COUNT(*) > 1 )
                        """).df()
        print(out)

    def show_dups_funding(self):
        out = self.con.execute(f"""
                        SELECT
                            timestamp,
                            currency,
                            COUNT(*) AS dups
                        FROM funding_rates
                        GROUP BY
                            timestamp,
                            currency
                        HAVING COUNT(*) > 1;

                        """).df()
        print(out)

    def dedup_funding(self):
        self.run_command("""
                        CREATE OR REPLACE TABLE funding_rates AS
                        SELECT *
                        FROM (
                            SELECT
                                *,
                                ROW_NUMBER() OVER (
                                    PARTITION BY timestamp, currency
                                    ORDER BY rowid
                                ) AS rn
                            FROM funding_rates
                        )
                        WHERE rn = 1; ALTER TABLE funding_rates DROP rn
                        """)


    def push_data(self, table_name: str, data: DataFrame):
        self.con.register("batch", data)
        self.con.execute(f'INSERT INTO {table_name} SELECT * FROM batch')

    def upsert_data(self, table_name: str, data: DataFrame):
        self.con.register("batch", data)
        self.con.execute(f'INSERT OR IGNORE INTO {table_name} SELECT * FROM batch')

    def latest_ts(self, table_name: str, currency: str) -> datetime:

        q = f"select max(timestamp) from {table_name} where currency = '{currency}'"
        if table_name == 'spot_trades':
            q = f"select max(timestamp) from {table_name}"
        val = self.con.execute(q).fetchall()
        
        if val[0][0]:
            return val[0][0] + timedelta(milliseconds=1)
        else:
            return self.min_date
    
    def update_futures_ohlc(self, table_name: str, currency: str):
        if table_name == 'ohlc_1min_futures':
            interval = '1 minutes'
        else:
            raise ValueError("Unsupported table")

        start_query = f"""
            SELECT COALESCE(
                (SELECT max(bucket) + INTERVAL '{interval}' FROM {table_name} WHERE currency = '{currency}'),
                (SELECT min(timestamp) FROM future_trades WHERE currency = '{currency}')
            )
        """
        start_ts = self.con.execute(start_query).fetchone()[0]

        if not start_ts:
            return  
        
        query = f"""
            INSERT INTO {table_name}
            SELECT
                instrument_name,
                currency,
                time_bucket(INTERVAL '{interval}', timestamp) AS bucket,
                arg_min(price, timestamp) AS open,
                max(price) AS high,
                min(price) AS low,
                arg_max(price, timestamp) AS close,
                sum(amount) AS volume,
                count(*) AS n_orders,
                count(CASE WHEN direction = 'buy' THEN 1 END) * 100.0 / count(*) AS pct_buys_count,
                sum(CASE WHEN direction = 'buy' THEN amount ELSE 0 END) * 100.0 / sum(amount) AS pct_buys_volume,
                sum(CASE WHEN direction = 'buy' THEN price * amount ELSE 0 END) / 
                    NULLIF(sum(CASE WHEN direction = 'buy' THEN amount ELSE 0 END), 0) AS vw_buy_price,
                sum(CASE WHEN direction = 'sell' THEN price * amount ELSE 0 END) / 
                    NULLIF(sum(CASE WHEN direction = 'sell' THEN amount ELSE 0 END), 0) AS vw_sell_price,
                sum(price * amount) / sum(amount) AS vwap,
                sum(amount * (price / index_price - 1)) / sum(amount) AS vw_diff_index,
                median(price) AS median_price
            FROM future_trades
            WHERE 
                currency = ? 
                AND timestamp >= ?
                AND timestamp < time_bucket(INTERVAL '{interval}', (SELECT max(timestamp) FROM future_trades WHERE currency = ?))
            GROUP BY 1, 2, 3
        """
        
        self.con.execute(query, [currency, start_ts, currency])
                

    def update_index_ohlc(self, table_name: str, currency: str):
        interval = '1 minutes'

        start_query = f"""
            SELECT COALESCE(
                (
                    SELECT max(bucket) + INTERVAL '{interval}'
                    FROM {table_name}
                    WHERE currency = '{currency}'
                ),
                (
                    SELECT min(timestamp)
                    FROM (
                        SELECT timestamp FROM future_trades WHERE currency = '{currency}'
                        UNION ALL
                        SELECT timestamp FROM option_trades WHERE currency = '{currency}'
                    )
                )
            )
        """

        start_ts = self.con.execute(start_query).fetchone()[0]

        if not start_ts:
            return

        query = f"""
            INSERT INTO {table_name}
            SELECT
                currency,
                bucket,

                min_by(index_price, timestamp) AS open,
                max(index_price)               AS high,
                min(index_price)               AS low,
                max_by(index_price, timestamp) AS close,

                count(*) AS obs

            FROM (
                SELECT
                    currency,
                    timestamp,
                    time_bucket(INTERVAL '{interval}', timestamp) AS bucket,
                    index_price
                FROM future_trades
                WHERE currency = '{currency}'
                AND timestamp >= ?
                
                UNION ALL

                SELECT
                    currency,
                    timestamp,
                    time_bucket(INTERVAL '{interval}', timestamp) AS bucket,
                    index_price
                FROM option_trades
                WHERE currency = '{currency}'
                AND timestamp >= ?
            ) t
            WHERE bucket < time_bucket(
                INTERVAL '{interval}',
                (
                    SELECT max(timestamp)
                    FROM (
                        SELECT timestamp FROM future_trades WHERE currency = '{currency}'
                        UNION ALL
                        SELECT timestamp FROM option_trades WHERE currency = '{currency}'
                    )
                )
            )
            GROUP BY currency, bucket
        """

        self.con.execute(query, [start_ts, start_ts])

    def futures_term_structure(self, currency: str):
        q = f"""
            WITH 
            prep AS (
            SELECT
                date_trunc('hour', ft.timestamp) AS ts_hour,
                ft.instrument_name,
                ft.currency,
                ft.price,
                ft.index_price,
                ft.amount AS amount,
                i.expiration_timestamp AS expiration,
                date_diff('second', ft.timestamp,  expiration) / 31536000.0 AS T,
                CASE 
                    WHEN T <= 0.0383 THEN '2w'
                    WHEN T <= 0.0833 THEN '1m'
                    WHEN T <= 0.2500 THEN '3m'
                    WHEN T <= 0.5000 THEN '6m'
                    ELSE '12m'
                END AS expiry_bucket,
                ((ft.price / ft.index_price) - 1) / T AS apr
            FROM future_trades ft
            JOIN instruments i
                ON ft.instrument_name = i.instrument_name
            WHERE ft.instrument_name NOT LIKE '%PERPETUAL%'
            AND currency = '{currency}'
            AND ft.timestamp < time_bucket( INTERVAL '1 hour', (SELECT max(timestamp) FROM future_trades WHERE currency = '{currency}') )
            )

            SELECT
                ts_hour,
                currency,
                expiry_bucket,
                'vw_APR' AS metric_type,
                SUM(apr * amount) / SUM(amount) AS metric_value
            FROM prep
            GROUP BY 1, 2, 3

            UNION ALL

            SELECT
                ts_hour,
                currency,
                expiry_bucket,
                'avg_APR' AS metric_type,
                AVG(apr) AS metric_value
            FROM prep
            GROUP BY 1, 2, 3

            UNION ALL

            SELECT
                ts_hour,
                currency,
                expiry_bucket,
                'count' AS metric_type,
                COUNT(*) AS metric_value
            FROM prep
            GROUP BY 1, 2, 3

            UNION ALL

            SELECT
                ts_hour,
                currency,
                expiry_bucket,
                'volume' AS metric_type,
                SUM(amount) AS metric_value
            FROM prep
            GROUP BY 1, 2, 3
            """
        df = self.run_command(q).df()
        self.upsert_data('futures_term_structure', df)
    
    def options_term_structure(self, currency: str):
        q = f"""
            WITH
            prel AS (
                SELECT 
                    date_trunc('hour', timestamp) AS timestamp,
                    instrument_name,
                    currency,
                    iv / 100 AS iv,
                    amount,
                    CAST(split(instrument_name, '-')[3] AS DOUBLE) AS strike, 
                    split(instrument_name, '-')[4] AS call_put,
                    index_price
                FROM option_trades
                WHERE currency = '{currency}'
                AND timestamp < time_bucket( INTERVAL '1 hour', (SELECT max(timestamp) FROM option_trades WHERE currency = '{currency}') )
                ),

            add_expir AS (
                SELECT
                    pr.*,
                    date_diff('second', pr.timestamp, i.expiration_timestamp) / 31536000.0 AS T,

                FROM prel pr
                JOIN instruments i
                    ON pr.instrument_name = i.instrument_name
            ),

            calculations AS (
                SELECT 
                    *,
                    dist_normal_cdf(0, 1, ( ln( index_price / strike) + (0.5 * pow(iv, 2)) * T) / (iv * sqrt(T) ) ) AS phi_d1,
                    CASE 
                        WHEN call_put = 'C' THEN phi_d1
                        WHEN call_put = 'P' THEN phi_d1 - 1
                    END AS delta,
                    abs(delta) AS abs_delta
                FROM add_expir
            ),

            bucket_data AS (
                SELECT 
                    timestamp,
                    iv,
                    amount,
                    CASE 
                        WHEN abs_delta <= 0.2 THEN '0-0.2'
                        WHEN abs_delta <= 0.4 THEN '0.2-0.4'
                        WHEN abs_delta <= 0.6 THEN '0.4-0.6'
                        WHEN abs_delta <= 0.8 THEN '0.6-0.8'
                        ELSE '0.8-1' 
                    END AS delta_bucket,
                    CASE 
                        WHEN T <= 0.0082 THEN '3d'
                        WHEN T <= 0.0192 THEN '1w'
                        WHEN T <= 0.0383 THEN '2w'
                        WHEN T <= 0.0833 THEN '1m'
                        WHEN T <= 0.2500 THEN '3m'
                        WHEN T <= 0.5000 THEN '6m'
                        ELSE '12m'
                    END AS expiry_bucket
                FROM calculations
            )

            SELECT 
                timestamp AS ts_hour, 
                '{currency}' AS currency,
                delta_bucket,
                expiry_bucket, 
                'vw_iv' AS metric_type, 
                SUM(iv * amount) / NULLIF(SUM(amount), 0) AS metric_value
            FROM bucket_data
            GROUP BY 1, 2, 3, 4

            UNION ALL

            SELECT 
                timestamp AS ts_hour, 
                '{currency}' AS currency,
                delta_bucket,
                expiry_bucket, 
                'avg_iv' AS metric_type, 
                AVG(iv) AS metric_value
            FROM bucket_data
            GROUP BY 1, 2, 3, 4

            UNION ALL

            SELECT 
                timestamp AS ts_hour, 
                '{currency}' AS currency,
                delta_bucket,
                expiry_bucket, 
                'count' AS metric_type, 
                COUNT(*) AS metric_value
            FROM bucket_data
            GROUP BY 1, 2, 3, 4

            UNION ALL

            SELECT 
                timestamp AS ts_hour, 
                '{currency}' AS currency,
                delta_bucket,
                expiry_bucket, 
                'volume' AS metric_type, 
                SUM(amount) AS metric_value
            FROM bucket_data
            GROUP BY 1, 2, 3, 4
            """
        df = self.run_command(q).df()
        self.upsert_data('options_term_structure', df)
    
    def init_views(self):
        # realized funding by minute
        q = """
            CREATE VIEW funding_view AS 
            SELECT 
                f.bucket, 
                CASE 
                    WHEN ((f.instrument_name = 'BTC-PERPETUAL')) THEN ('BTC') 
                    WHEN ((f.instrument_name = 'ETH-PERPETUAL')) THEN ('ETH') 
                    ELSE NULL 
                END AS currency, 
                ln((f.close / j.close)) AS diff, 
                ((greatest(0.00025, ln((f.close / j.close))) + least(-0.00025, ln((f.close / j.close)))) / 480) AS funding 
                FROM ohlc_1min_futures AS f 
                INNER JOIN ohlc_1min_index AS j 
                    ON ((
                        (f.bucket = j.bucket) 
                        AND (((f.instrument_name = 'BTC-PERPETUAL') AND (j.currency = 'BTC')) 
                        OR ((f.instrument_name = 'ETH-PERPETUAL') 
                        AND (j.currency = 'ETH'))))
                    ) WHERE (f.instrument_name IN ('BTC-PERPETUAL', 'ETH-PERPETUAL')) ORDER BY f.bucket;
            """
        self.run_command(q)
        self.run_command("COMMENT ON VIEW funding_view IS 'e' ") # tag views

        # spot ohlc
        q = """
            CREATE OR REPLACE VIEW spot_1min_ohlc AS
            WITH ticks AS (
                SELECT
                    date_trunc('minute', timestamp) AS bucket,
                    instrument_name,
                    timestamp,
                    price,
                    amount
                FROM spot_trades
            ),
            returns AS (
                SELECT
                    bucket,
                    instrument_name,
                    timestamp,
                    POW(
                        LN(price / LAG(price) OVER (
                            PARTITION BY instrument_name, bucket
                            ORDER BY timestamp
                        )),
                        2
                    ) AS ret2,
                    amount
                FROM ticks
            ),
            agg AS (
                SELECT
                    bucket,
                    instrument_name,
                    SUM(amount) AS total_volume
                FROM ticks
                GROUP BY 1, 2
            )
            SELECT 
                t.bucket,
                t.instrument_name,
                MIN(t.timestamp)                        AS first_trade,
                MAX(t.timestamp)                        AS last_trade,
                ARG_MIN(t.price, t.timestamp)           AS open,
                MAX(t.price)                            AS high,
                MIN(t.price)                            AS low,
                ARG_MAX(t.price, t.timestamp)           AS close,
                SUM(t.price * t.amount) / SUM(t.amount) AS vwap,
                SUM(t.amount)                           AS volume,
                COUNT(*)                                AS n_orders,
                SUM(r.ret2)                             AS realized_sq_vol,
                SUM(r.ret2 * r.amount / a.total_volume) AS realized_sq_vol_vw

            FROM ticks t
            LEFT JOIN returns r
                ON t.bucket = r.bucket
            AND t.instrument_name = r.instrument_name
            LEFT JOIN agg a
                ON t.bucket = a.bucket
            AND t.instrument_name = a.instrument_name
            GROUP BY 1, 2

            """
        self.run_command(q)
        self.run_command("COMMENT ON VIEW spot_1min_ohlc IS 'e' ") # tag views

    def init_macros(self):
        # dollar candles 
        q = """
            CREATE OR REPLACE MACRO futures_by_volume(
                instr,
                start_ts,
                end_ts,
                volume_bin
            ) AS TABLE (
                WITH ticks AS (
                    SELECT 
                        timestamp, 
                        price, 
                        amount,
                        SUM(amount) OVER (ORDER BY timestamp) AS cum_volume
                    FROM future_trades
                    WHERE instrument_name = instr
                    AND timestamp BETWEEN start_ts AND end_ts
                ),
                by_volume AS (
                    SELECT
                        min(timestamp) AS ts_start,
                        max(timestamp) AS ts_end,
                        FLOOR(cum_volume / volume_bin) AS candle_id,
                        SUM(price * amount) / SUM(amount) AS vwap,
                        COUNT(*) AS n_trades
                    FROM ticks
                    GROUP BY candle_id
                    ORDER BY candle_id
                )
                SELECT * FROM by_volume
            );
            """
        self.run_command(q)
        self.run_command("COMMENT ON MACRO TABLE futures_by_volume IS 'e' ") # tag macros

        # dollar bars with period funding
        q = """
            CREATE OR REPLACE MACRO funding_volume_weighted(instr, start_ts, end_ts, volume_bin) AS TABLE (
            WITH 
            candles AS (
                SELECT *
                FROM futures_by_volume(instr, start_ts, end_ts, volume_bin)
                ),

            funding AS (
                SELECT bucket, diff, funding
                FROM funding_view
                WHERE currency = split(instr, '-')[1] AND bucket BETWEEN start_ts AND end_ts
            ),

            joined AS (
                SELECT 
                    c.candle_id,
                    SUM(f.diff) AS premium,
                    SUM(f.funding) AS funding
                FROM candles c LEFT JOIN funding f 
                    ON f.bucket >= c.ts_start AND f.bucket < c.ts_end
                GROUP BY c.candle_id
            )

            SELECT 
                strftime(c.ts_start, '%Y-%m-%d %H:%M:%S.%f%z') AS ts_start,
                strftime(c.ts_end, '%Y-%m-%d %H:%M:%S.%f%z') AS ts_end,
                c.vwap,
                c.n_trades,
                COALESCE(j.premium, 0) as premium,
                COALESCE(j.funding, 0) as funding
            FROM candles c LEFT JOIN joined j
                ON c.candle_id = j.candle_id
            ORDER BY c.ts_start
            );
            """
        self.run_command(q)
        self.run_command("COMMENT ON MACRO TABLE funding_volume_weighted IS 'e' ") # tag macros

        # dollar bars on ohlc
        q = """
	CREATE OR REPLACE MACRO ohlc_dollar_bars(
	    instr,
	    start_ts,
	    end_ts,
	    volume_bin
	) AS TABLE (
	    WITH 
	    prep AS (
		SELECT 
		    bucket, 
		    high, 
		    low, 
		    close, 
		    volume,
		    n_orders,
		    SUM(volume) OVER (ORDER BY bucket) AS cum_volume,
		    FLOOR(cum_volume / volume_bin) AS candle_id
		FROM ohlc_1min_futures
		WHERE
		    instrument_name = instr
		    AND bucket BETWEEN start_ts AND end_ts
		)
	    
	    SELECT
		min(bucket) AS ts_start,
		max(bucket) AS ts_end,
		max(high) AS high,
		min(low) AS low,
		arg_max(close, bucket) AS close,
		sum(close * volume) / sum(volume) AS vwap,
		sum(volume) AS volume,
		sum(n_orders) AS n_orders
	    FROM prep
	    GROUP BY candle_id
	    ORDER BY candle_id
	)
            """
        self.run_command(q)
        self.run_command("COMMENT ON MACRO TABLE ohlc_dollar_bars IS 'e' ") # tag macros

