from src.api import API
from src.connection import DBManager
from src.definitions import *
from src.config import *


def setup_database():
    db = DBManager(db_address)
    api = API(db)
    
    # --- DERIBIT DATA SECTION --- #
    db.create_new_table(tbl_funding_rates, att_funding_rates)
    db.create_new_table(tbl_future_trades, att_future_trades)
    db.create_new_table(tbl_futures_combo, att_futures_combo)
    db.create_new_table(tbl_option_trades, att_option_trades)
    db.create_new_table(tbl_spot_trades, att_spot_trades)
    db.create_new_table(tbl_instruments, att_instruments)

    # --- AGGREGATES SECTION --- #
    db.create_new_table(tbl_1m_ohlc, att_1m_ohlc)
    db.create_new_table(tbl_index_ohlc, att_index_ohlc)
    db.create_new_table(tbl_fut_ts, att_fut_ts)
    db.create_new_table(tbl_opt_ts, att_opt_ts)

    # --- VIEWS AND MACROS --- #
    db.init_views()
    db.init_macros()
