from src.api import API
from src.connection import DBManager
from src.config import *


def make_aggregates():
    db = DBManager(db_address)

    for each in currencies:

        # --- AGGREGATES SECTION --- #

        # 1 mins ohlc futures
        db.update_futures_ohlc('ohlc_1min_futures', each)

        # 1 mins ohlc index
        db.update_index_ohlc('ohlc_1min_index', each)

        # futures term structure
        db.futures_term_structure(each)

        # options term structure
        db.options_term_structure(each)
