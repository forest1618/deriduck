from datetime import datetime, timezone
from src.api import API
from src.connection import DBManager
from src.config import *

end_date = datetime.now(timezone.utc)
end_date = end_date.replace(hour=0, minute=0, second=0, microsecond=0)

db = DBManager(db_address)
api = API(db)

def update_data(
        table: str, 
        currency: str, 
        kind: str, 
        update_fn):
    latest = db.latest_ts(table, currency)
    if kind == '':
        update_fn(currency, latest, end_date)
        return
    update_fn(currency, kind, latest, end_date, table)

def update_db_data():
    print(f'{datetime.now(timezone.utc)} - Update started - Stop it with Ctrl-C if you want')
    for each in currencies:

        # --- DERIBIT DATA SECTION --- #

        # funding rates
        update_data(
            'funding_rates', each+'-PERPETUAL', '', 
            api.get_funding_rate_history)

        # futures trades
        update_data(
            'future_trades', each, 'future', 
            api.get_last_trades_by_currency)

        # future combos
        update_data(
            'fut_combo_trades', each, 'future_combo', 
            api.get_last_trades_by_currency)

        # options trades
        update_data(
            'option_trades', each, 'option', 
            api.get_last_trades_by_currency)
        
    # instruments
    api.update_available_instruments(each)

    # spot trades
    update_data(
        'spot_trades', 'USDT', 'spot',  # USDT returns all spot currencies
        api.get_last_trades_by_currency)
