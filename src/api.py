import time
import requests
import pandas as pd
from datetime import datetime, date, timezone, timedelta
from src.config import *

def datetime_to_timestamp(datetime_obj: date) -> int: 
    return int(datetime.timestamp(datetime_obj)*1000)

def timestamp_to_datetime(timestamp: int) -> date: 
    return datetime.fromtimestamp(timestamp/1000, tz=timezone.utc)

def call(session, url: str, params: dict):
    time.sleep(latency) 
    response = session.get(url, params=params)
    return response.json()

class API:
    def __init__(self, db_manager):
        self.db = db_manager
                
    def get_funding_rate_history(
            self,
            instrument_name: str, 
            start_date: date, 
            end_date: date
            ):
        
        url = 'https://www.deribit.com/api/v2/public/get_funding_rate_history'
        next_chunk = min(end_date, start_date + timedelta(days=25))
        
        params = {
            "instrument_name": instrument_name,
            "start_timestamp": datetime_to_timestamp(start_date),
            "end_timestamp": datetime_to_timestamp(next_chunk)
            }
        
        with requests.Session() as session:
            while True:
                response_data = call(session, url, params)

                if len(response_data["result"]) == 0:
                    break
                
                attributes = [
                    'timestamp',
                    'index_price',
                    'prev_index_price',
                    'interest_8h',
                    'interest_1h'
                ]

                df = pd.DataFrame(response_data["result"])[attributes]
                df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms', utc=True)
                df['instrument_name'] = instrument_name
                df['currency'] = instrument_name.split('-')[0]

                self.db.push_data('funding_rates', df)

                max_val = max([x['timestamp'] for x in response_data['result']])
                if max_val == datetime_to_timestamp(end_date):
                    break
                params["start_timestamp"] = params['end_timestamp']
                end_time = min(
                    end_date,
                    timestamp_to_datetime(params['start_timestamp']) + timedelta(days=25)
                )
                params["end_timestamp"] = datetime_to_timestamp(end_time)
        
    def get_last_trades_by_currency(
            self,
            currency: str, 
            kind: str,
            start_date: date, 
            end_date: date,
            into_table: str
            ):
        
        url = 'https://history.deribit.com/api/v2/public/get_last_trades_by_currency'
        params = {
            "currency": currency, 
            "kind": kind,
            "count": 500,
            "include_old": True,
            "start_timestamp": datetime_to_timestamp(start_date),
            "end_timestamp": datetime_to_timestamp(end_date),
            "sorting":"asc"
            }
        
        if kind == 'future':
            attributes = [
                'trade_id',
                'timestamp',
                'instrument_name',
                'price',
                'mark_price',
                'amount',
                'index_price',
                'direction'
                ]
        
        if kind == 'future_combo':
            attributes = [
                'trade_id',
                'timestamp',
                'instrument_name',
                'price',
                'mark_price',
                'amount',
                'index_price',
                'direction'
                ] 
            
        if kind == 'option': 
            attributes = [
                'trade_id',
                'timestamp',
                'instrument_name',
                'price',
                'iv',
                'mark_price',
                'amount',
                'index_price',
                'direction'
                ]   
            
        if kind == 'spot':
            attributes = [
                'trade_id',
                'timestamp',
                'instrument_name',
                'price',
                'mark_price',
                'amount',
                'index_price',
                'direction'
            ]
        
        with requests.Session() as session:
            while True:
                response_data = call(session, url, params)

                if len(response_data["result"]["trades"]) == 0:
                    break
                df = pd.DataFrame(response_data["result"]["trades"])[attributes]
                if kind != 'spot':
                    df['currency'] = currency
                df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms', utc=True)
                self.db.push_data(into_table, df)
                params["start_timestamp"] = response_data["result"]["trades"][-1]["timestamp"] + 1


    def update_available_instruments(
            self,
            currency: str
            ):
        
        url = 'https://history.deribit.com/api/v2/public/get_instruments'
        url2 = 'https://www.deribit.com/api/v2/public/get_instruments'

        params = {
            "currency": currency, 
            "expired": "true"
            }
        params2 = {
            "currency": currency, 
            "expired": "false"
        }
        
        attributes = [
            "instrument_name",
            "tick_size",
            "taker_commission",
            "settlement_period",
            "settlement_currency",
            "quote_currency",
            "maker_commission",
            "kind",
            "expiration_timestamp",
            "creation_timestamp",
            "counter_currency",
            "contract_size",
            "base_currency"
            ]
        
        def conv(x):
            if  x > 30000000000000: # default perpetual value
                return pd.to_datetime(1000, unit='ms', utc=True)
            else:
                return pd.to_datetime(x, unit='ms', utc=True)

        with requests.Session() as session:     
            response_data = call(session, url, params)
            df = pd.DataFrame(response_data["result"])[attributes]
            df['expiration_timestamp'] = df['expiration_timestamp'].apply(conv)
            df['creation_timestamp'] = df['creation_timestamp'].apply(conv)

            self.db.upsert_data('instruments', df)

            response_data = call(session, url2, params2)
            df = pd.DataFrame(response_data["result"])[attributes]
            df['expiration_timestamp'] = df['expiration_timestamp'].apply(conv)
            df['creation_timestamp'] = df['creation_timestamp'].apply(conv)

            self.db.upsert_data('instruments', df)
            