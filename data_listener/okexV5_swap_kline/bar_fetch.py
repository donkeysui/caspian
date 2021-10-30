import datetime
import ccxt
import pandas as pd
from influxdb import DataFrameClient
import sys
import json
import time


class BarFetcher:

    def __init__(self, symbols, measurement=None):

        self._get_threshold = 0.1
        self.conn = ccxt.okex()
        self.dbclient = DataFrameClient(database='test')
        self.symbols = symbols
        if measurement:
            self.influx_measurement = measurement
        else:
            self.influx_measurement = 'swap_kline'
        self.convert = datetime.datetime.fromtimestamp

    def fetch(self, symbol):

        res = self.conn.fetchOHLCV(symbol)
        resdf = pd.DataFrame(res, columns=['time', 'o', 'h', 'l', 'c', 'v'])
        resdf.time = resdf.time / 1000
        resdf.time = resdf.time.apply(self.convert)
        resdf.index = resdf.time
        resdf.drop('time', axis=1, inplace=True)
        resdf['symbol'] = symbol

        return resdf

    def write_influx(self, data):

        self.dbclient.write_points(data,
                                   measurement=self.influx_measurement,
                                   tag_columns=['symbol'])

    def all_fetch_and_write(self):

        for symbol in self.symbols:
            self.write_influx(self.fetch(symbol))
            time.sleep(self._get_threshold)


if __name__ == '__main__':

    if len(sys.argv) > 1:
        config_file_name = sys.argv[1]

    with open(config_file_name) as file:
        swap_symbols = json.load(file)['symbols']

    Fetcher = BarFetcher(symbols=swap_symbols)
    Fetcher.all_fetch_and_write()


