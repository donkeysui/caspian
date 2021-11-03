import os
import json
from .const import trade_columns, orderbook_columns
import pandas as pd

class FileConcatenater:

    def __init__(self):

        self._config_file = "concatenater_config.json"
        self._read_config()

    def _read_config(self):

        with open(self._config_file) as config_file:
            config = json.load(config_file)

        self._src_url = config['src_url'] if config['src_url'].endswith('/') else config['src_url'] + '/'
        self._dst_url = config['dst_url'] if config['dst_url'].endswith('/') else config['dst_url'] + '/'
        self._file_type = config['file_type']

        if not isinstance(self._src_url, list) and False:
            self._src_url = [self._src_url]

    def get_file_type(self):
        return self._file_type

    def _get_roots(self):
        raw_data_files = os.listdir(self._src_url)
        real_data_files = [x for x in raw_data_files if x.count('-') == 7]
        file_roots = set()
        for filename in real_data_files:
            file_roots.add(filename.split('.')[0])
        return list(file_roots)

    def _get_root_files(self, root):
        result = []
        raw_data_files = os.listdir(self._src_url)
        for filename in raw_data_files:
            if filename.startswith(root):
                result.append(filename)
        result.sort(reverse=True)
        return result

    def _concatenate_csv(self, root):
        filelist = self._get_root_files(root)
        with open(f'{self._dst_url}{root}', 'a') as file:
            for filename in filelist:
                with open(f'{self._src_url}{filename}') as subfile:
                    file.write(subfile.read())

    def _concatenate_hdf(self, root):
        filelist = self._get_root_files(root)
        if root.count('orderbook'):
            columns = orderbook_columns
        elif root.count('trade'):
            columns = trade_columns
        else:
            raise AttributeError("Invalid File Type, Not in orderbook and trade.")

        res_df = None
        for filename in filelist:
            if res_df is None:
                dataframe = pd.read_csv(f"{self._src_url}{filename}", names=columns, header=None)
                res_df = dataframe
            else:
                dataframe = pd.read_csv(f"{self._src_url}{filename}", names=columns, header=None)
                res_df.append(dataframe)

    # main function
    def concatenate_all(self):

        if self._file_type == 'csv':
            all_roots = self._get_roots()
            for root in all_roots:
                self._concatenate_csv(root)

        elif self._file_type == 'hdf':
            all_roots = self._get_roots()
            for root in all_roots:
                self._concatenate_hdf(root)


if __name__ == '__main__':

    concatenater = FileConcatenater()

    if concatenater.get_file_type() == 'csv':
        concatenater.concatenate_all()

    elif concatenater.get_file_type() == 'hdf':
        concatenater.concatenate_all_hdf()