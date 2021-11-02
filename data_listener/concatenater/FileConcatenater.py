import os
import json


class FileConcatenater:

    def __init__(self):

        self._config_file = "concatenater_config.json"
        self._read_config()

    def _read_config(self):

        with open(self._config_file) as config_file:
            config = json.load(config_file)

        self._src_url = config['src_url'] if config['src_url'].endswith('/') else config['src_url'] + '/'
        self._dst_url = config['dst_url'] if config['dst_url'].endswith('/') else config['dst_url'] + '/'

        if not isinstance(self._src_url, list):
            self._src_url = [self._src_url]

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

    def _concatenate(self, root):
        filelist = self._get_root_files(root)
        with open(f'{self._dst_url}{root}', 'a') as file:
            for filename in filelist:
                with open(f'{self._src_url}{filename}') as subfile:
                    file.write(subfile.read())

    # main function
    def concatenate_all(self):
        all_roots = self._get_roots()
        for root in all_roots:
            self._concatenate(root)

if __name__ == '__main__':

    concatenater = FileConcatenater()
    concatenater.concatenate_all()