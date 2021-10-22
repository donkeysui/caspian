import datetime
from loguru import logger
import const
import os

class FileWriter:

    def __init__(self, configs):

        symbol = configs.get('symbol', None)
        exchange = configs.get('exchange', None)
        data_type = configs.get('data_type', None)
        dir_url = configs.get('dir_url', None)
        file_format = configs.get('file_format')

        if not symbol:
            logger.error("INIT ERROR -> FileWriter -> symbol is None! Please check the configs")
        if not exchange:
            logger.error("INIT ERROR -> FileWriter -> exchange is None! Please check the configs")
        if not data_type:
            logger.error("INIT ERROR -> FileWriter -> data_type is None! Please check the configs")
        if not dir_url:
            logger.error("INIT ERROR -> FileWriter -> dir_url is None! Please check the configs")
        if not file_format:
            logger.error("INIT ERROR -> FileWriter -> file_format is None! Please check the configs")

        self._symbol = symbol
        self._exchange = exchange
        self._data_type = data_type
        self._dir_url = dir_url
        self._file_format = file_format

        self.memory_data_length = 0;
        self.File = None

    def __repr__(self):
        pass

    def open_file_handle(self, filename: str) -> None:
        if not self.File:
            self.File = open(filename, 'a')

    def append_memory(self, data: dict) -> None:
        def check_string_available(string):
            if string.startswith(',') or string.endswith(','):
                return False
            if not string.count(','):
                return False
            return True

        if isinstance(data, dict):
            line_string = [item for item in data.values()].join(',')
        elif isinstance(data, str):
            line_string = data

        self.memory_data_length += 1

        if self.memory_data_length >= const.DEFAULT_MOMERY_CHUCK_SIZE:
            self.File.flush() # Flush缓冲区，写入文件
            self.memory_data_length = 0 # memory标记位置0
            self._check_file_chuck() # 检查文件大小是否超过设定大小


    def _check_file_chuck(self) -> None:
        file_size = os.path.getsize(self.File.name)
        if file_size > const.DEFAULT_FILE_CHUCK_SIZE:
            change()

    def avail_file_format(self) -> list:
        return const.AVAIL_FILE_FORMAT

    def get_filename(self, platform: str, symbol: str, data_type: str, file_index: int) -> str:
        now = datetime.datetime.today()
        year = now.year
        month = now.month
        day = now.day
        file_format = self.file_format
        filename = f"{platform}-{symbol}-{data_type}-{year}-{month}-{day}-{file_index}.{file_format}"
        return filename

