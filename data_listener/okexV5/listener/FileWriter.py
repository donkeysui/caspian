import datetime
from loguru import logger
from . import const
import os


class FileWriter:

    def __init__(self, configs):

        symbol = configs.get('symbol', None)
        exchange = configs.get('exchange', None)
        data_type = configs.get('data_type', None)
        file_url = configs.get('file_url', None)
        file_format = configs.get('file_format')

        if not symbol:
            logger.error("INIT ERROR -> FileWriter -> symbol is None! Please check the configs")
        if not exchange:
            logger.error("INIT ERROR -> FileWriter -> exchange is None! Please check the configs")
        if not data_type:
            logger.error("INIT ERROR -> FileWriter -> data_type is None! Please check the configs")
        if not file_url:
            logger.error("INIT ERROR -> FileWriter -> file_url is None! Please check the configs")
        if not file_format:
            logger.error("INIT ERROR -> FileWriter -> file_format is None! Please check the configs")

        self._symbol = symbol
        self._exchange = exchange
        self._data_type = data_type
        self._file_url = file_url
        self._file_format = file_format

        self._file_index_digits = const.DEFAULT_FILE_INDEX_DIGITS

        self.memory_data_length = 0
        self.File = None

        self._open_file_handle()

    def __repr__(self):
        pass

    def _open_file_handle(self) -> None:
        if not self.File or self.File.closed:
            filename = self._get_filename(index=1)
            self.File = open(f"{self._file_url}/{filename}", 'a')

    def write(self, data: dict) -> None:
        def check_string_available(string):
            if string.startswith(',') or string.endswith(','):
                return False
            if not string.count(','):
                return False
            return True

        if isinstance(data, dict):
            line_string = ','.join([item for item in data.values()])
        elif isinstance(data, str):
            line_string = data

        if not check_string_available(line_string):
            raise AttributeError(f"{line_string} not applicable string format.")
            return

        self.File.write(f"{line_string}\n")
        self.memory_data_length += 1

        if self.memory_data_length >= const.DEFAULT_MOMERY_CHUCK_SIZE:
            self.File.flush()  # Flush缓冲区，写入文件
            self.memory_data_length = 0  # memory标记位置0
            self._check_file_chuck()  # 检查文件大小是否超过设定大小

    def _check_file_chuck(self) -> None:
        file_size = os.path.getsize(self.File.name)
        if file_size > const.DEFAULT_FILE_CHUCK_SIZE:
            self._renames_all_file()

    def _renames_all_file(self):
        self.File.close()
        all_files = os.listdir(self._file_url)
        same_type_files = [filename for filename in all_files if filename.count(self._get_filename())]
        files_amount = len(same_type_files)
        for index in range(files_amount, 0, -1):
            src_filename = self._get_filename(index)
            dst_filename = self._get_filename(index + 1)
            os.rename(f"{self._file_url}/{src_filename}", f"{self._file_url}/{dst_filename}")
        self._open_file_handle()

    def _get_index(self, index: int) -> str:
        digits = self._file_index_digits
        if len(str(index)) > digits:
            raise AttributeError(f"index {index} exceeded the limit of digits.")
        return (digits - len(str(index))) * '0' + str(index)

    def _get_next_filename(self, filename: str):
        filename_root, file_index = filename.split('.')
        next_index = self._get_index(int(file_index) + 1)
        return f"{filename_root}.{next_index}"

    def _avail_file_format(self) -> list:
        return const.AVAIL_FILE_FORMAT

    def _get_filename(self, index=None) -> str:
        exchange = self._exchange
        symbol = self._symbol
        data_type = self._data_type
        now = datetime.datetime.today()
        year = now.year
        month = now.month
        day = now.day
        if isinstance(index, int):
            index = self._get_index(index)
        if index:
            filename = f"{exchange}-{symbol}-{data_type}-{year}-{month}-{day}.{index}"
        else:
            filename = f"{exchange}-{symbol}-{data_type}-{year}-{month}-{day}"
        return filename
