####################################################################################
#
#           CONST VARIABLES USED DEFINED BELOW!
#           INITED BY DONKEY KHAN - 2021/10/21 22:49
#
####################################################################################

# 可用的文件类型，这里的文件类型指的是归档后的文件
AVAIL_FILE_FORMAT = ['hdf5', 'feather', 'csv', 'parquet']

# 默认的文件分块大小，1024*1024意为Mb
DEFAULT_FILE_CHUCK_SIZE = 64 * 1024 * 1024

# 默认的内存字符串数组上限，最好是2的整数幂
DEFAULT_MOMERY_CHUCK_SIZE = 1024

# 默认的文件末尾index位数，4意味着0001, 0002 如5就意味着00001, 00002。
DEFAULT_FILE_INDEX_DIGITS = 4