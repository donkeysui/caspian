import oss2

class OSSUploader:

    def __init__(self):

        self._endpoint = 'https://oss-cn-shanghai.aliyuncs.com'
        self._access_key_id = ''
        self._access_key_secret = ''

    def _auth(self):
        auth = oss2.Auth(self._access_key_id, self._access_key_secret)
        return auth

    def _bucket(self, bucket_name):
        auth = self._auth()
        endpoint = self._endpoint
        bucket = oss2.Bucket(auth, endpoint, bucket_name)
        return bucket

    def upload_local_file(self, file_url, bucket_name):
        filename = file_url.split('/')[0]
        bucket = self._bucket(bucket_name)
        bucket.put_object_from_file(filename, file_url)

if __name__ == '__main__':

    uploader = OSSUploader()
    uploader.upload_local_file('C:/Users/nd/Desktop/cryptoexchange_websocket_loads/data_listener/okexV5/listener/const.py', 'orderbook-data')
