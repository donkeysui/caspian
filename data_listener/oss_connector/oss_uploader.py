import oss2
import json

class OSSUploader:

    def __init__(self):

        self._endpoint = ''
        self._access_key_id = ''
        self._access_key_secret = ''

        self._read_access_file()

    def _read_access_file(self):
        with open('access_key.json') as key_file:
            key_dict = json.load(key_file)
            self._endpoint = key_dict['endpoint']
            self._access_key_id = key_dict['access_key']
            self._access_key_secret = key_dict['access_secret']

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
    uploader.upload_local_file('/home/public/data_package/eth_package_all.tar', 'orderbook-data')
