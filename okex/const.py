url = 'wss://real.okex_api.com:8443/ws/v3'
api_key = ""
secret_key = ""
passphrase = ""

def store_to_file(message):
    if isinstance(message, dict):
        message = str(message)
    if not isinstance(message, str):
        message = str(message)
    with open('okex_data','a') as f:
        f.writelines(message)
