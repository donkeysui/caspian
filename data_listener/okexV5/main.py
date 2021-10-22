# -*- coding: utf-8 -*-
"""
  @ Author:   Mr.Hat
  @ Email:    shenghong6560@gmail.com
  @ Date:     2021/5/9 1:44
  @ Description: 
  @ History:
"""
import sys
import json


def initialize(symbol):
    from listener.MainEntrance import Listener
    Listener(symbol)


def main():
    if len(sys.argv) > 1:
        input_ = sys.argv[1]
        config_file = f"{input_}.json"
        
        with open(config_file) as file:
            config_json = json.load(file)
            configs = dict()
            configs["symbol"] = config_json['DATA']['symbols']
            configs["channels"] = config_json['DATA']['channels']
            configs["silent"] = config_json['DATA']['silent']
            configs["influx_database"] = config_json['DATA']['influx_database']
    else:
        config_file = None

    from xuanwu.quant import quant
    quant.initialize(config_file)
    initialize(configs)
    quant.start()


if __name__ == '__main__':
    sys.path.append('/home/public/jason_turkey_trade/')
    main()
