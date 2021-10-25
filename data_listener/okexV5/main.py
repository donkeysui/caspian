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
import yaml


def initialize(symbol):
    from listener.MainEntrance import Listener
    Listener(symbol)


def main():
    if len(sys.argv) > 1:
        input_ = sys.argv[1]
        config_file = f"{input_}.json"
        with open(config_file) as file:
            
            config_dict = json.load(file)
            config_dict = config_dict['DATA']
            configs = dict()
            configs["symbol"] = config_dict['symbol']
            configs["channels"] = config_dict['channels']
            configs["silent"] = config_dict['silent']
            configs["influx_database"] = config_dict['influx_database']
            configs["platform"] = config_dict["platform"]
    else:
        config_file = None

    from xuanwu.quant import quant
    quant.initialize(config_file)
    initialize(configs)
    quant.start()

if __name__ == '__main__':
    sys.path.append('/home/public/jason_turkey_trade/')
    main()
