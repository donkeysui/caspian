# -*- coding: utf-8 -*-
"""
  @ Author:   Mr.Hat
  @ Email:    shenghong6560@gmail.com
  @ Date:     2021/4/25 9:47
  @ Description: 
  @ History:
"""
import sys


def initialize(symbol):
    from strategys.current_arbitrage import CurrentArbitrage
    CurrentArbitrage(symbol)


def main():
    if len(sys.argv) > 1:
        input_ = sys.argv[1].split(".")
        config_file = f"{input_[0]}.json"
        symbol = input_[1]
    else:
        config_file = None
        symbol = "BTC-PERP"

    from xuanwu.quant import quant
    quant.initialize(config_file)
    initialize(symbol)
    quant.start()


if __name__ == '__main__':
    main()
