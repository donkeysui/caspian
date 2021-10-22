# -*- coding: utf-8 -*-
"""
  @ Author:   Mr.Hat
  @ Email:    shenghong6560@gmail.com
  @ Date:     2021/5/9 3:42
  @ Description: 
  @ History:
"""
import configparser

d = [
    "CHZUSD",
    "SOLUSD",
    "SRMUSD",
    "UNIUSD",
    "MATICUSD",
    "SUSHIUSD",
    "EOSUSD",
    "XRPUSD",
    "DOGEUSD"
]

cf = configparser.ConfigParser()


def config_create(name1, name2):
    section_name = f"program:{name1}"
    cf.add_section(section_name)
    cf.set(section_name, "directory", "/root/ftx_strategy")
    cf.set(section_name, "autostart", "false")
    cf.set(section_name, "startsecs", "5")
    cf.set(section_name, "autorestart", "true")
    cf.set(section_name, "startretries", "3")
    cf.set(section_name, "user", "root")
    cf.set(section_name, "redirect_stderr", "true")
    cf.set(section_name, "stdout_logfile_maxbytes", "20MB")
    cf.set(section_name, "stdout_logfile_backups", "20")
    cf.set(section_name, "command", f"python main.py config.{name2}")
    cf.set(section_name, "stdout_logfile", f"/root/ftx_strategy/log/{name1}.log")
    cf.set(section_name, "stderr_logfile", f"/root/ftx_strategy/log/{name1}.err")

    with open("ftx_ticker_count.ini", "w+") as file:
        cf.write(file)


if __name__ == '__main__':
    for i in d:
        q = i.split("USD")[0]
        s = f"{q.lower()}-usd"
        s1 = f"{q.lower()}_usd"
        print(f"[program:{s1}]-{s}-{s1}")
        config_create(s1, s)
