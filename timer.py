#! /usr/bin/python
#-* coding: utf-8 -*

# System Package
import time
import datetime

# 3rd Package

# Self Package

# Logic

# TODAY       = time.strftime("%Y-%m-%d", time.localtime())
# TODAY       = "2018-06-13"

def timer(function):
    def _(*args, **kwargs):
        start = time.time()
        function(*args, **kwargs)
        __ = time.time() - start
        print('time spent: {}'.format(__))
    return _

class Today():
    def __init__(self, str=None):
        self.date = time.strftime("%Y-%m-%d", time.strptime(str, "%Y-%m-%d")) \
            if str else \
            time.strftime("%Y-%m-%d", time.localtime())

    def __str__(self):
        return self.date
    def __repr__(self):
        return self.date


    def yesterday(self):
        yesterday_str = (datetime.datetime.strptime(self.date, "%Y-%m-%d") - datetime.timedelta(days=1)).strftime("%Y-%m-%d")
        return Today(yesterday_str)

if __name__ == '__main__':
    a = Today()
    print(a.yesterday)