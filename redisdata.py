#! /usr/bin/python
#-* coding: utf-8 -*

# System Package
import time

# 3rd Package
import redis

# Self Package

# CONST (Or consider as CONST)

# Global Arguments

# Logic

class RedisSet():
    def __init__(self, _config, _seek, _today):
        self.conn = redis.Redis(**_config)
        self.seek = _seek
        self.key = "{date}:uois".format(date = _today)
        self.pkey = "{date}:uois".format(date = _today.yesterday)

        self.len = self.len_set()
        self.data = self.get_set() if self.len<self.seek else self.get_set(self.seek, -1)
        self.seek = self.len

    # @timer
    def len_set(self):
        return self.conn.zcard(self.key)

    # @timer
    def get_set(self, start=0, end=-1):
        uois = self.conn.zrange(self.key, start, end)
        if start==0 and end == -1 and self.seek != 0:
            uois.extend(self.conn.zrange(self.pkey, self.seek, -1))
        return uois

    def get_hash(self, domain):
        return self.conn.hgetall(domain)


if __name__ == '__main__':
    from config import config
    from timer import Today
    redis_set = RedisSet(config.redis, 0, Today('2018-06-14'))

    for uoi in redis_set.data:
        hash_data = redis_set.conn.hgetall(uoi)
        print(hash_data)
        for key in hash_data:
            print("{} : {}".format(key, hash_data[key]))
