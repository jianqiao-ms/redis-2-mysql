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
        pool = redis.ConnectionPool(**_config)
        self.conn = redis.Redis(connection_pool=pool)
        # self.conn = redis.Redis(**_config)
        self.redis_pipe = self.conn.pipeline(transaction=True)
        self.seek = _seek
        self.key = "{date}:uois".format(date = _today)
        self.pkey = "{date}:uois".format(date = _today.yesterday())

        self.lenth = self.len_set()
        self.uois = self.range_uois() if self.lenth<self.seek else self.range_uois(self.seek, -1)
        self.seek = self.lenth

    def len_set(self):
        return self.conn.zcard(self.key)

    def range_uois(self, start=0, end=-1):
        uois = self.conn.zrange(self.key, start, end)
        if start==0 and end == -1 and self.seek != 0:
            yesterday_uois = self.conn.zrange(self.pkey, self.seek, -1)
            self.lenth+=len(yesterday_uois)
            uois.extend(yesterday_uois)
        return uois

    def get_hash(self):
        # result = list()
        for uoi in self.uois:
            self.redis_pipe.hgetall(uoi)
            # result.append(self.conn.hgetall(uoi))
        # return map(self.conn.hgetall, self.uois)
        # map(self.redis_pipe.hgetall, self.uois)
        result = self.redis_pipe.execute()
        return result

    def get_hash_list(self, GROUPLEN=32):
        result = list()
        group = int( self.lenth / GROUPLEN )
        endfix = self.lenth % GROUPLEN

        print(self.lenth)
        print(group)
        print(endfix)
        print('================\n')

        cmd = "local rst={}; for i,v in pairs(KEYS) do rst[i]=redis.call('hgetall', v) end; return rst"

        for i in range(group):
            result.extend(self.conn.eval(cmd,
                                         GROUPLEN,
                                         *self.uois[i*GROUPLEN:(i+1)*GROUPLEN]))
            print('{:10}:{}'.format(i,len(result)))

        result.extend(self.conn.eval(cmd,
                                         endfix,
                                         *self.uois[group*GROUPLEN:]))

        # return self.conn.eval(cmd,
        #                              30,
        #                              "0802c489-746d-4247-9a08-b682317470fc", "c62c9c56-167e-49f7-8409-7d2cc0750b71",
        #                               "fdcb4cc8-a78c-4ee8-9b64-d591e6d7ccef", "c44ace29-d7fc-4752-a208-329292ed35bd",
        #                               "2cd6487c-a7d1-4c6f-a35f-52ed5f109211", "d866eb60-247f-4470-b28d-736572d3ef6b",
        #                               "65672df7-ca52-457e-a16f-89da54e4ac81", "8877c298-5ea2-476d-971d-37b989383655",
        #                               "b1ce5b49-9bcb-4614-8989-19feee4d91c1", "1d1ec9a8-5789-4d27-bc19-51cea689fa65",
        #                               "406b009d-b3d5-4ae6-b8ce-feb0599d0646", "53e37255-ecef-477d-9792-8d576841be32",
        #                               "b5d3789d-15d6-4a8c-a83d-ab2a6d21cd79", "010faf00-d313-4230-8d58-ee118b89b460",
        #                               "3a503e68-d5ed-4ed4-b84a-85c6fec69ee0", "b15fe4fb-3c50-4518-bcef-8c27d3d457a5",
        #                               "24b1831f-0aec-4135-9257-3fb39eea6b1c", "fe515e3f-bd2b-44fa-a153-63a1e19b2a5e",
        #                               "b13e3d0b-6ee7-48a0-b95a-c4c9eae7dd2e", "93cf212f-5f1c-4ef5-b71c-9278580173cf",
        #                               "c9d1984c-b4aa-41c8-80e4-e19ac057652b", "491a88c1-8d00-4a80-a7aa-17dfc4ef49fb",
        #                               "cba2c82f-2a31-45e9-908d-9ca6dcfa3f0f", "5fb49be4-aa29-42c0-ac51-3c3faa34c62e",
        #                               "6ed47a55-d501-4964-8a30-29b97f8a2ad7", "fbbe3faa-6e70-4087-8be2-2319f2f30b24",
        #                               "87e9219e-5461-4a65-a4e8-04e5c1922e2e", "010830cc-ff10-4920-8925-4efa2023b95f",
        #                               "5e73f3a3-0339-4218-8ccb-b6aad20dd78c", "c61fd6ef-1856-4b5b-a04b-ccc926270bf8"
        #                              )
        return result

if __name__ == '__main__':
    from config import config
    from timer import Today
    redis_set = RedisSet(config.redis, 0, Today('2018-06-14'))

    for uoi in redis_set.data:
        hash_data = redis_set.conn.hgetall(uoi)
        print(hash_data)
        for key in hash_data:
            print("{} : {}".format(key, hash_data[key]))
