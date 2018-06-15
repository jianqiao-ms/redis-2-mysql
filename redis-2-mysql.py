#! /usr/bin/python
#-* coding: utf-8 -*

# System Package
import time

# 3rd Package
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import IntegrityError

# Self Package
from config import config
from config import NewDict as dict
from timer import Today
from timer import timer
from redisdata import RedisSet
from mysqldata import TimeLine

# CONST (Or consider as CONST)
mysql_conn =  create_engine(
        'mysql+pymysql://{user}:{passwd}@{host}:{port}/{database}?charset=utf8'.format(**config.mysql),
        # echo = True
    ).connect()
InsertSql = TimeLine.__table__.insert()
redis_set = RedisSet(config.redis, config.seek.redis, Today().yesterday())
default_record = dict()
map(lambda x:default_record.__setitem__(x,None), TimeLine.__table__.columns.keys())
# default_record = dict(
#     id                      = None,
#     unique_order_id         = None,
#     qrcode1_generated_at    = None,
#     qrcode1_scanned_at      = None,
#     landingpage_opened_at   = None,
#     sms_requested_at        = None,
#     sms_delivered_at        = None,
#     sms_delivered_status    = None,
#     sms_delivered_message   = None,
#     submit_at               = None,
#     qrcode2_generated_at    = None,
#     create_at               = None,
#     update_at               = None
# )


# Class & Functions
def dump(hash):
    _ = default_record
    # _.unique_order_id = uoi
    _.update(hash)
    try:
        mysql_conn.execute(InsertSql, **_)
    except IntegrityError as e:
        pass

# Logic
if __name__ == '__main__':
    start = time.time()

    # print(redis_set.get_hash()[0])
    redis_set.get_hash()

    # map(dump, redis_set.get_hash_list())
    # mysql_conn.close()
    print(time.time() - start)
