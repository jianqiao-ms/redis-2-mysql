#! /usr/bin/python
#-* coding: utf-8 -*

# System Package
import time

# 3rd Package
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

# Self Package
from config import config
from config import NewDict as dict
from timer import Today
from redisdata import RedisSet
from mysqldata import TimeLine

# CONST (Or consider as CONST)
mysql_session = sessionmaker(bind = create_engine(
    'mysql+mysqlconnector://{user}:{passwd}@{host}:{port}/{database}'.format(**config.mysql)))()

default_record = dict(
    id                      = None,
    unique_order_id         = None,
    qrcode1_generated_at    = None,
    qrcode1_scanned_at      = None,
    landingpage_opened_at   = None,
    sms_requested_at        = None,
    sms_delivered_at        = None,
    sms_delivered_status    = None,
    sms_delivered_message   = None,
    submit_at               = None,
    qrcode2_generated_at    = None,
    # create_at               = '',
    # update_at               = ''
)

# Global Arguments
redis_set = RedisSet(config.redis, 0, Today())

# Logic

if __name__ == '__main__':
    for uoi in redis_set.data:
        _ = default_record
        _.unique_order_id = uoi


        _.update(redis_set.get_hash(uoi))
        print(_)
        print('=====================\n\n')

        print(_['sms_delivered_message'])
        print(type(_['sms_delivered_message']))

        # for key in _:
        #     print("{}:{}".format(key, _[key]))
        #
        #
        # print('=====================\n\n')
        #
        # print(_['sms_delivered_message'])
        # print(type(_['sms_delivered_message']))
        #
        print('=====================\n\n')


        a = TimeLine(**_)
        print(a)





        mysql_session.add(a)
        break
    mysql_session.commit()

    # a = mysql_session.query(TimeLine).all()
    # print(a)