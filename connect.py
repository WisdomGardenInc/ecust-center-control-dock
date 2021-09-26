# -*- coding: utf-8 -*-
import time
from socket import *
from logger import log
from config import cfg
from cacheout import Cache

import pymysql
from apscheduler.schedulers.blocking import BlockingScheduler

space_config_list = {}
scheduler = BlockingScheduler()
cache = Cache(maxsize=128, ttl=300, timer=time.time, default=None)


def get_conditional_booking():
    db = pymysql.connect(host=cfg['roomis']['db']['host'], port=int(cfg['roomis']['db']['port']),
                         user=cfg['roomis']['db']['username'],
                         password=cfg['roomis']['db']['password'], database=cfg['roomis']['db']['database'],
                         charset="utf8")
    sql = """
         select 
             cs.name as space_name,
             cs.id as space_id
         from
             booking_event_item bei
         left join booking_event be on bei.event_id = be.id
         left join core_space cs on be.space_id = cs.id
         where bei.start_time >= DATE_ADD(now(), INTERVAL +480 MINUTE)
         and bei.org_id = 1
		 and bei.status = 'ACTIVE'
         and bei.date = DATE_FORMAT(now(), '%Y-%m-%d')
         and bei.start_time <= DATE_ADD(now(), INTERVAL +485 MINUTE)
    """.format(cfg['roomis']['orgid'])
    log.info("current execute log:{}".format(sql))
    cursor = db.cursor()
    data = []
    try:
        cursor.execute(sql)
        data = cursor.fetchall()
    except error as e:
        log.error(str(e))
    db.close()
    return data


def read_space_connect_config():
    space_center_config_info_list = cfg['room-devices']['center-control-info']
    if len(space_center_config_info_list) == 0:
        return space_config_list
    for space_config_info in space_center_config_info_list:
        element = space_config_info.split('#', 1)
        space_config_list[element[0]] = element[1]
    log.info("current space config list:{}".format(space_config_list))


def connect_roomis_center_control(spaces):
    log.info("current spaces:{}".format(spaces))
    if len(spaces) == 0:
        return
    for space in spaces:
        space_name = space[0]
        space_id = str(space[1])
        log.info("current space name:{}, space id:{}".format(space_name, space_id))
        if cache.get(space_id) is not None:
            log.info("space id:{} has connected in the past 5 minutes")
            continue
        if space_config_list[space_id] is None:
            log.info("space id:{} does not exist in the config")
            continue
        room_center_control_info = space_config_list[space_id]
        config_info = str(room_center_control_info).split("#", 1)
        center_control_host = str(config_info[0])
        center_control_command = str(config_info[1])
        client_socket = socket(AF_INET, SOCK_STREAM)
        client_socket.settimeout(int(cfg['room-devices']['connect-timeout']))
        client_socket.setblocking(True)
        for i in range(int(cfg['room-devices']['retry-times'])):
            try:
                log.info("ready to connect space's center control. space id:{}, control address:{}:5050"
                         .format(space_id, center_control_host))
                client_socket.connect((center_control_host, 5050))
                log.info("command which send to the center control:{}".format(center_control_command))
                client_socket.send(center_control_command.encode("utf-8"))
                data = client_socket.recv(128).decode("utf-8")
                log.info("current receive server response:{}".format(str(data)))
                cache.set(space_id, "1")
                break
            except ConnectionRefusedError as e:
                log.error('connect {} error. failed reason: {}'.format(space_config_list[space_id], str(e)))
                continue
            except TimeoutError as e:
                log.error('communication with {} failed. socket timeout error is catched. failed reason: {}'
                          .format(space_config_list[space_id], str(e)))
            except error as e:
                log.error('unknown error is catched. reason: {}'.format(str(e)))
        client_socket.close()


def schedule_job():
    log.info("start schedule job")
    read_space_connect_config()
    data = get_conditional_booking()
    connect_roomis_center_control(data)


if __name__ == '__main__':
    scheduler.add_job(schedule_job, 'cron', minute='0/1')
    scheduler.start()
