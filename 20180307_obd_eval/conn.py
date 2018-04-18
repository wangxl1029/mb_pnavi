#!/usr/bin/python3
#-*- coding=utf8 -*-
import pymongo
import logging

if __name__ == '__main__':
	print("这是主模块")
	host_ip = "1.2.3.4"
	port_num = "4567"
	print(host_ip, port_num)
	logging.basicConfig(filename='example.log',level=logging.DEBUG)
	logging.info("I told you so")
else:
	print("this is not main module.")
	