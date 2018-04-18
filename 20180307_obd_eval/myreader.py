#!/usr/bin/python3
# -*- coding=utf-8 -*-
import configparser
import pymongo



def dump_gpsinfo():
	pass

def dump_track(t):
	print(t['header'])

config=configparser.ConfigParser()

if config.read('myreader.ini')!= []:
	print(config.sections())
else:
	print('configure ini file error!')
	exit(-1)

host_port = config['mongo']['port']
host_ip = config['mongo']['host']

print("mongo host is {}:{}".format(host_ip, host_port))

db_name = config['mongo']['database']
colle_name_trk = config['mongo']['colle_tracks']
colle_name_jny = config['mongo']['colle_journeys']

print(db_name, colle_name_trk, colle_name_jny)

mg_client = pymongo.MongoClient(host_ip, int(host_port))
db = mg_client.get_database(db_name)

ct = db.get_collection(colle_name_trk)
cj = db.get_collection(colle_name_jny)

itemcnt = 0
for curitem in cj.find():
	if isinstance(curitem, dict):
		guid = curitem['guid']
		#print("guid[{}] = {}".format(itemcnt, guid))
		#infonum = len(curitem[])
		itemcnt += 1
	
	#dump_journey(curitem)

for curtrk in ct.find():
	dump_track(curtrk)


print("item count is {}".format(itemcnt))

mg_client.close()


