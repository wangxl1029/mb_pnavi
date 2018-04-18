#/usr/bin/python3
# -*- coding=utf8 -*-

import configparser
import pymongo
import json
import io
import os
import getopt
import sys


def getMongoHost():
	config=configparser.ConfigParser()

	if config.read('myreader.ini')!= []:
		print(config.sections())
	else:
		print('configure ini file error!')
		exit(-1)

	#fjson = open("mghost.json", "w", encoding="utf-8")
	mg_host = {\
		'IP':config['mongo']['host'],\
		'port':int(config['mongo']['port'])\
	}

	#print(json.dumps(mg_host, indent=4), file=fjson)
	#fjson.write(json.dumps(mg_host, indent=4))
	#fjson.close()

	mg_name = {\
		'db': config['mongo']['database'],\
		'colle_tracks': config['mongo']['colle_tracks'],\
		'colle_journeys' : config['mongo']['colle_journeys'],\
		'colle_learning_outcome' : config['mongo']['colle_learning_outcome'],\
	}

	return (mg_host, mg_name)


def dump_journey(sel_idx):
	mg_hst, mg_nam = getMongoHost()	
	#print('mongo host as json:')
	#print(json.dumps(mg_hst, indent=4))
	#print('mongo names as json:')
	#print(json.dumps(mg_nam, indent=4))
	#mongo client stuff
	mg_client = pymongo.MongoClient(mg_hst['IP'], mg_hst['port'])
	db = mg_client.get_database(mg_nam['db'])
	cj = db.get_collection(mg_nam['colle_journeys'])
	'''
	tracks_txt = open("tracks.txt", "w", encoding="utf-8")
	for cur in ct.find():
		print(cur, file = tracks_txt)
	tracks_txt.close()
	'''
	cnt_j = 0
	isFound = False
	for cur in cj.find():
		if sel_idx == cnt_j:
			if isinstance(cur, dict):
				journeys_jsn = open("journey_" + str(cnt_j) + ".json", "w", encoding="utf-8")
				cur['_id'] = 'na'
				journeys_jsn.write(json.dumps(cur, indent=4))
				journeys_jsn.close()
				isFound = True
		cnt_j += 1
	if not isFound:
		print("Target #{} is not found!".format(sel_idx))
	print("All journey number is {}".format(cnt_j))
	#tracks_json = open("tracks.json", "w", encoding="utf-8")
	#for cur in ct.find():
	#	if isinstance(cur, dict):
	#		tracks_json.write(json.dumps(cur, indent=4))
	mg_client.close()

def dump_track(sel_idx):
	mg_hst, mg_nam = getMongoHost()	
	mg_client = pymongo.MongoClient(mg_hst['IP'], mg_hst['port'])
	db = mg_client.get_database(mg_nam['db'])
	ct = db.get_collection(mg_nam['colle_tracks'])
	cnt_t = 0
	isTooMany = False
	isFound = False
	for cur in ct.find():
		if sel_idx == cnt_t:
			if isinstance(cur, dict):
				tracks_jsn = open("track_" + str(cnt_t) + ".json", "w", encoding="utf-8")
				cur['_id'] = 'na'
				tracks_jsn.write(json.dumps(cur, indent=4))
				tracks_jsn.close()
				isFound = True
		cnt_t += 1
		if cnt_t > 1000:
			isTooMany = True
			break
	mg_client.close()
	if not isFound:
		print("Target #{} is not found!".format(sel_idx))
	if isTooMany:
		print('Record is too many stop at #{}!'.format(cnt_t))
	else:
		print("All track number is {}.".format(cnt_t))

def dump_learning_outcome(sel_idx):
	mg_hst, mg_nam = getMongoHost()	
	mg_client = pymongo.MongoClient(mg_hst['IP'], mg_hst['port'])
	db = mg_client.get_database(mg_nam['db'])
	lo = db.get_collection(mg_nam['colle_learning_outcome'])
	cnt_lo = 0
	isTooMany = False
	isFound = False
	for cur in lo.find():
		if sel_idx == cnt_lo:
			if isinstance(cur, dict):
				learning_outcome_jsn = open("learning_outcome_" + str(cnt_lo) + ".json", "w", encoding="utf-8")
				cur['_id'] = 'na'
				learning_outcome_jsn.write(json.dumps(cur, indent=4))
				learning_outcome_jsn.close()
				isFound = True
		cnt_lo += 1
		if cnt_lo > 1000:
			isTooMany = True
			break
	mg_client.close()
	if not isFound:
		print("Target #{} is not found!".format(sel_idx))
	if isTooMany:
		print('Record is too many stop at #{}!'.format(cnt_lo))
	else:
		print("All track number is {}.".format(cnt_lo))

def exportJourneyGuids():
	mg_hst, mg_nam = getMongoHost()	
	mg_client = pymongo.MongoClient(mg_hst['IP'], mg_hst['port'])
	db = mg_client.get_database(mg_nam['db'])
	cj = db.get_collection(mg_nam['colle_journeys'])
	cnt_j = 0
	isFound = False
	for cur in cj.find():
		if isinstance(cur, dict):
			cnt_j += 1
		if cnt_j > 100:
			break
	mg_client.close()
	return cnt_j		

class myUsage:
	"""docstring for myUsage
	"""
	def __init__(self, arg):
		super(myUsage, self).__init__()
		self.arg = arg
	def __init__(self):
		self.shortopts = 'hj:l:t:'
		self.longopts = [\
			"help", "journey=", "track=", \
			"export-journey-guids", \
			"export-lo-guids", \
		]
	def usage():
		print('''\
Usage: dump2json [OPTIONS]
	-h	--help
	-j #	--journey=#
	-t #	--track=#
''')

if __name__ == '__main__':
	print(sys.argv[0], os.path.basename(sys.argv[0]))
	usg = myUsage()
	try: 
		opts, args = getopt.getopt(sys.argv[1:], usg.shortopts, usg.longopts)
	except getopt.GetoptError as err:
		print(err)
		usage()
		sys.exit(2)
	sel_idx = 0
	for o, a in opts:
		if o in ("-h", "--help"):
			usg.usage()
		elif o in ("-j", "--journey"):
			sel_idx = int(a)
			print('dump journey at #{} ...'.format(sel_idx))
			dump_journey(sel_idx)
		elif o in ("-l", "--learning"):
			sel_idx = int(a)
			print('dump learning outcome at #{} ...'.format(sel_idx))
			dump_learning_outcome(sel_idx)
		elif o in ("-t", "--track"):
			sel_idx = int(a)
			print('dump track at #{} ...'.format(sel_idx))
			dump_track(sel_idx)
		elif o == '--export-journey-guids':
			j_guid_num = exportJourneyGuids()
			print('All guid number in target journey is {}'.format(j_guid_num))
		else:
			#usg.usage()
			exit()
