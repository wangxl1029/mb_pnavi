#!/usr/bin/python3
# -*- coding=utf-8 -*-
import configparser
import pymongo
import os
import sys
import logging
import pickle
import shelve
import math
import numpy as np
from sklearn.cluster import DBSCAN 

conf=configparser.ConfigParser()
logor=logging.getLogger()
logor.setLevel(logging.DEBUG)
#logor.addHandler(logging.NullHandler)

def GetFileNameAndExt(filename):
	filepath,tempfilename = os.path.split(filename)
	shortname,extname = os.path.splitext(tempfilename)
	return shortname,extname

def GetIniFilePath():
	basname,ignore=GetFileNameAndExt(sys.argv[0])
	return basname + ".ini"
# copyright. ZhengXG
def outOfChina(lat, lon):
	return not (72.004 <= lon <= 137.8347 and 0.8293 <= lat <= 55.8271)
# copyright. ZhengXG
def haversine(gps1, gps2):
    if gps1.shape != (2,):
        # print("====", type(gps1), gps1.shape, gps2.shape, gps1)
        return 1000

    latA, lngA = gps1
    latB, lngB = gps2

    pi180 = math.pi / 180
    arcLatA = latA * pi180
    arcLatB = latB * pi180
    x = (math.cos(arcLatA) * math.cos(arcLatB) *
         math.cos((lngA - lngB) * pi180))
    y = math.sin(arcLatA) * math.sin(arcLatB)
    s = x + y
    if s > 1:
        s = 1
    if s < -1:
        s = -1
    alpha = math.acos(s)
    distance = alpha * 6378.137

    return distance

class CJobPersistence:
	def __init__(self):
		self.guard_num	= int(conf['thresholds']['guard_num'])
		self.min_gpsnum	= int(conf['thresholds']['min_gpsnum'])
		self.joblst = []
		self.dbUserIdList = []
		basname,_=GetFileNameAndExt(sys.argv[0])
		self.userIdPckPath = "userids.pkl"
		self.mainPckPath = self.GetPickleFilePath()
		self.dic = shelve.open(basname + '.shv')
#		self.mainDict = {}
		
	'''
	def __del__(self):
		self.dic.close()

	def loadMainly(self):
		if {} == self.mainDict:
			with open(mainPckPath, "rb") as f:
				self.mainDict = pickle.load(f)

	def saveMainly(self, dbCurTrkIdx):
		self.mainDict = {'curTrkIdx' : dbCurTrkIdx}
		with open() as f:
			pickle.save(self.mainDict, f)
	'''
	def loadUserIds(self):
		if [] == self.dbUserIdList and os.path.exists(self.userIdPckPath):
			with open(self.userIdPckPath, 'rb') as f:
				self.dbUserIdList = pickle.load(f)

	def saveUserIds(self):
		if not [] == self.dbUserIdList:
			with open(self.userIdPckPath, 'wb') as f:
				pickle.dump(self.dbUserIdList, f)

	def updateUserIds(self, IdListGenCallable):
		isUpdated = False
		if [] == jp.dbUserIdList:
			self.dbUserIdList = IdListGenCallable()
			self.saveUserIds()
			isUpdated = True
		return isUpdated

	def GetPickleFilePath(self):
		basname,ignore=GetFileNameAndExt(sys.argv[0])
		return basname + ".pkl"

	def syncTrackNums(self, track_num, valid_track_num, track_allnum):
		self.dic['dbTrackNum']	= track_num
		self.dic['dbValidTrackNum']	= valid_track_num
		self.dic['dbAllTrackNum']	= track_allnum
		self.dic.sync()

class CPosList:
	def __init__(self):
		self.lst = []
	def addpos(self, lon, lat):
		self.lst += [(lat, lon)]

def checkTrack(cur_tk):
			gpscnt=len(cur_tk['gpsInfo'])
			posList = CPosList()
			if gpscnt >= jp.min_gpsnum:
				available_cnt += 1
				for gps in cur_tk['gpsInfo']:
					if outOfChina(lon=gps['longitude'], lat=gps['latitude']):
						illegal_cnt += 1
					else:
						posList.addpos(lon=gps['longitude'], lat=gps['latitude'])
				#print(f'track#{track_cnt} all gps number is {len(posList.lst)}')
				if [] != posList.lst and 0 == illegal_cnt:
					cluster = DBSCAN(eps=0.5, min_samples=6,metric=haversine)
					#cluster = DBSCAN(eps=0.5, min_samples=6)
					#print(posList.lst)
					d = np.array(posList.lst)
					db = cluster.fit(d)
					labels = db.labels_
					labels_set = set(labels)
					n_clusters_ = len(labels_set) - (1 if -1 in labels else 0)
					if 1 != n_clusters_:
						print(f'There are(is) {n_clusters_} cluster(s).')
						print(labels_set)
					else:
						ct_ft.insert_one(cur_tk)
				else:
					print('postion list is empty!')
			del posList

class CEvalSession:
	def __init__(self, host_addr, host_port, db_name):
		self._client = pymongo.MongoClient(host_addr, int(host_port))
		self._db = self._client.get_database(db_name)
		self._src = None	# source collection
		self._dst = None	# destination collection
	def __del__(self):
		self._client.close()
	def initCollection(self, src_name, dst_name):
		if None == self._src:
			self._src = self._db.get_collection(src_name)
		if None == self._dst:
			self._dst = self._db.get_collection(dst_name)
	def getUniqueSrcUserIdList(self):
		return self._src.distinct("header.userid")

	def getDbTrackNum(self, userid = None, srcflag = True):
		if srcflag:
			return self._src.count({}) if None == userid else self._src.count({'header.userid':userid})
		else:
			return self._dst.count({}) if None == userid else self._dst.count({'header.userid':userid})
	def checkIntegrity(self):
		pass
	def doJob(self):
		return False

if __name__ == '__main__':
	iniFile=GetIniFilePath()
	if conf.read(iniFile) == []:
		print(f'Init file \"{iniFile}\" read error!')
		exit(-1)
	print(f'Init file \"{iniFile}\" read OK.')
	logor.info('Init file \"%s\" read OK.', iniFile)
	host_port = conf['mongo']['host_port']
	host_addr = conf['mongo']['host_addr']

	print("mongo host is {}:{}".format(host_addr, host_port))

	db_name = conf['mongo']['database']
	colle_name_trk = conf['mongo']['colle_tracks']
	colle_name_trk_filtered = conf['mongo']['colle_tracks_filtered']
	print("db name : {}, track name :{}".format(db_name, colle_name_trk))
#	colle_name_trk_filtered = colle_name_trk + '_filtered'
	print("track filtered name :{}".format(colle_name_trk_filtered))

	#mg_client = pymongo.MongoClient(host_addr, int(host_port))
	#db = mg_client.get_database(db_name)
	sess = CEvalSession(host_addr, host_port, db_name)
	#ct = db.get_collection(colle_name_trk)
	#ct_ft = db.get_collection(colle_name_trk_filtered)
	#ct_ft.drop()
	sess.initCollection(colle_name_trk, colle_name_trk_filtered)
	jp = CJobPersistence()
	jp.loadUserIds()
	if jp.updateUserIds(sess.getUniqueSrcUserIdList):
	#if [] == jp.dbUserIdList:
	#	jp.dbUserIdList = ct.distinct("header.userid")
	#	jp.saveUserIds()
		print("user IDs saved!")
	else:
		print("user IDs loaded!")
	dbCurDevIdx = 0 if not 'dbCurDevIdx' in jp.dic else jp.dic['dbCurDevIdx']
	print(f'Process by device num is {dbCurDevIdx+1}/{len(jp.dbUserIdList)}.')
	#track_allnum = ct.count({}) if not 'dbAllTrackNum' in jp.dic else jp.dic['dbAllTrackNum']
	track_allnum = sess.getDbTrackNum() if not 'dbAllTrackNum' in jp.dic else jp.dic['dbAllTrackNum']
	track_num = 0 if not 'dbTrackNum' in jp.dic else jp.dic['dbTrackNum']
	valid_track_num = 0 if not 'dbValidTrackNum' in jp.dic else jp.dic['dbValidTrackNum']
	#def _syncGuard():
	#	jp.dic['dbTrackNum']	= track_num
	#	jp.dic['dbValidTrackNum']	= valid_track_num
	#	jp.dic['dbAllTrackNum']	= track_allnum
	#	jp.dic.sync()

	#check previous insertion
	#filtered_num = ct.count({'header.userid':jp.dbUserIdList[dbCurDevIdx]})
	sess.checkIntegrity()
	#filtered_num = sess.getDbTrackNum(jp.dbUserIdList[dbCurDevIdx])
	#if filtered_num > 0:
	#	pass
	#isKeyboardInterrupt = False
	isKeyboardInterrupt = sess.doJob()
	'''
	try:
		for dev_idx in range(dbCurDevIdx, len(jp.dbUserIdList)):
			track_cnt = 0
			available_cnt = 0
			illegal_cnt = 0
			#for cur_tk in ct.find({'header.userid':jp.dbUserIdList[dev_idx]}):
			for cur_tk in ct.find({'header.userid':jp.dbUserIdList[dev_idx]}):
				checkTrack(cur_tk)
			if 0 == track_cnt % jp.guard_num:
				#_syncGuard()
				jp.syncTrackNums(track_num, valid_track_num, track_allnum)
			track_cnt+=1
			jp.dic['dbCurDevIdx']	= dev_idx
			jp.dic.sync()
			track_num += track_cnt
			valid_track_num += available_cnt
			print(f'dev{dev_idx}:user#{jp.dbUserIdList[dev_idx]} trackcnt#{available_cnt}/{track_cnt} bad gps#{illegal_cnt}')
	except KeyboardInterrupt:
		isKeyboardInterrupt = True
	'''
	if isKeyboardInterrupt:
		print('Key board interrupt!')
	print(f'All track number is {track_allnum}.')
	print(f'Guard number is {jp.guard_num}.')
	
	#jp.dic['dbCurDevIdx']	= dev_idx
	jp.syncTrackNums(track_num, valid_track_num, track_allnum)
	#_syncGuard()
	#mg_client.close()
	print(f'{track_num} track(s) done. {valid_track_num} is available.')
