#/usr/bin/python3
# -*- coding=utf8 -*-



import getopt

def usage():
	print("""\
Usage : 
	""")

if __name__ == '__main__':
	try: 
		opts, args = getopt.getopt(sys.argv[1:], 'hj:l:t:', ["help", "journey=", "track="])
	except getopt.GetoptError as err:
		print(err)
		usage()
		sys.exit(2)
