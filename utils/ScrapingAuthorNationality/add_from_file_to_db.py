from DB import *

datastore = DB()
filep = open('outfiles/outfile', 'r')

for line in filep:
	aid = line[:line.find('\t')]
	name = line[line.find('\t') + 1 : line.rfind('\t')]
	country = line[line.rfind('\t') + 1:].strip()
	if not datastore.exists(aid):
		datastore.insert(aid, name, country)