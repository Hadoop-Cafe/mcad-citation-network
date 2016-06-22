from URL import *
from DB import *
from threading import Thread
import os
import sys

class Scrape(Thread):

	def __init__(self, infile):
		Thread.__init__(self)
		self.infile = infile

	def run(self):
		datastore = DB();

		readFile = open(self.infile, 'r')

		count = 0
		for line in readFile:
			count = count + 1

		readFile = open(self.infile, 'r')
		i = 0;
		for line in readFile:
			i = i + 1
			authorId = line[:line.find('\t')].strip()

			if not datastore.exists(authorId):
				authorName = line[line.find('\t') + 1:].strip()
				surname = authorName[authorName.rfind(' ') + 1:].strip()
				u = URL('http://forebears.io/surnames/' + surname, authorId)
				datastore.insert(authorId, authorName, u.fetch())				
			print 'Thread ' + self.infile[8:] + ' is ' + str(i * 100.0 / count) + '% completed..'

def main():
	threadList = []

	infiles = os.listdir('infiles/')

	for infile in infiles:
		thr = Scrape('infiles/' + infile).start()
		threadList.append(thr)

if __name__ == '__main__':
	main()