from URL import *
from threading import Thread, Lock
import os
import sys

class Scrape(Thread):

	def __init__(self, infile, outfile, lock):
		Thread.__init__(self)
		self.infile = infile
		self.outfile = outfile
		self.lock = lock

	def run(self):
		readFile = open(self.infile, 'r')
		for name in readFile:
			authorId = name[:name.find('\t')].strip()
			authorName = name[name.find('\t') + 1:].strip()
			surname = authorName[authorName.rfind(' ') + 1:].strip()
			u = URL('http://forebears.io/surnames/' + surname, authorId)
			self.outfile.write(authorId + '\t' + authorName + '\t' +str(u.fetch()) + '\n')

# for x in range(1, 4):
# 	Scrape(x).start()


def main():
	sys.stdout = open('outfiles/log', 'w')

	outfile = open('outfiles/outfile', 'w');
	lock = Lock()
	threadList = []

	infiles = os.listdir('infiles/')

	for infile in infiles:
		thr = Scrape('infiles/' + infile, outfile, lock).start()
		threadList.append(thr)

if __name__ == '__main__':
	main()