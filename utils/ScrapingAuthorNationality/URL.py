import urllib2
import time
from bs4 import BeautifulSoup
import sys

class URL:

    def __init__(self, url, authid):
        self.url = url
        self.authid = authid
        proxy = urllib2.ProxyHandler({'http': 'iit2013204:404104211@172.31.1.6:8080'})
        opener = urllib2.build_opener(proxy)
        urllib2.install_opener(opener)

    def fetch(self):
        # print 'Attempting url fetch.'
        OK = False
        tries = 0
        while not OK:
            try:
                response = urllib2.urlopen(self.url)
                # print 'Success : ' + self.url
                OK = True
            except urllib2.HTTPError as e:
                tries = tries + 1
                if tries >= 256:
                    return -1
                # print 'Failure.\nAn HTTP error occured : ' + str(e.code)
                # print 'Refetching : ' + self.url
            time.sleep(2)
        html = response.read()

        try :
            soup = BeautifulSoup(html, "html.parser")
            tr = soup.find_all('tr')
            td = tr[1].find('td')
            return td.text
        except:
            return 'err'
