import re
import time
import json
from io import open
from pprint import pprint
from py2neo import authenticate, Graph, Node, Relationship

import pycurl
from StringIO import StringIO
from urllib import urlencode
import datetime

authenticate("localhost:7474", "", "")
sgraph = Graph("http://localhost:7474/db/data/")
batchSize = 1000

userPaths = ['Users/LA/LA-Users.txt', 'Users/NYC/NYC-Users.txt']
friendshipPaths = ['Friendship/LA/LA-relations.txt', 'Friendship/NYC/NYC-user-relations.txt']
venuePaths = ['Venues/LA/LA-Venues.txt', 'Venues/NYC/NYC-Venues.txt']
tipPaths = ['Tips/LA/LA-Tips.txt', 'Tips/NYC/NYC-Tips.txt']
catPath = 'categories.txt'

def checkDates():
	dates = []
	for path in tipPaths:
		with open(path) as f:
			batch = sgraph.cypher.begin()
				
			for line in f:			
				
				p = line.replace("\r", "").replace("\n", "").replace("\"", "").split("\t");
				userId = p[0]
				tips = []
				line = ""
				
				for x in range (0, len(p)):
					if x+2 < len(p) and p[x+2] == "null":
						tips.append(line)
						line = userId + "\t"
					else:
						line += p[x] + "\t"
			
				for tip in tips:
					t = tip.replace("\r", "").replace("\n", "").replace("\"", "").split("\t");
					
					try:
						newDate = datetime.datetime.fromtimestamp(int(t[4])).strftime('%Y-%m-%d')
						exists = False
						for date in dates:
							if date == newDate:
								exists = True
								break
						if exists != False:
							dates.append(date)
						time.sleep(0.5)
					except:
						continue
	print dates
	print len(dates)
checkDates()