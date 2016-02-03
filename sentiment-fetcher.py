import re
import time
import json
from io import open
from pprint import pprint
from py2neo import authenticate, Graph, Node, Relationship

import pycurl
from StringIO import StringIO
from urllib import urlencode

authenticate("localhost:7474", "", "")
sgraph = Graph("http://localhost:7474/db/data/")

tipPaths = ['Tips/LA/LA-tips.txt', 'Tips/NYC/NYC-Tips.txt']

def main():
	getTipsSentiment()

def getTipsSentiment():
	print "get tips sentiment"
	wFile = open("tips-ny", "w")
	ban = True

	for path in tipPaths:
		with open(path, errors='ignore') as f:
			for line in f:			
				# userId, venueId, null, Comment
				# p[0], p[1], p[2], p[3]
				
				#p = line.replace("\r", "").replace("\n", "").replace("\"", "").split("\t");
				p = line.split("\t");
				
				if (len(p) == 2):
					p[3] == ""
				elif (len(p) < 2):
					print "no user/s"
					continue

				userId = p[0]

				if ban == True and userId == "1213061":
					ban = False
				else:
					print userId

				if ban == True:
					continue 

				tips = []
				line = ""
				
				for x in range (0, len(p)):
					if x+2 < len(p) and p[x+2] == "null":
						#print line
						#print ""
						#time.sleep(0.5)
						tips.append(line)
						line = userId + "\t"
					else:
						line += p[x] + "\t"
			
				for tip in tips:
					p = tip.replace("\r", "").replace("\n", "").replace("\"", "").split("\t");
					buffer = StringIO()
					c = pycurl.Curl()
					c.setopt(c.URL, 'https://twinword-sentiment-analysis.p.mashape.com/analyze/')
					c.setopt(pycurl.HTTPHEADER, 
						['X-Mashape-Key: tGymXYpdjpmshzZvFLohbZGIyZG3p1pOPyIjsno1fsyePwOF3D',
	         		    'Content-Type: application/x-www-form-urlencoded',
	  					'Accept: application/json'])
					
					try:
						post_data = {'text': p[3]}
						postfields = urlencode(post_data)
					except:
						continue

					c.setopt(c.POSTFIELDS, postfields)
					c.setopt(c.WRITEDATA, buffer)
					try:
						c.perform()
						c.close()
					
						body = buffer.getvalue()
						res = json.loads(body)
						print(p[0], res['type'], res['score'], res['ratio'])
						
						wFile.write(tip + str(res['type']) + "\t" + str(res['score']) + "\t" + str(res['ratio']) + "\n")
					except:
						print "failed to call sentiment api"
						continue

	wFile.close()


main()