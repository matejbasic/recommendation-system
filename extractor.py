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
batchSize = 1000

userPaths = ['Users/LA/LA-Users.txt', 'Users/NYC/NYC-Users.txt']
friendshipPaths = ['Friendship/LA/LA-relations.txt', 'Friendship/NYC/NYC-user-relations.txt']
venuePaths = ['Venues/LA/LA-Venues.txt', 'Venues/NYC/NYC-Venues.txt']
tipPaths = ['Tips/LA/LA-tips.txt', 'Tips/NYC/NYC-Tips.txt']
catPath = 'categories.txt'

categoriesAll = []

def setConstraints():
	print "CONSTRAINTS"
	#CREATE CONSTRAINT ON (a:Venue) ASSERT a.oid IS UNIQUE
	#CREATE CONSTRAINT ON (a:Category) ASSERT a.oid IS UNIQUE
	#CREATE CONSTRAINT ON (a:Person) ASSERT a.oid IS UNIQUE
	#CREATE CONSTRAINT ON ()-[like:LIKED]-() ASSERT exists(like.day)

def loadUsers():
	print "load users"
	for path in userPaths:
		i = 0
		with open(path) as f:
			batch = sgraph.cypher.begin()
			for line in f:	
				#line = line.encode("UTF-8")
				p = line.replace('\"', "").replace("\'", '"').replace("\\", "").replace("\r", "").replace("\n", "").split("\t");
				# id        name      surname    link       gender      homeCity
				query = "create (n:Person {oid: {a}, name: {b}, surname: {c}, gender: {d}, homecity: {e}})"
				batch.append(query, {"a": p[0], "b": p[1], "c": p[2], "d": p[4], "e": p[5]})
				batch.process()
				if (i == batchSize):
					i = 0
					batch.commit()
					batch = sgraph.cypher.begin()
				else:
					i += 1

			# if any query left
			batch.commit()

def setFriendships():
	print "set Friendship"
	for path in friendshipPaths:
		i = 0
		with open(path) as f:
			batch = sgraph.cypher.begin()
			query = "match (x:Person), (y:Person) where x.oid={a} and y.oid={b} and not (y)-[:Friendship]->(x) create unique (x)-[r:Friendship]->(y)"
				
			for line in f:			
				# id   id
				p = line.replace("\r", "").replace("\n", "").split("\t");
				batch.append(query, {"a": p[0], "b": p[1]})
				batch.process()
				if (i == batchSize):
					i = 0
					print "commit to db"
					batch.commit()
					batch = sgraph.cypher.begin() 
				else:
					i += 1

			# if any query left
			batch.commit()

def loadSubCategories(item, parentName):
	for cat in item['categories']:
		temp = {}
		temp['name'] = cat['name']
		temp['oid'] = cat['id']
		temp['parent'] = parentName
		
		categoriesAll.append(temp)

		if ('categories' in cat):
			loadSubCategories(cat, cat['name'])

def loadCategories():
	print "load categories"
	with open(catPath, encoding='latin-1') as f:    
   		data = json.load(f)

	for item in data['response']['categories']:
		temp = {}
		temp['name'] = item['name'] 
		if ('id' in item):
			temp['oid'] = item['id']
		else:
			temp['oid'] = ""
		temp['parent'] = ""

		categoriesAll.append(temp)
		if ('categories' in item):
			loadSubCategories(item, item['name'])

	#print categoriesAll
	i = 0
	newId = 0
	batch = sgraph.cypher.begin()
	for cat in categoriesAll:
		print cat
		query = "create (n:Category {oid: {a}, name: {b}, id: {c}})"
		batch.append(query, {"a": cat['oid'], "b": cat['name'], "c": newId})
		newId += 1
		batch.process()
		if cat["parent"] != "":
			query = "match (x:Category), (y:Category) where x.name={a} and y.name={b} create unique (x)-[r:Child_Category]->(y)"
			batch.append(query, {"a": cat['name'], "b": cat['parent']})			
			batch.process()

		if (i == batchSize):
			i = 0
			batch.commit()
			batch = sgraph.cypher.begin()				
		else:
			i += 1
	batch.commit()

def loadVenues():
	print "load venues"
	for path in venuePaths:
		i = 0
		with open(path) as f:
			batch = sgraph.cypher.begin()

			for line in f:
				#venueID	name	lat		lng		address		city 	state 		
				p = line.replace('\"', "").replace("\'", '"').replace("\\", "").replace("\r", "").replace("\n", "").split("\t");
				query = "create (n:Venue {oid: {a}, name: {b}, lat: {c}, lng: {d}, address: {e}, city: {f}, state: {g}})"
				batch.append(query, {"a": p[0], "b": p[1], "c": p[2], "d": p[3], "e": p[4], "f": p[5], "g": p[6]})
				batch.process()
				if (i == batchSize):
					i = 0
					print "commited to db"
					batch.commit()
					batch = sgraph.cypher.begin()
				else:
					i += 1

			# if any query left
			batch.commit()

def setTips():
	print "set tips"
	for path in tipPaths:
		i = 0
		query = "match (x:Person), (y:Venue) where x.oid={a} and y.oid={b} create (x)-[r:Mark{mark: {c}}]->(y)"
		with open(path) as f:
			batch = sgraph.cypher.begin()
				
			for line in f:	
				#print line
				#time.sleep(0.7)
				p = line.replace("\r", "").replace("\n", "").replace("\"", "").split("\t");
				
				l = len(p)
				val = float(p[l-2])
				mark = 1
				if p[l-3] == "negative":
					if val < -0.75:
						mark = 1
					elif val < -0.5:
						mark = 2
					elif val < -0.25:
						mark = 3
					else:
						mark = 4
				elif p[l-3] == "neutral":
					if val < 0:
						mark = 5
					else:
						mark = 6
				elif p[l-3] == "positive":
					if val < 0.25:
						mark = 7
					elif val < 0.5:
						mark = 8
					elif val < 0.75:
						mark = 9
					else:
						mark = 10
				#print p[0], p[1], mark
				try:
					batch.append(query, {"a": p[0], "b": p[1], "c": mark})
					#print p[0], p[1], p[2], p[3]
					batch.process()
				except:
					continue
				if (i == batchSize):
					i = 0
					batch.commit()
					batch = sgraph.cypher.begin() 
				else:
					i += 1

			# if any query left
			batch.commit()

def setVenueCategory():
	print "set venues categories"
	for path in venuePaths:
		i = 0
		query = "match (x:Venue), (y:Category) where x.oid={a} and y.oid={b} create unique (x)-[r:Is_Category]->(y)"
		with open(path) as f:
			batch = sgraph.cypher.begin()

			for line in f:
				#venueID	name	lat		lng		address		city 	state 		
				p = line.replace('\"', "").replace("\'", '"').replace("\\", "").replace("\r", "").replace("\n", "").split("\t");
				
				if len(p) <= 13:
					continue
				catIds = p[13].split(" ")
				for catId in catIds:	
					batch.append(query, {"a": p[0], "b": catId})
					batch.process()
					if (i == batchSize):
						i = 0
						batch.commit()
						print "commited to db"
						batch = sgraph.cypher.begin()
					else:
						i += 1

			# if any query left
			batch.commit()

def setTipsTime():
	print "set tips time"
	for path in tipPaths:
		i = 0
		query = "match (x:Person)-[m:Mark]->(y:Venue) where x.oid={a} and y.oid={b} set m.time = {c}"
		with open(path) as f:
			batch = sgraph.cypher.begin()
				
			for line in f:	
				#print line
				#time.sleep(0.7)
				p = line.replace("\r", "").replace("\n", "").replace("\"", "").split("\t");
				
				try:
					batch.append(query, {"a": p[0], "b": p[1], "c": p[4]})
					#print p[0], p[1], p[2], p[3]
					batch.process()
				except:
					continue
				if (i == batchSize):
					i = 0
					print "commited"
					batch.commit()
					batch = sgraph.cypher.begin() 
				else:
					i += 1

			# if any query left
			batch.commit()

def main():
	loadUsers()
	loadCategories()
	loadVenues()

	# set constraints before relationships
	setFriendships()
	setVenueCategory()

	setTips()
	setTipsTime()


main()