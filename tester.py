import re
import time
from datetime import datetime
from py2neo import authenticate, Graph, Node, Relationship
from recommendations import *

authenticate("localhost:7474", "", "")
graph = Graph("http://localhost:7474/db/data/")

def main():
	print "start testing"
	
	userNum = 10000
	recommendationNums = [5, 10, 15, 20]
	#systems = ["cfsnAll", "cfsn4", "cfsn3", "cfsn2", "cfsn", "cf", "cb"]
	systems = ["cb", "cf", "cfsn", "cfsn2", "cfsn3", "cfsn4", "cfsnAll"]
	radius = 800
	categoriesNum = 10	
	#date = 1288999341
	date = 1300790800

	rc = Recommendations(graph, radius, date)	
	uls = getRandomUsersLocations(userNum, 10)
	print "number of users:", len(uls), "radius:", radius, "date: " , date

	for recommendationNum in recommendationNums:
		rc.setRecommendationNum(recommendationNum)
		print "number of recommendations:", recommendationNum

		for system in systems:
			print "SYSTEM:", system
			
			positiveHits = 0
			negativeHits = 0
			sumUserFutureVenues = 0
			notRecommendedIrrelevant = 0
			markDifference = 0

			sumHitMark = 0
			sumMissMark = 0

			duration = 0
			
			for u in uls:
				#print "-----------"
				categories = getUsersFavCategories(u.oid, date, categoriesNum)
				
				startTime = datetime.now()
				if system == "cb":
					data = rc.getCBRecommendations(u.lat, u.lng, categories)
				elif system == "cf":
					data = rc.getCFRecommendations(u.lat, u.lng, categories)
				elif system == "cfsn":
					data = rc.getCFSNRecommendations(u.lat, u.lng, u.oid, categories)
				elif system == "cfsn2":
					data = rc.getCFSN2Recommendations(u.lat, u.lng, u.oid, categories)
				elif system == "cfsn3":
					data = rc.getCFSN3Recommendations(u.lat, u.lng, u.oid, u.gender, categories)
				elif system == "cfsn4":
					data = rc.getCFSN4Recommendations(u.lat, u.lng, u.oid, u.city, categories)
				elif system == "cfsnAll":
					data = rc.getCFSNAllRecommendations(u.lat, u.lng, u.oid, u.gender, u.city, categories)
				
				duration = datetime.now() - startTime

				notRecommendedIrrelevant += data["notRecommendedIrrelevant"]
				for i in range(0, len(data["recommendations"])):
					recomm = data["recommendations"][i]
					venuesFuture = getUserFutureVenues(u.oid, u.lat, u.lng, radius, data["misc"][i]["categoryId"], date)
					#print len(venuesFuture), data["misc"][i]["categoryId"]
					if len(venuesFuture) == 0:
						continue
					sumUserFutureVenues += len(venuesFuture)
			
					userMarked = isAlreadyMarked(venuesFuture, recomm.oid)
					if (userMarked):
						positiveHits += 1
						markDifference += recomm.mark - userMarked
						sumHitMark += recomm.mark 
					else:
						negativeHits += 1
						sumMissMark += recomm.mark
						#print recomm.name, ": ", recomm.mark, " org mark: ", str(userMarked)	
				#print ""						

			printStats(sumHitMark, sumMissMark, positiveHits, negativeHits, 
				sumUserFutureVenues, notRecommendedIrrelevant, markDifference, userNum, duration)

def getRandomUsersLocations(userNum, minMarkNum):
	query = "MATCH (p:Person)-[:Mark]->() WITH p,rand() AS r ORDER BY r "
	query += "MATCH (p)-[m:Mark]->(v:Venue) WITH p,m,v,COUNT(m) AS markNum "
	query += "WHERE markNum > " + str(minMarkNum) + " "
	query += "RETURN p.name AS username, p.gender AS gender, p.homecity AS city, p.oid AS oid, v.lat AS lat, v.lng AS lng, markNum "
	query += "LIMIT " + str(userNum)
	
	return graph.cypher.execute(query)

def getUsersFavCategories(userOid, date, categoriesNum):
	query = "MATCH (p:Person)-[m:Mark]-(v:Venue)-[:Is_Category]->(c:Category) WHERE p.oid='" + userOid + "' "
	query += "AND toInt(m.time) <= " + str(date) + " "
	query += "RETURN c.name AS name, c.id AS id, " + getWeightFormula("m") + " AS weight "
	query += "ORDER BY weight DESC LIMIT " + str(categoriesNum)

	return graph.cypher.execute(query)

def getUserFutureVenues(userOid, lat, lng, radius, categoryId, date):
	query = "MATCH (p:Person)-[m:Mark]-(v:Venue)-[:Is_Category]->(c:Category) "
	query += "WHERE acos(sin(" + lat + ") * sin(toFloat(v.lat)) + cos(" + lat + ") * cos(toFloat(v.lat)) * "
	query += "cos(toFloat(v.lng) - (" + lng + "))) * 6371 <= " + str(radius) + " AND c.id=" + str(categoryId) + " "
	query += "AND p.oid='" + userOid + "' AND toInt(m.time) > " + str(date) + " "
	query += "RETURN v.name AS name, v.oid AS oid, m.mark AS mark, "
	query += getWeightFormula("m") + " AS weight ORDER BY weight DESC"

	return graph.cypher.execute(query)

def getUserPastVenues(userOid, lat, lng, radius, categoryId, date):
	query = "MATCH (p:Person)-[m:Mark]-(v:Venue)-[:Is_Category]->(c:Category) "
	query += "WHERE acos(sin(" + lat + ") * sin(toFloat(v.lat)) + cos(" + lat + ") * cos(toFloat(v.lat)) * "
	query += "cos(toFloat(v.lng) - (" + lng + "))) * 6371 <= " + str(radius) + " AND c.id=" + str(categoryId) + " "
	query += "AND p.oid='" + userOid + "' AND toInt(m.time) <= " + str(date) + " "
	query += "RETURN v.name AS name, v.oid AS oid, m.mark AS mark, "
	query += getWeightFormula("m") + " AS weight ORDER BY weight DESC"

	return graph.cypher.execute(query)

def getUsersMarks(userOid, date):
	query = "MATCH (p:Person)-[m:Mark]->(v:Venue) WHERE p.oid='" + userOid + "' "
	query += "WHERE m.time <=" + str(date) + " "
	query += "RETURN toInt(avg(m.mark)) AS mark, v.oid AS venueOid ORDER BY v.oid"
	
	return graph.cypher.execute(query)

def isAlreadyMarked(venues, recommendationId):
	for venue in venues:
		if venue.oid == recommendationId:	
			return venue.mark
	return False

def printStats(avgHitMark, avgMissMark, positiveHits, negativeHits, sumUserFutureVenues, notRecommendedIrrelevant, markDifference, userNum, duration):
	##
	#	STATS
	##
	avgHitMark = float(avgHitMark) / (positiveHits or 1)
	avgMissMark = float(avgMissMark) / (negativeHits or 1)

	# negativeHits - irrelevant recommended venues
	precision = float(positiveHits) / (positiveHits + negativeHits)
	recall = float(positiveHits) / sumUserFutureVenues
	if negativeHits != 0:
		fallout = float(negativeHits) / (negativeHits + notRecommendedIrrelevant)
		inverseRecall = notRecommendedIrrelevant / (negativeHits + notRecommendedIrrelevant)
	else:
		fallout = 0
		inverseRecall = 1
	missRate = float(sumUserFutureVenues - positiveHits) / sumUserFutureVenues

	inversePrecision = notRecommendedIrrelevant / ((sumUserFutureVenues - positiveHits) + notRecommendedIrrelevant)
	markedness = precision + inversePrecision - 1
	informedness = recall + inverseRecall - 1

	print "positive hits:", positiveHits, "negative hits:", negativeHits
	print "precision:", precision, "recall:", recall, "fallout:", fallout, "missRate:", missRate
	print "markedness:", markedness, "informedness:", informedness

	print "average relevant mark:", avgHitMark, "average irrelevant mark:", avgMissMark, "mark difference:", float(markDifference) / userNum
	print "duration:", duration
	print ""

main()