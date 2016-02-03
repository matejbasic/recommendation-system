import time
import mysql.connector

cnx = mysql.connector.connect(user='', password='',
                              host='127.0.0.1',
                              database='recomm')

class Recommendations():

	def __init__(self, graph, radius, recommendationNum, date):
		self.graph = graph
		self.radius = radius
		self.recommendationNum = recommendationNum
		self.date = date

	def __init__(self, graph, radius, date):
		self.graph = graph
		self.radius = radius
		self.date = date

	def setGraph(self, graph):
		self.graph = graph

	def setRadius(self, radius):
		self.radius = radius

	def setRecommendationNum(self, rn):
		self.recommendationNum = rn

	def setDate(self, date):
		self.date = date

	##
	#	Content-based recommendations.
	#	Returns most favorited venues (by whole community) in given categories, location and radius
	##
	def getCBRecommendations(self, lat, lng, categories):
		recommendations = []
		ircData = []
		userRecommendationNum = self.recommendationNum
		categorySumWeight = 0
		irrelevantVenuesNum = 0
		
		for category in categories:
			categorySumWeight += float(category.weight)

		catPonder = self.recommendationNum
		if len(categories) > 0 and len(categories) < self.recommendationNum:
			catPonder = int(self.recommendationNum * (self.recommendationNum / len(categories)))
					
		for category in categories:
						
			categoryRecommendationNum = int(catPonder * (float(category.weight) / categorySumWeight))
			if categoryRecommendationNum == 0:
				categoryRecommendationNum = 1
						
			if userRecommendationNum - categoryRecommendationNum > 0:
				userRecommendationNum -= categoryRecommendationNum
			elif userRecommendationNum == 0:
				continue
			else:
				categoryRecommendationNum = userRecommendationNum
				userRecommendationNum = 0
						
			#print category.name, category.id, category.weight, categoryRecommendationNum
			
			query = "MATCH (v:Venue)-[:Is_Category]->(c:Category) WHERE acos(sin(" + lat + ") * sin(toFloat(v.lat)) + cos(" + lat + ") * "
			query += "cos(toFloat(v.lat)) * cos(toFloat(v.lng) - (" + lng + "))) * 6371 <= " + str(self.radius) + " "
			query += "AND c.id=" + str(category.id) + " MATCH (v)-[m:Mark]-(:Person) "
			query += "WHERE toInt(m.time) <= " + str(self.date) + " "
			query += "RETURN v.name AS name, v.oid AS oid, " + getWeightFormula("m") + " AS weight, "
			query += "sum(m.mark)/count(m) as mark ORDER BY weight DESC LIMIT " + str(categoryRecommendationNum)

			categoryRecommendations = self.graph.cypher.execute(query);
			for cr in categoryRecommendations:
				recommendations.append(cr)
				temp = {}
				temp["categoryId"] = category.id
				temp["offset"] =  categoryRecommendationNum
				temp["limit"] = self.recommendationNum - categoryRecommendationNum
				ircData.append(temp)

		return {"recommendations": recommendations, "misc": ircData, "notRecommendedIrrelevant": 0}

	##
	#	Collaboration filtering recommendations.
	#	Returns favorited venues by experts in given categories and location
	##
	def getCFRecommendations(self, lat, lng, categories):
		notRecommendedIrrelevant = 0
		currentRecommendationNum = self.recommendationNum
		first = True
		offset = 0
		cats = ""
		recommendations = []
		ircData = []
		
		for category in categories:
			if first == False:
				cats += ", "
			else:
				first = False
			cats += str(category.id)

		while currentRecommendationNum > 0 and cats != "":
			# find users with max number of checkins in given location
			query = "MATCH (v:Venue)-[m:Mark]-(p:Person) WHERE acos(sin(" + lat + ") * sin(toFloat(v.lat)) + "
			query += "cos(" + lat + ") * cos(toFloat(v.lat)) * cos(toFloat(v.lng) - (" + lng + "))) * 6371 "
			query += "<= 1000  AND toInt(m.time) <= " + str(self.date) + " "
			query += "RETURN p.name as name, p.oid as oid, COUNT(m.mark) AS marksNum, "
			query += "toFloat(SUM(m.mark))/COUNT(m) AS marksAvg ORDER BY marksNum DESC, marksAvg DESC "
			query += "SKIP " + str(offset) + " LIMIT " + str(self.recommendationNum)
			#print query

			users = self.graph.cypher.execute(query);
			topUsers = []
			
			for user in users:
				if currentRecommendationNum == 0:
					continue
				# get similarity by categories for each user
				query = "MATCH (p:Person)-[m:Mark]-(v:Venue)-[ic:Is_Category]->(c:Category) "
				query += "WHERE p.oid='" + user.oid + "' AND c.id IN [" + cats + "] AND "
				query += "toInt(m.time) <= " + str(self.date) + " "
				query += "RETURN c.name, c.oid, count(m) AS markNum"
				matchedCats = self.graph.cypher.execute(query)
				weight = 0
				for c in matchedCats:
					weight += c.markNum

				# get similarity by venues for each user
				# TODO

				topUsers.append((user.oid, weight))
			
			topUsers.sort(key=lambda tup: tup[1], reverse=True)
			
			for tu in topUsers:
				# if user has weight = 0 or if we already satisfied number of recommendations needed
				if tu[1] == 0 or currentRecommendationNum == 0:
					break
				# get users venues in given location and radius
				query = "MATCH (p:Person)-[m:Mark]-(v:Venue)-[ic:Is_Category]->(c:Category) "
				query += "WHERE acos(sin(" + lat + ") * sin(toFloat(v.lat)) + cos(" + lat + ") * "
				query += "cos(toFloat(v.lat)) * cos(toFloat(v.lng) - (" + lng + "))) * 6371 <= 1000 AND "
				query += "toInt(m.time) > " + str(self.date) + " AND "
				query += "p.oid='" + str(tu[0]) + "' AND c.id IN [" + cats + "] "
				query += "RETURN v.name AS name, v.oid AS oid, m.mark AS mark, c.id AS categoryId "
				query += "ORDER BY mark DESC LIMIT " + str(self.recommendationNum)
				
				data = self.graph.cypher.execute(query);
				for r in data:
					if currentRecommendationNum == 0:
						break
					currentRecommendationNum -= 1
					recommendations.append(r)
					
					temp = {}
					temp["categoryId"] = r.categoryId
					temp["offset"] = currentRecommendationNum
					temp["limit"] = 1
					ircData.append(temp)
		
				notRecommendedIrrelevant += currentRecommendationNum if len(data) - currentRecommendationNum > 0 else 0
				
			offset += self.recommendationNum
			if offset > self.recommendationNum * 3:
				break

		return {"recommendations": recommendations, "misc": ircData, "notRecommendedIrrelevant": notRecommendedIrrelevant}

	##
	#	Collaboration filtering with SN recommendations.
	#	Based on CF but with giving priority to friends
	##
	def getCFSNRecommendations(self, lat, lng, userOid, categories):
		notRecommendedIrrelevant = 0
		currentRecommendationNum = self.recommendationNum
		first = True
		offset = 0
		cats = ""
		recommendations = []
		ircData = []
		
		for category in categories:
			if first == False:
				cats += ", "
			else:
				first = False
			cats += str(category.id)

		while currentRecommendationNum > 0 and cats != "":
			# find users with max number of checkins in given location
			query = "MATCH (v:Venue)-[m:Mark]-(p:Person) WHERE acos(sin(" + lat + ") * sin(toFloat(v.lat)) + "
			query += "cos(" + lat + ") * cos(toFloat(v.lat)) * cos(toFloat(v.lng) - (" + lng + "))) * 6371 "
			query += "<= 1000  AND toInt(m.time) <= " + str(self.date) + " "
			query += "RETURN p.name as name, p.oid as oid, COUNT(m.mark) AS marksNum, "
			query += "toFloat(SUM(m.mark))/COUNT(m) AS marksAvg ORDER BY marksNum DESC, marksAvg DESC "
			query += "SKIP " + str(offset) + " LIMIT " + str(self.recommendationNum)

			users = self.graph.cypher.execute(query);
			topUsers = []
			
			for user in users:
				# get category similarity for each user
				query = "MATCH (p:Person)-[m:Mark]-(v:Venue)-[ic:Is_Category]->(c:Category) "
				query += "WHERE p.oid='" + user.oid + "' AND c.id IN [" + cats + "] AND "
				query += "toInt(m.time) <= " + str(self.date) + " "
				query += "RETURN c.name, c.oid, count(m) AS markNum"
				matchedCats = self.graph.cypher.execute(query)
				
				# dohvati cijeli subgraf korisnik
				# sto treba: u - m - v - cat
				# dohvati lokacije

				query = "MATCH (p:Person)-[f:Friendship]-(p2:Person) WHERE "
				query += "p.oid='" + userOid + "' AND p2.oid='" + user.oid + "' RETURN COUNT(f) AS f"				
				af = self.graph.cypher.execute(query)
				
				weight = 0
				for c in matchedCats:
					weight += c.markNum
				if weight > 0:
					topUsers.append((user.oid, weight, af[0].f))
			
			topUsers.sort(key=lambda tup: tup[1], reverse=True)
			# sort that friends get first
			topUsers.sort(key=lambda tup: tup[2], reverse=True)

			for tu in topUsers:
				if tu[1] == 0 or currentRecommendationNum == 0:
					break
				# get users venues in given location and radius
				query = "MATCH (p:Person)-[m:Mark]-(v:Venue)-[ic:Is_Category]->(c:Category) "
				query += "WHERE acos(sin(" + lat + ") * sin(toFloat(v.lat)) + cos(" + lat + ") * "
				query += "cos(toFloat(v.lat)) * cos(toFloat(v.lng) - (" + lng + "))) * 6371 <= 1000 AND "
				query += "toInt(m.time) > " + str(self.date) + " AND "
				query += "p.oid='" + str(tu[0]) + "' AND c.id IN [" + cats + "] "
				query += "RETURN v.name AS name, v.oid AS oid, m.mark AS mark, c.id AS categoryId "
				query += "ORDER BY mark DESC LIMIT " + str(self.recommendationNum)
				
				data = self.graph.cypher.execute(query);
				for r in data:
					if currentRecommendationNum == 0:
						break
					currentRecommendationNum -= 1
					recommendations.append(r)
					
					temp = {}
					temp["categoryId"] = r.categoryId
					temp["offset"] = currentRecommendationNum
					temp["limit"] = 1
					ircData.append(temp)

				notRecommendedIrrelevant += currentRecommendationNum if len(data) - currentRecommendationNum > 0 else 0

			offset += self.recommendationNum
			if offset > self.recommendationNum * 3:
				break

		return {"recommendations": recommendations, "misc": ircData, "notRecommendedIrrelevant": notRecommendedIrrelevant}

	##
	#	Collaboration filtering with SN recommendations.
	#	Based on CF but with giving priority to users with higher mutual friends
	##
	def getCFSN2Recommendations(self, lat, lng, userOid, categories):
		notRecommendedIrrelevant = 0
		currentRecommendationNum = self.recommendationNum
		first = True
		offset = 0
		cats = ""
		recommendations = []
		ircData = []
		
		for category in categories:
			if first == False:
				cats += ", "
			else:
				first = False
			cats += str(category.id)

		while currentRecommendationNum > 0 and cats != "":
			# find users with max number of checkins in given location
			query = "MATCH (v:Venue)-[m:Mark]-(p:Person) WHERE acos(sin(" + lat + ") * sin(toFloat(v.lat)) + "
			query += "cos(" + lat + ") * cos(toFloat(v.lat)) * cos(toFloat(v.lng) - (" + lng + "))) * 6371 "
			query += "<= 1000  AND toInt(m.time) <= " + str(self.date) + " "
			query += "RETURN p.name as name, p.oid as oid, COUNT(m.mark) AS marksNum, "
			query += "toFloat(SUM(m.mark))/COUNT(m) AS marksAvg ORDER BY marksNum DESC, marksAvg DESC "
			query += "SKIP " + str(offset) + " LIMIT " + str(self.recommendationNum)

			users = self.graph.cypher.execute(query);
			topUsers = []
			
			for user in users:
				# get similarity for each user
				query = "MATCH (p:Person)-[m:Mark]-(v:Venue)-[ic:Is_Category]->(c:Category) "
				query += "WHERE p.oid='" + userOid + "' AND c.id IN [" + cats + "] AND "
				query += "toInt(m.time) <= " + str(self.date) + " "
				query += "RETURN c.name, c.oid, count(m) AS markNum"

				matchedCats = self.graph.cypher.execute(query)

				query = "MATCH (p:Person)-[f:Friendship]-(other:Person)-[f2:Friendship]-(p2:Person) "
				query += "WHERE p.oid='" + userOid + "' AND p2.oid='" + user.oid + "' "
				query += "RETURN count(f) AS count"
				mutual = self.graph.cypher.execute(query)			

				weight = 0
				for c in matchedCats:
					weight += c.markNum
				if weight > 0:
					topUsers.append((user.oid, weight, mutual[0].count))

			# sort by number of mutual friends
			topUsers.sort(key=lambda tup: tup[2], reverse=True)
			# sort by weight
			topUsers.sort(key=lambda tup: tup[1], reverse=True)

			for tu in topUsers:
				if tu[1] == 0 or currentRecommendationNum == 0:
					break
				# get users venues in given location and radius
				query = "MATCH (p:Person)-[m:Mark]-(v:Venue)-[ic:Is_Category]->(c:Category) "
				query += "WHERE acos(sin(" + lat + ") * sin(toFloat(v.lat)) + cos(" + lat + ") * "
				query += "cos(toFloat(v.lat)) * cos(toFloat(v.lng) - (" + lng + "))) * 6371 <= 1000 AND "
				query += "toInt(m.time) > " + str(self.date) + " AND "
				query += "p.oid='" + str(tu[0]) + "' AND c.id IN [" + cats + "] "
				query += "RETURN v.name AS name, v.oid AS oid, m.mark AS mark, c.id AS categoryId "
				query += "ORDER BY mark DESC LIMIT " + str(self.recommendationNum)
				
				data = self.graph.cypher.execute(query);
				for r in data:
					if currentRecommendationNum == 0:
						break
					currentRecommendationNum -= 1
					recommendations.append(r)
					
					temp = {}
					temp["categoryId"] = r.categoryId
					temp["offset"] = currentRecommendationNum
					temp["limit"] = 1
					ircData.append(temp)

				notRecommendedIrrelevant += currentRecommendationNum if len(data) - currentRecommendationNum > 0 else 0

			offset += 100
			if offset > self.recommendationNum * 3:
				break

		return {"recommendations": recommendations, "misc": ircData, "notRecommendedIrrelevant": notRecommendedIrrelevant}

	##
	#	Collaboration filtering recommendations.
	#	Based on CF but with giving priority to same sex users
	##
	def getCFSN3Recommendations(self, lat, lng, userOid, userGender, categories):
		notRecommendedIrrelevant = 0
		currentRecommendationNum = self.recommendationNum
		first = True
		offset = 0
		cats = ""
		recommendations = []
		ircData = []
		
		for category in categories:
			if first == False:
				cats += ", "
			else:
				first = False
			cats += str(category.id)

		while currentRecommendationNum > 0 and cats != "":
			# find users with max number of checkins in given location
			query = "MATCH (v:Venue)-[m:Mark]-(p:Person) WHERE acos(sin(" + lat + ") * sin(toFloat(v.lat)) + "
			query += "cos(" + lat + ") * cos(toFloat(v.lat)) * cos(toFloat(v.lng) - (" + lng + "))) * 6371 "
			query += "<= 1000  AND toInt(m.time) <= " + str(self.date) + " "
			query += "RETURN p.name as name, p.oid as oid, p.gender AS gender, p.homecity AS city, COUNT(m.mark) AS marksNum, "
			query += "toFloat(SUM(m.mark))/COUNT(m) AS marksAvg ORDER BY marksNum DESC, marksAvg DESC "
			query += "SKIP " + str(offset) + " LIMIT " + str(self.recommendationNum)

			users = self.graph.cypher.execute(query);
			topUsers = []
			
			for user in users:
				# get similarity for each user
				query = "MATCH (p:Person)-[m:Mark]-(v:Venue)-[ic:Is_Category]->(c:Category) "
				query += "WHERE p.oid='" + userOid + "' AND c.id IN [" + cats + "] AND "
				query += "toInt(m.time) <= " + str(self.date) + " "
				query += "RETURN c.name, c.oid, count(m) AS markNum"

				matchedCats = self.graph.cypher.execute(query)			

				gender = 0 if userGender == user.gender else 1

				weight = 0
				for c in matchedCats:
					weight += c.markNum
				if weight > 0:
					topUsers.append((user.oid, weight, gender))
					
			
			# sort by weight
			topUsers.sort(key=lambda tup: tup[1], reverse=True)
			# sort by gender match
			topUsers.sort(key=lambda tup: tup[2], reverse=True)

			for tu in topUsers:
				if tu[1] == 0 or currentRecommendationNum == 0:
					break
				# get users venues in given location and radius
				query = "MATCH (p:Person)-[m:Mark]-(v:Venue)-[ic:Is_Category]->(c:Category) "
				query += "WHERE acos(sin(" + lat + ") * sin(toFloat(v.lat)) + cos(" + lat + ") * "
				query += "cos(toFloat(v.lat)) * cos(toFloat(v.lng) - (" + lng + "))) * 6371 <= 1000 AND "
				query += "toInt(m.time) > " + str(self.date) + " AND "
				query += "p.oid='" + str(tu[0]) + "' AND c.id IN [" + cats + "] "
				query += "RETURN v.name AS name, v.oid AS oid, m.mark AS mark, c.id AS categoryId "
				query += "ORDER BY mark DESC LIMIT " + str(self.recommendationNum)
				
				data = self.graph.cypher.execute(query);
				for r in data:
					if currentRecommendationNum == 0:
						break
					recommendations.append(r)
					currentRecommendationNum -= 1

					temp = {}
					temp["categoryId"] = r.categoryId
					temp["offset"] = currentRecommendationNum
					temp["limit"] = 1
					ircData.append(temp)

				notRecommendedIrrelevant += currentRecommendationNum if len(data) - currentRecommendationNum > 0 else 0
			offset += self.recommendationNum
			if offset > self.recommendationNum * 3:
				break

		return {"recommendations": recommendations, "misc": ircData, "notRecommendedIrrelevant": notRecommendedIrrelevant}

	##
	#	Collaboration filtering recommendations.
	#	Based on CF but with giving priority to users from same city/state
	##
	def getCFSN4Recommendations(self, lat, lng, userOid, userCity, categories):
		notRecommendedIrrelevant = 0
		currentRecommendationNum = self.recommendationNum
		first = True
		offset = 0
		cats = ""
		recommendations = []
		ircData = []
		
		for category in categories:
			if first == False:
				cats += ", "
			else:
				first = False
			cats += str(category.id)

		while currentRecommendationNum > 0 and cats != "":
			# find users with max number of checkins in given location
			query = "MATCH (v:Venue)-[m:Mark]-(p:Person) WHERE acos(sin(" + lat + ") * sin(toFloat(v.lat)) + "
			query += "cos(" + lat + ") * cos(toFloat(v.lat)) * cos(toFloat(v.lng) - (" + lng + "))) * 6371 "
			query += "<= 1000  AND toInt(m.time) <= " + str(self.date) + " "
			query += "RETURN p.name as name, p.oid as oid, p.gender AS gender, p.homecity AS city, COUNT(m.mark) AS marksNum, "
			query += "toFloat(SUM(m.mark))/COUNT(m) AS marksAvg ORDER BY marksNum DESC, marksAvg DESC "
			query += "SKIP " + str(offset) + " LIMIT " + str(self.recommendationNum)

			users = self.graph.cypher.execute(query);
			topUsers = []
			
			for user in users:
				# get similarity for each user
				query = "MATCH (p:Person)-[m:Mark]-(v:Venue)-[ic:Is_Category]->(c:Category) "
				query += "WHERE p.oid='" + userOid + "' AND c.id IN [" + cats + "] AND "
				query += "toInt(m.time) <= " + str(self.date) + " "
				query += "RETURN c.name, c.oid, count(m) AS markNum"

				matchedCats = self.graph.cypher.execute(query)			

				cityMatch = 0
				uhc = userCity.split(" ")
				chc = user.city.split(" ")
				if len(uhc) > 0 and len(chc) > 0:
					uCity = uhc[len(uhc)-1]
					cCity = chc[len(chc)-1]
					if uCity == cCity:
						cityMatch = 1

				weight = 0
				for c in matchedCats:
					weight += c.markNum
				if weight > 0:
					topUsers.append((user.oid, weight, cityMatch))
					
			
			# sort by weight
			topUsers.sort(key=lambda tup: tup[1], reverse=True)
			# sort by city match
			topUsers.sort(key=lambda tup: tup[2], reverse=True)

			for tu in topUsers:
				if tu[1] == 0:
					break
				# get users venues in given location and radius
				query = "MATCH (p:Person)-[m:Mark]-(v:Venue)-[ic:Is_Category]->(c:Category) "
				query += "WHERE acos(sin(" + lat + ") * sin(toFloat(v.lat)) + cos(" + lat + ") * "
				query += "cos(toFloat(v.lat)) * cos(toFloat(v.lng) - (" + lng + "))) * 6371 <= 1000 AND "
				query += "toInt(m.time) > " + str(self.date) + " AND "
				query += "p.oid='" + str(tu[0]) + "' AND c.id IN [" + cats + "] "
				query += "RETURN v.name AS name, v.oid AS oid, m.mark AS mark, c.id AS categoryId "
				query += "ORDER BY mark DESC LIMIT " + str(self.recommendationNum)
				
				data = self.graph.cypher.execute(query);
				for r in data:
					if currentRecommendationNum == 0:
						break
					currentRecommendationNum -= 1
					recommendations.append(r)
					
					temp = {}
					temp["categoryId"] = r.categoryId
					temp["offset"] = currentRecommendationNum
					temp["limit"] = 1
					ircData.append(temp)

				notRecommendedIrrelevant += currentRecommendationNum if len(data) - currentRecommendationNum > 0 else 0

			offset += 100
			if offset > self.recommendationNum * 3:
				break

		return {"recommendations": recommendations, "misc": ircData, "notRecommendedIrrelevant": notRecommendedIrrelevant}

	##
	#	Collaboration filtering with SN recommendations.
	#	Combination of all above 
	##
	def getCFSNAllRecommendations(self, lat, lng, userOid, userGender, userCity, categories):
		notRecommendedIrrelevant = 0
		currentRecommendationNum = self.recommendationNum
		first = True
		offset = 0
		cats = ""
		recommendations = []
		ircData = []
		
		for category in categories:
			if first == False:
				cats += ", "
			else:
				first = False
			cats += str(category.id)

		counter = 0
		while currentRecommendationNum > 0 and cats != "":
			# find users with max number of checkins in given location
			query = "MATCH (v:Venue)-[m:Mark]-(p:Person) WHERE acos(sin(" + lat + ") * sin(toFloat(v.lat)) + "
			query += "cos(" + lat + ") * cos(toFloat(v.lat)) * cos(toFloat(v.lng) - (" + lng + "))) * 6371 "
			query += "<= 1000  AND toInt(m.time) <= " + str(self.date) + " "
			query += "RETURN p.name as name, p.oid as oid, p.gender AS gender, p.homecity AS city, COUNT(m.mark) AS marksNum, "
			query += "toFloat(SUM(m.mark))/COUNT(m) AS marksAvg ORDER BY marksNum DESC, marksAvg DESC "
			query += "SKIP " + str(offset) + " LIMIT " + str(self.recommendationNum)

			users = self.graph.cypher.execute(query);
			topUsers = []
			
			for user in users:
				# get similarity for each user
				query = "MATCH (p:Person)-[m:Mark]-(v:Venue)-[ic:Is_Category]->(c:Category) "
				query += "WHERE p.oid='" + userOid + "' AND c.id IN [" + cats + "] AND "
				query += "toInt(m.time) <= " + str(self.date) + " "
				query += "RETURN c.name, c.oid, count(m) AS markNum"

				matchedCats = self.graph.cypher.execute(query)
				
				query = "MATCH (p:Person)-[f:Friendship]-(p2:Person) WHERE "
				query += "p.oid='" + userOid + "' AND p2.oid='" + user.oid + "' RETURN COUNT(f) AS f"
				af = self.graph.cypher.execute(query)

				query = "MATCH (p:Person)-[f:Friendship]-(other:Person)-[f2:Friendship]-(p2:Person) "
				query += "WHERE p.oid='" + userOid + "' AND p2.oid='" + user.oid + "' "
				query += "RETURN count(f) AS count"
				mutual = self.graph.cypher.execute(query)			

				gender = 1 if userGender == user.gender else 0

				cityMatch = 0
				uhc = userCity.split(" ")
				chc = user.city.split(" ")
				if len(uhc) > 0 and len(chc) > 0:
					uCity = uhc[len(uhc)-1]
					cCity = chc[len(chc)-1]
					if uCity == cCity:
						cityMatch = 1

				weight = 0
				for c in matchedCats:
					weight += c.markNum
				if weight > 0:
					topUsers.append((user.oid, weight, af[0].f, mutual[0].count, gender, cityMatch))
			
			# sort by mutual friends
			topUsers.sort(key=lambda tup: tup[3], reverse=True)
			# sort by gender match
			topUsers.sort(key=lambda tup: tup[4], reverse=True)
			# sort by city match
			topUsers.sort(key=lambda tup: tup[5], reverse=True)
			# sort by weight
			topUsers.sort(key=lambda tup: tup[1], reverse=True)
			# sort that friends gets first
			topUsers.sort(key=lambda tup: tup[2], reverse=True)

			for tu in topUsers:
				if tu[1] == 0:
					break
				# get users venues in given location and radius
				query = "MATCH (p:Person)-[m:Mark]-(v:Venue)-[ic:Is_Category]->(c:Category) "
				query += "WHERE acos(sin(" + lat + ") * sin(toFloat(v.lat)) + cos(" + lat + ") * "
				query += "cos(toFloat(v.lat)) * cos(toFloat(v.lng) - (" + lng + "))) * 6371 <= 1000 AND "
				query += "toInt(m.time) > " + str(self.date) + " AND "
				query += "p.oid='" + str(tu[0]) + "' AND c.id IN [" + cats + "] "
				query += "RETURN v.name AS name, v.oid AS oid, m.mark AS mark, c.id AS categoryId "
				query += "ORDER BY mark DESC LIMIT " + str(self.recommendationNum)
				
				data = self.graph.cypher.execute(query);
				for r in data:
					if currentRecommendationNum == 0:
						break
					currentRecommendationNum -= 1
					recommendations.append(r)
					
					temp = {}
					temp["categoryId"] = r.categoryId
					temp["offset"] = currentRecommendationNum
					temp["limit"] = 1
					ircData.append(temp)

				notRecommendedIrrelevant += currentRecommendationNum if len(data) - currentRecommendationNum > 0 else 0

			offset += 100
			if offset > self.recommendationNum * 3:
				break

		return {"recommendations": recommendations, "misc": ircData, "notRecommendedIrrelevant": notRecommendedIrrelevant}

def getWeightFormula(m):
	#return "(sum(" + m + ".mark)/count(" + m + "))*(count(" + m + ")*0.8454)"
	return "count(" + m + ") / (1 / avg(" + m + ".mark))"

#Returns the Pearson correlation coefficient for p1 and p2 
def simPearson(prefs,p1,p2):
	#Get the list of mutually rated items
	si = {}
	for item in prefs[p1]:
		if item in prefs[p2]: 
			si[item] = 1

	#if they are no rating in common, return 0
	if len(si) == 0:
		return 0

	#sum calculations
	n = len(si)

	#sum of all preferences
	sum1 = sum([prefs[p1][it] for it in si])
	sum2 = sum([prefs[p2][it] for it in si])

	#Sum of the squares
	sum1Sq = sum([pow(prefs[p1][it],2) for it in si])
	sum2Sq = sum([pow(prefs[p2][it],2) for it in si])

	#Sum of the products
	pSum = sum([prefs[p1][it] * prefs[p2][it] for it in si])

	#Calculate r (Pearson score)
	num = pSum - (sum1 * sum2/n)
	den = sqrt((sum1Sq - pow(sum1,2)/n) * (sum2Sq - pow(sum2,2)/n))
	if den == 0:
		return 0

	r = num/den

	return r

def getCFRecommendationsDiscaredApproach(graph, u, categories, radius, recommendationNum, date):
	notRecommendedIrrelevant = 0
	cursor = cnx.cursor()
	first = True
	offset = 0
	cats = ""
	recommendations = []
	ircData = []
	
	for category in categories:
		if first == False:
			cats += ", "
		else:
			first = False
		cats += str(category.id)

	counter = 0
	while len(recommendations) < recommendationNum and cats != "":
		query = "SELECT userId FROM prefferedCategories WHERE categoryId IN (" + cats + ") "
		query += "GROUP BY userId ORDER BY SUM(weight) DESC LIMIT 10 OFFSET " + str(offset)
		#print query
		cursor.execute(query)
		for data in cursor:
			# 0. ensure that user has data 
			# 1. get users recommendations in defined categories
			userId = data[0]
			query = "MATCH (p:Person)-[m:Mark]-(v:Venue)-[ic:Is_Category]->(c:Category) WHERE c.id IN [" + cats + "] "
			query += "AND acos(sin(" + u.lat + ") * sin(toFloat(v.lat)) + cos(" + u.lat + ") * "
			query += "cos(toFloat(v.lat)) * cos(toFloat(v.lng) - (" + u.lng + "))) * 6371 <= " + str(radius) + " "
			query += "AND toInt(m.time) <= " + str(date) + " "
			query += "RETURN v.name AS name, v.oid AS oid, " + getWeightFormula("m") + " AS weight, "
			query += "sum(m.mark)/count(m) AS mark, c.id AS categoryId ORDER BY weight DESC LIMIT " + str(recommendationNum)
			# print query
			
			data = graph.cypher.execute(query);
			for r in data:
				recommendations.append(r)
				recommendationNum -= 1
				temp = {}
				temp["categoryId"] = r.categoryId
				temp["offset"] = recommendationNum
				temp["limit"] = 1
				ircData.append(temp)

			# 2. sort recomms by mark
			# 3. enough recomms ? finish : goto 1

		counter += 1
		offset += 10
		if (counter > 10):
			break

	return {"recommendations": recommendations, "misc": ircData, "notRecommendedIrrelevant": notRecommendedIrrelevant}

