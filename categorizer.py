import time
import mysql.connector
from py2neo import authenticate, Graph
from recommendations import getWeightFormula

authenticate("localhost:7474", "", "")
graph = Graph("http://localhost:7474/db/data/")
cnx = mysql.connector.connect(user='', password='',
                              host='127.0.0.1',
                              database='')
cursor = cnx.cursor()

def getInsertQuery(userId, categoryId, weight):
	query = "INSERT INTO prefferedCategories(userId, categoryId, weight) "
	query += "VALUES(" + str(userId) + "," + str(categoryId) + "," + str(weight) + ")"
	return query

def getAllUsers():
	query = "MATCH (p:Person) RETURN p.oid AS oid"
	return graph.cypher.execute(query)

def getUsersFavCategories(userOid, categoriesNum):
	query = "MATCH (p:Person)-[m:Mark]-(v:Venue)-[:Is_Category]->(c:Category) WHERE p.oid='" + userOid + "' "
	query += "RETURN c.id AS id, " + getWeightFormula("m") + " AS weight "
	query += "ORDER BY weight DESC LIMIT " + str(categoriesNum)
	return graph.cypher.execute(query)

def main():
	users = getAllUsers()
	usersNum = len(users)
	counter = 0
	for user in users:
		categories = getUsersFavCategories(user.oid, 10)
		for category in categories:
			insertQuery = getInsertQuery(user.oid, category.id, category.weight)
			cursor.execute(insertQuery)

		cnx.commit()
		counter += 1
		if counter % 10000 == 0:
			print "processed", counter, "of", usersNum

	cursor.close()
	cnx.close()
	print "done"

main()