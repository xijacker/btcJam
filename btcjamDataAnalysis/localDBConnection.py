from pyspark.sql import SQLContext
import psycopg2 

class LocalSql:
	def __init__(self, sc):
#		self._sc = SparkContext("http://btcjamdb.cexf0eq5s7ky.us-east-1.rds.amazonaws.com", "btcJam Data Analysis")
		self._sqlContext = SQLContext(sc)
		self._connectPostgreSql()

	def _connectPostgreSql(self):	
		try:
			self._conn = psycopg2.connect(database="btcjam", user="david", password="lmxtGongShi")
#			self._conn = psycopg2.connect(database="btcjam_production", user="david", password="lmxtGongShi")
			self._cur = self._conn.cursor()
		except:
			self._conn = None
			print "+++++++++Unable to connect the PostgreSql database"

	def getSqlContext(self):
		return self._sqlContext

	def getCursor(self):
		return self._cur

	def getConn(self):
		return self._conn
