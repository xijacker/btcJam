from pyspark import SparkContext
import psycopg2 
import time
import matplotlib.pyplot as plt
from localDBConnection import LocalSql


"""
Analysis the late payments based on the is_late in credit_reports
The results looks like not accuracy and it might be changed to used the raw data from Users, Loans and multipayments
"""

class DataAnalysis:
	def __init__(self):
		self._sc = SparkContext("local", "btcJam Data Analysis")
		localDbConn = LocalSql(self._sc)
		self._cur = localDbConn.getCursor()
		self._sqlContext = localDbConn.getSqlContext()
		self._conn	= localDbConn.getConn()

	def badBehaviorsAnalysis(self):
		if self._conn != None:
			self._cur.execute("SELECT user_id, pays_in_time, is_late, repaid_loans, active_loans FROM credit_reports")
			self._credit_reports = self._sc.parallelize(self._cur.fetchmany(-1))
			self._credit_reports = self._credit_reports.map(lambda s : (s[0], (s[1], s[2], s[3], s[4])))

			self._cur.execute("SELECT id, bad_behavior FROM users")
			self._user_behavior = self._sc.parallelize(self._cur.fetchmany(-1)).map(lambda s : (s[0], s[1]))

			self._credit_reports = self._user_behavior.fullOuterJoin(self._credit_reports)
			print self._credit_reports.take(20)

			# get the positive users who has payment and no late payment		
#			self._positive_credit = self._credit_reports.filter(lambda s: s[1][1] is not None and isinstance(s[1][1][2], int) and s[1][1][2] > 0)
#			self._positive_credit = self._credit_reports.filter(lambda s: s[1][0] is not None and isinstance(s[1][0], int) and s[1][0] is False)
			self._positive_credit = self._credit_reports.filter(lambda s: s[1][1] is not None and s[1][1][2] > 0)
			#self._positive_credit = self._credit_reports.filter(lambda s: s[1][0] is not None and s[1][0] is False)
			# get the negatvie users who have late payment
			self._negative_credit = self._credit_reports.filter(lambda s: s[1][0] is not None and s[1][0] is True)

			print self._negative_credit.take(50)
		#	print self._credit_reports.take(10)
			# map to 	
			postiveSamples = self._positive_credit.map(lambda s : (s[1][0], 1)).reduceByKey(lambda x, y : x+y)
			negativeSamples = self._negative_credit.map(lambda s : (s[1][0], 1)).reduceByKey(lambda x, y : x+y)
			#paybackByCountry = postiveByCountry.fullOuterJoin(negativeByCountry).map(lambda s : (s[0], (s[1][0] if isinstance(s[1][0], int) else 0, s[1][1] if isinstance(s[1][1], int) else 0))).map(lambda s : (s[1][1]*100.0/(s[1][1]+s[1][0]), (s[0], s[1][0], s[1][1]))).sortByKey()
			paybackByCountry = postiveSamples.fullOuterJoin(negativeSamples).map(lambda s : (s[0], (s[1][0] if isinstance(s[1][0], int) else 0, s[1][1] if isinstance(s[1][1], int) else 0))).map(lambda s : (s[1][1]*100.0/(s[1][1]+s[1][0]), (s[0], s[1][0], s[1][1])))

			plt.figure(1)
			paybackCollect = paybackByCountry.collect()
			
			latepayCount = 0
			paymentnolate = 0 

			for a in paybackCollect:
				latepaid = a[1][2]
				latepayCount += latepaid
				paidback = a[1][1] if isinstance(a[1][1], int) else 0
				paymentnolate += paidback
				plt.subplot(3,1,1)
				plt.bar(paybackCollect.index(a), a[0]+0.00000001 if isinstance(a[0], float) else 0.0)
				plt.subplot(3,1,2)
				plt.bar(paybackCollect.index(a), paidback)
				plt.subplot(3,1,3)
				plt.bar(paybackCollect.index(a), latepaid+0.0000001)
				print(a)

			plt.show()

			print "Total Late Payment: " + str(latepayCount) + "; Tatal Payment not late: " + str(paymentnolate) + ";  Total Late pay Ratio: " + str(latepayCount*100.0/(latepayCount + paymentnolate)) + ";   Negative customver: " + str(self._negative_credit.count())
		#	print paybackByCountry.take(1000)
		else:
			print "Cannot connect to Database"

	def getCredit_Reports(self):
		if self._conn != None:
			self._cur.execute("SELECT id, country, pays_in_time, is_late, repaid_loans, active_loans FROM credit_reports")
#			self._cur.execute("SELECT id, credit_label, pays_in_time, is_late, repaid_loans, active_loans FROM credit_reports")
#			self._cur.execute("SELECT id, has_credit_check, pays_in_time, is_late, repaid_loans, active_loans FROM credit_reports")
#			self._cur.execute("SELECT id, has_job, pays_in_time, is_late, repaid_loans, active_loans FROM credit_reports")
#			self._cur.execute("SELECT id, has_fake_email, pays_in_time, is_late, repaid_loans, active_loans FROM credit_reports")
#			self._cur.execute("SELECT id, gender, pays_in_time, is_late, repaid_loans, active_loans FROM credit_reports")
#			self._cur.execute("SELECT id, has_verified_phone, pays_in_time, is_late, repaid_loans, active_loans FROM credit_reports")
#			self._cur.execute("SELECT id, has_linkedin_login, pays_in_time, is_late, repaid_loans, active_loans FROM credit_reports")
#			self._cur.execute("SELECT id, has_otc, pays_in_time, is_late, repaid_loans, active_loans FROM credit_reports")
#			self._cur.execute("SELECT id, has_bitcointalk, pays_in_time, is_late, repaid_loans, active_loans FROM credit_reports")
#			self._cur.execute("SELECT id, has_verified_address, pays_in_time, is_late, repaid_loans, active_loans FROM credit_reports")
#			self._cur.execute("SELECT id, has_ebay, pays_in_time, is_late, repaid_loans, active_loans FROM credit_reports")
#			self._cur.execute("SELECT id, paypal_email_consistency, pays_in_time, is_late, repaid_loans, active_loans FROM credit_reports")
#			self._cur.execute("SELECT id, geo_demerit, pays_in_time, is_late, repaid_loans, active_loans FROM credit_reports")

#			self._cur.execute("SELECT id, linkedin_connections, pays_in_time, is_late, repaid_loans, active_loans FROM credit_reports")
#			self._cur.execute("SELECT id, account_age, pays_in_time, is_late, repaid_loans, active_loans FROM credit_reports")
#			self._cur.execute("SELECT id, strongly_connected, pays_in_time, is_late, repaid_loans, active_loans FROM credit_reports")

			self._credit_reports = self._sc.parallelize(self._cur.fetchmany(-1))

			""" set the positive and negative examples using the is_late in credit_reports
			"""	
			# get the positive users who has payment and no late payment		
			self._positive_credit = self._credit_reports.filter(lambda s: s[4] > 0)

			# get the negatvie users who have late payment
			self._negative_credit = self._credit_reports.filter(lambda s: s[3] is True)

			# map to 	
			postiveByCountry = self._positive_credit.map(lambda s : (s[1], 1)).reduceByKey(lambda x, y : x+y)
			negativeByCountry = self._negative_credit.map(lambda s : (s[1], 1)).reduceByKey(lambda x, y : x+y)
			#paybackByCountry = postiveByCountry.fullOuterJoin(negativeByCountry).map(lambda s : (s[0], (s[1][0] if isinstance(s[1][0], int) else 0, s[1][1] if isinstance(s[1][1], int) else 0))).map(lambda s : (s[1][1]*100.0/(s[1][1]+s[1][0]), (s[0], s[1][0], s[1][1]))).sortByKey()
			paybackByCountry = postiveByCountry.fullOuterJoin(negativeByCountry).map(lambda s : (s[0], (s[1][0] if isinstance(s[1][0], int) else 0, s[1][1] if isinstance(s[1][1], int) else 0))).map(lambda s : (s[1][1]*100.0/(s[1][1]+s[1][0]), (s[0], s[1][0], s[1][1])))

			plt.figure(1)
			paybackCollect = paybackByCountry.collect()
			
			latepayCount = 0
			paymentnolate = 0 
			labels = []
			for a in paybackCollect:
				latepaid = a[1][2]
				latepayCount += latepaid
				paidback = a[1][1] if isinstance(a[1][1], int) else 0
				paymentnolate += paidback
				plt.subplot(3,1,1)
				plt.bar(paybackCollect.index(a), a[0]+0.00000001 if isinstance(a[0], float) else 0.0)
				plt.subplot(3,1,2)
				plt.bar(paybackCollect.index(a), paidback)
				plt.subplot(3,1,3)
				plt.bar(paybackCollect.index(a), latepaid+0.0000001)
				labels.append(a[1][0])
				print(a)
			plt.subplot(3,1,1)
			plt.xlabel("Percent of late payment by country using is_late in credit_reports ")
			plt.xticks(range(len(labels)), labels)

			plt.subplot(3,1,2)			
			plt.xlabel("Number of total payment by country")
			plt.xticks(range(len(labels)), labels)

			plt.subplot(3,1,3)
			plt.xlabel("Number of late payment by country")
			plt.xticks(range(len(labels)), labels)	
			plt.show()

			print "Total Late Payment: " + str(latepayCount) + "; Tatal Payment not late: " + str(paymentnolate) + ";  Total Late pay Ratio: " + str(latepayCount*100.0/(latepayCount + paymentnolate)) + ";   Negative customver: " + str(self._negative_credit.count())
		#	print paybackByCountry.take(1000)
		else:
			print "Cannot connect to Database"

if __name__ == "__main__":
	DA = DataAnalysis()
	DA.getCredit_Reports()
	#DA.badBehaviorsAnalysis()
