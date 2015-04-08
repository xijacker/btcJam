from pyspark.sql import SQLContext
from pyspark import SparkContext
import psycopg2 
import datetime
import matplotlib.pyplot as plt
import matplotlib.lines as lines
from localDBConnection import LocalSql
from collections import deque
import sys

MARKERS = ['o', '+']
COLORS = ['r', 'b', 'g', 'c', 'm', 'y', 'k', 'r', 'b', 'g', 'c', 'm', 'y', 'k'] 
class CohortAnalysis:
	""" CohortAnalysis analyzes the corhort of the loans using table loans and multi_payments
	It will anayze the cohort payments related to the time, country, gender, facebook etc.

	currently, it will use the local PostgreSql data base
	"""
	
	
	def __init__(self):
		self._sc = SparkContext("local", "btcJam Cohort Analysis")
		localDbConn = LocalSql(self._sc)
#		self._curDate = datetime.date.today()
		self._curDate = datetime.date(2015,04,6)
		self._cur = localDbConn.getCursor()
		self._sqlContext = localDbConn.getSqlContext()
		self._conn	= localDbConn.getConn()
		self._AVERAGE_DAYS = 30 # number of days to calculate the average 
		self._DevelopedCountries = set(['AD','AT','AU','BE','CA','CH','DE','DK','ES', 'IE','EU','FI','FR','GB','GG','GI','HK','IM','IR','IS','IT','JE','JP','KR','LU','MC','MP','MT','NL','NO','NZ','PT','RE','SE','SG','SM','SV','SZ','UK','US','A1', 'United States', 'United Kingdom', 'Belgium', 'Australia', 'Italy']) 
		self.legends = []

	def readData(self):
		if self._conn != None:
			self._cur.execute("SELECT id, loan_id, dtpayment, dtpaid, total_multi_payments, multi_payment_number FROM multi_payments WHERE dtpayment < Now()")
			self._multiPayments = self._sc.parallelize(self._cur.fetchmany(-1))
			self._cur.execute("SELECT user_id, id, created_at FROM loans where reimbursed = False")
			self._loans = self._sc.parallelize(self._cur.fetchmany(-1))
			# Filter out the refinancing loans, filter out the reimbursed loans
			#self._loans = self._loans.filter(lambda s: s[1])
			self._cur.execute("SELECT id, country FROM users")
			self._users = self._sc.parallelize(self._cur.fetchmany(-1))

			# 30 days loan only and removed the refinancing 
			self._cur.execute("SELECT id, user_id, loan_id, payment_type_id, parent_listing_id FROM listings WHERE payment_type_id in (4,5,6) and (id not in (SELECT distinct parent_listing_id FROM listings where parent_listing_id is not null))")

			# 30 days loan only 
#			self._cur.execute("SELECT id, user_id, loan_id, payment_type_id, parent_listing_id FROM listings WHERE payment_type_id in (4,5,6)")


			# weekly loans only
#			self._cur.execute("SELECT id, user_id, loan_id, payment_type_id, parent_listing_id FROM listings WHERE payment_type_id in (3)")

			# all oans 
#			self._cur.execute("SELECT id, user_id, loan_id, payment_type_id, parent_listing_id FROM listings")

			self._listings30days = self._sc.parallelize(self._cur.fetchmany(-1))
			self._cur.execute("SELECT id FROM listings")
			self._listingsIds = self._sc.parallelize(self._cur.fetchmany(-1)).collect()

		else:
			print "Cannot connect to PostgreSql"	

	def graphPaybackRatio(self, dtPayments):
		dtPaymentsCollect = dtPayments.values().collect()
		dates = datenum(dtPayments.keys().collect(), 'yyyymmdd')
		plt.plot(dates, dtPayments)
#		for a in dtPaymentsCollect:
#			plt.bar(dtPaymentsCollect.index(a), a[0])
		plt.show()

	def drawLatepayRatioInGivenDays(self, dtPayments, label='testttt', mkr='o', days = 30, clr = 'r'):
		dtPaymentsCollect = dtPayments.collect()
#		print dtPayments.take(10)
#		print dtPaymentsCollect[0]
		sumLatePay = 0
		sumOntimePay = 0
		sumNopayback = 0

		tempDeque = deque()
		tempData = None
		emptydate = datetime.date.today() + datetime.timedelta(days = - 10000)

#		print dtPayments.first()

		preAddedDate = emptydate

		X = []
		Y = []
		Y1 = []
		for a in dtPaymentsCollect: # while curDay <= self._curDate:
			selectedDate = a[0]
			if selectedDate > self._curDate:
				break
			tempDeque.append(a)

			while (selectedDate - preAddedDate).days > 0:
				if preAddedDate == emptydate:
					preAddedDate = selectedDate
				else:
					preAddedDate += datetime.timedelta(days = 1)
				preAddedDate = selectedDate
				print a
				if (selectedDate-preAddedDate).days == 0:
					sumNopayback += a[1][3]
					sumLatePay += a[1][2]
					sumOntimePay += a[1][1]
				else:
					print "+++++++++++++++ " + str(preAddedDate) +  ";;; " + str(selectedDate)

				if tempData is None:
					tempData = tempDeque.popleft()

	#			print "---------" + str((a[0] - tempData[0]).days) + ";;;;;;" +  str(a) + ";         " +  str(tempData)
				
				while (preAddedDate - tempData[0]).days >= days:
					sumNopayback -= tempData[1][3]
					sumLatePay -= tempData[1][2]
					sumOntimePay -= tempData[1][1]
#					print "---------" + str((a[0] - tempData[0]).days) + ";;;;;;" +  str(a) + ";         " +  str(tempData)
					tempData = tempDeque.popleft()	
				print str(preAddedDate) + ";     " + str(sumLatePay*100.0/(sumLatePay + sumOntimePay)) + ";   late Payments: " + str(sumLatePay) + ";      OntimePayments:  " + str(sumOntimePay) + ";  Never payback: " + str(sumNopayback)
				X.append(preAddedDate)
				Y.append(sumLatePay*100.0/(sumLatePay + sumOntimePay))
				Y1.append(sumLatePay + sumOntimePay)
		plt.subplot(2, 1, 1)		
		plt.plot(X, Y, color = clr, marker='+', label = label)
		plt.ylabel('Late payback ratio in the past ' + str(days) + ' days')		
		plt.legend()

		plt.subplot(2, 1, 2)		
		plt.plot(X, Y1, color = clr, marker='+')
		plt.ylabel('Total due payments in the past ' + str(days) + ' days')		
	
		plt.legend()
		
	def dailyPaments(self):
		
		# Calculate the map from dtPayment to (late Payment number, ontime Payment number) 
		dtPayments = self._multiPayments.map(lambda s : (s[2], (1 if s[3] is not None and s[2] >= s[3] else 0, 1 if s[3] is None or s[2] < s[3] else 0))).reduceByKey(lambda x, y: (x[0]+y[0], x[1]+y[1]))
		
		# Calculate the map from dtPayment to (late payment ratio, late Payment number, ontime Payment number 
		dtPayments = dtPayments.map(lambda s: (s[0], (100.0*s[1][0]/(s[1][0] + s[1][1]), s[1][0], s[1][1], s[1][2]))).sortByKey()

		#self.graphPaybackRatio(dtPayments)
#		self.drawLatepayRatioInGivenDays(dtPayments, self._loans,'late payment', self._users, days = self._AVERAGE_DAYS)
		self.drawLatepayRatioInGivenDays(dtPayments, label = 'late payment', days = self._AVERAGE_DAYS)

	def totalLatePaymentRatio(self, dtPayments):	
		# dtPayments for all loans with pay count of late payment, ontime payment and country
		dtPayments = dtPayments.reduceByKey(lambda x, y: (x[0]+y[0], x[1]+y[1], x[2]+y[2]))
		print dtPayments.take(30)
		# map from dtPayments to late percent payments sorted by dtPayments
		dtPayments = dtPayments.map(lambda s: (s[0], (100.0*s[1][0]/(s[1][0] + s[1][1]), s[1][0], s[1][1], s[1][2]))).sortByKey()

#		print dtPayments.take(60)

		self.drawLatepayRatioInGivenDays(dtPayments, days = self._AVERAGE_DAYS, label ='total late payment', mkr = '+')
		
	def totalLatePaymentRatioForCreatedDay(self, dtPayments):	
		# dtPayments for all loans with pay count of late payment, ontime payment and country
		print dtPayments.take(50)
		dtPayments = dtPayments.reduceByKey(lambda x, y: (x[0]+y[0], x[1]+y[1], x[2]+y[2]))
		print dtPayments.take(30)
		# map from dtPayments to late percent payments sorted by dtPayments
		dtPayments = dtPayments.map(lambda s: (s[0], (100.0*s[1][0]/(s[1][0] + s[1][1]), s[1][0], s[1][1], s[1][2]))).sortByKey()

#		print dtPayments.take(60)

		self.drawLatepayRatioInGivenDays(dtPayments, days = self._AVERAGE_DAYS, label = 'Total')


	def groupLatePaymentRatio(self, dtPayments, Group, notGroupIncluded, grpName = "Developed"):

		# dtGroupPayments for all loans with pay count of late payment, ontime payment, never pay back in for the countries in Group
		print dtPayments.take(10)
		dtGroupPayments = dtPayments.filter(lambda s: s[1][3][0] in Group).reduceByKey(lambda x, y: (x[0]+y[0], x[1]+y[1], x[2]+y[2]))
#		dtGroupPayments = dtPayments.filter(lambda s: s[1][0] in Group).reduceByKey(lambda x, y: (x[0]+y[0], x[1]+y[1], x[2]+y[2]))

#		print dtGroupPayments.take(20)

		# map from dtPayments to late percent payments sorted by dtPayments
		dtGroupPayments = dtGroupPayments.map(lambda s: (s[0], (100.0*s[1][0]/(s[1][0] + s[1][1]), s[1][0], s[1][1], s[1][2]))).sortByKey()

		if notGroupIncluded:
			color = 'b'
			self.drawLatepayRatioInGivenDays(dtGroupPayments, days = self._AVERAGE_DAYS, label = 'Group:'+ grpName, clr = color)
		else:
			color = 'y'
			self.drawLatepayRatioInGivenDays(dtGroupPayments, days = self._AVERAGE_DAYS, label = 'Group:'+ grpName, clr = color)

		if notGroupIncluded:
			# dtGroupPayments for all loans with pay count of late payment, ontime payment in for the countries in Group
			dtGroupPayments = dtPayments.filter(lambda s: s[1][3][0] not in Group).reduceByKey(lambda x, y: (x[0]+y[0], x[1]+y[1], x[2]+y[2]))

			# map from dtPayments to late percent payments sorted by dtPayments
			dtGroupPayments = dtGroupPayments.map(lambda s: (s[0], (100.0*s[1][0]/(s[1][0] + s[1][1]), s[1][0], s[1][1], s[1][2]))).sortByKey()

			self.drawLatepayRatioInGivenDays(dtGroupPayments, days = self._AVERAGE_DAYS, label = 'Out of above group', clr = 'g')

	""" Analaysis the late payments based on the Order of payments (first, second, ...) for the loans created from startDate (included) 
	to one month later at the same date (not included)
	"""		

	def cohortMonthlyWithOrderOfPayments(self, dtPayments, startDate, numPeriods, periodDays = 30):

		results = []
		for month in range(numPeriods):
			# set the enddate being a month from startDate
			endDate = startDate
			endDate = datetime.date(startDate.year+startDate.month/12, startDate.month%12 + 1, startDate.day)
			#endDate = startDate  + datetime.timedelta(days = 2) # ????DEBUG

			# filter to include only the payments created in the period
			periodPayments = dtPayments.filter(lambda s: s[0] >= startDate and s[0] < endDate)
	#		print periodPayments.take(300)

			# periodPayments for all loans with pay count of late payment, ontime payment and country
			# reduced by the order of payments
			result = periodPayments.values().map(lambda s: (s[4], (s[0], s[1], s[2]))).reduceByKey(lambda x, y: (x[0]+y[0], x[1]+y[1], x[2]+y[2])).sortByKey()#.map(lambda s: (startDate, s))
			results.append([startDate, result.collect()])

			# map from dtPayments to late percent payments sorted by dtPayments
	#		dtPayments = dtPayments.map(lambda s: (s[0], (100.0*s[1][0]/(s[1][0] + s[1][1]), s[1][0], s[1][1], s[1][2]))).sortByKey()

	#		print dtPayments.take(60)

	#		self.drawLatepayRatioInGivenDays(dtPayments, self._AVERAGE_DAYS)
			startDate = endDate		
#			print results.collect()
		width = 1
		i = 1
		for r in results:
			X1=[]
			Y1=[]
			Y2=[]
			Y3=[]
			for a in r[1]:
				#plt.bar(a[0] * width, )
				X1.append(a[0] * width)
				Y2.append(a[1][0]+a[1][1])

				''' Never paid back'''
				Y1.append(a[1][2]*100.0/(a[1][0]+a[1][1]))
				Y3.append(a[1][2])

				''' Late payback'''
#				Y1.append(a[1][1]*100.0/(a[1][0]+a[1][1]))
#				Y3.append(a[1][1])

			plt.subplot(3,1,1)
			plt.plot(X1,Y1, color=COLORS[i], marker=MARKERS[0], label = r[0])
			plt.subplot(3,1,2)
			plt.plot(X1,Y2, color=COLORS[i], marker=MARKERS[0], label = r[0])
			plt.subplot(3,1,3)
			plt.plot(X1,Y3, color=COLORS[i], marker=MARKERS[0], label = r[0])
			print str(r) + '\n'
			i+= 1
		plt.subplot(3,1,1)
		plt.ylabel('never payback ratio')
		plt.subplot(3,1,2)
		plt.ylabel('number of total payments')
		plt.subplot(3,1,3)
		plt.ylabel('number of not payback payments')
		plt.xlabel('Multiple payment number')
		plt.legend()
#		print results		
	"""
	Using daily payment data from multiPayments, Users, and Loans to analysis the cohort of the late payback ratio
	Note: 1. only anlayze the 30 days payment term loansJoinedUsers

	"""

	def cohortDailyPaments(self, cohortType = 'PAYMENTDAY', analysisType = 'LONGPERIOD'):

		# join the loans created_at with user country, a map from loan_id, (country, created_at)
		loansJoinedUsers =  self._loans.map(lambda s : (s[0], (s[1], s[2]))).leftOuterJoin(self._users).values().map(lambda s : (s[0][0], (s[1], s[0][1])))

#		self._listings30days = self._listings30days.filter(lambda s : s[4] is None or s[4] not in self._listingsIds)
		
		loansJoinedUsers = loansJoinedUsers.join(self._listings30days.map(lambda s: (s[2], s[3]))).map(lambda s: (s[0], s[1][0]))

#		print loansJoinedUsers.take(100)

#		print multiPayments.take(100)
		# join the multi payments with the country and created_at 
		dtPayments =self._multiPayments.map(lambda s: (s[1], s)).join(loansJoinedUsers).values()
		print dtPayments.take(300)

		if cohortType == 'PAYMENTDAY':	
			''' cohort analysis based on payment days'''
			# dtPayments for all loans with pay count of late payment, ontime payment, never payed back, and country 
			dtPayments = dtPayments.map(lambda s : (s[0][2], (1 if s[0][3] is not None and s[0][2] >= s[0][3] else 0, 1 if s[0][3] is None or s[0][2] < s[0][3] else 0, 1 if s[0][3] is None else 0, s[1], s[0][5])))
			print dtPayments.take(100)
		elif cohortType == 'CREATEDDAY':
			''' cohort analysis based on created at date'''
			# dtPayments for all loans with count number of late payment, ontime payment, never payback, and country
			dtPayments = dtPayments.map(lambda s : (s[1][1].date(), (1 if s[0][3] is not None and s[0][2] >= s[0][3] else 0, 1 if s[0][3] is None or s[0][2] < s[0][3] else 0, 1 if s[0][3] is None else 0, s[1], s[0][5])))
			print dtPayments.take(100)


		if analysisType == 'LONGPERIOD':
			# Total late payback ratio
			self.totalLatePaymentRatioForCreatedDay(dtPayments)

			# Late payback ratio ralated to developed/undeveloped countries
			self.groupLatePaymentRatio(dtPayments, self._DevelopedCountries, True)

			# Late payback ration ralated to developed/undeveloped countries
#			country = ['US', 'United States'] #['BR', 'CA']
#			self.groupLatePaymentRatio(dtPayments, country, True, country[0])

			plt.xlabel (cohortType + ' Based')
			plt.show()
		elif analysisType == 'MONTHLY':		
			self.cohortMonthlyWithOrderOfPayments(dtPayments, datetime.date(2014, 9 ,1), 6)	


def main(argv):
	cohort = CohortAnalysis()
	cohort.readData()
	paymentType = 'CREATEDDAY' #or 'PAYMENTDAY'
	displayType = 'MONTHLY' # or 'LONGPERIOD' 

	if len(argv) > 0:
		paymentType = argv[0]
		print "+++++++++++++++++++  " + paymentType 
	if len(argv) > 1:
		displayType = argv[1]
		if argv[0] == 'MONTHLY':
			paymentType = 'CREATEDDAY'
		print "-------------------  " + displayType 
	
#	cohort.dailyPaments()
# 	plt.subplot(2,1,1)

	# Month over month cohort based on created date
#	cohort.cohortDailyPaments('CREATEDDAY', 'MONTHLY')

	# all the data based on created date for all the period
#	cohort.cohortDailyPaments('CREATEDDAY', 'LONGPERIOD')

	# all the data based on payment date for all the period
	cohort.cohortDailyPaments(paymentType, displayType)

	'''
	plt.subplot(2,1,2)
	cohort.cohortDailyPaments(paymentType)
	plt.xlabel ('Payment Day based')
	'''
#	plt.legend('total late payback ratio')
#	plt.legend('Developed Country')
#	plt.legend('Developing Country')
#	plt.legend('U')

	plt.show()
	
if __name__ == "__main__":
	main(sys.argv[1:])


