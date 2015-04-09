
''' Constants '''
DEVELOPED_COUNTRIES = ['AD','AT','AU','BE','CA','CH','DE','DK','ES','IE','EU','FI','FR','GB','GG','GI','HK','IM','IR','IS','IT','JE','JP','KR','LU','MC','MP','MT','NL','NO','NZ','PT','RE','SE','SG','SM','SV','SZ','UK','US','A1', 'United States', 'United Kingdom', 'Belgium', 'Australia', 'Italy', 'India', 'Netherlands', 'Canada']
DA_FACTORS = ['COUNTRY', 'CREDIT_LABEL', 'HAS_CREDIT_CHECK', 'HAS_JOB', 'HAS_FAKE_EMAIL', 'GENDER', 'HAS_VERIFIED_PHONE', 'HAS_LINKEDIN_LOGIN', 'HAS_OTC', 'HAS_BITCOINTALK', 'HAS_VERIFIED_ADDRESS', 'HAS_EBAY', 'PAYPAL_EMAIL_CONSISTENCY', 'GEO_DEMERIT', 'LINKEDIN_CONNECTIONS', 'ACCOUNT_AGE', 'STRONGLY_CONNECTIONS']
DA_PAYMENT_TYPES = ['CREATEDDAY', 'PAYMENTDAY']
DA_ANALYSIS_TYPES = ['LONGPERIOD', 'MONTHLY']

''' Configuration variables'''
# The folowing data will be used by C
SELECTED_FACTOR = 'COUNTRY' #'CREDIT_LABEL'


# the group of countries being analyzed

# If set to MONTHLY for the six monthes cohort analysis, 
#DA_ANALYSIS_TYPE = 'MONTHLY'

if 'DA_ANALYSIS_TYPE' not in locals(): 
	DA_ANALYSIS_TYPE = 'LONGPERIOD'
	DA_GROUP = ['BR', 'RO', 'IN', 'ID', 'India', 'BG', 'TK',]#, ['MX', 'PH', 'RU']
	OCT_SPIKE_GROUP = ['MX', 'PH', 'RU', 'PH', 'PL', 'UA', 'RO', 'None', 'MD']
	DA_GROUP = DA_GROUP + OCT_SPIKE_GROUP
	print DA_GROUP 
#	DA_GROUP=['BR', 'RU','RO', 'IN', 'ID', 'India']#,
	DA_GROUP=DEVELOPED_COUNTRIES + OCT_SPIKE_GROUP #'MX', 'PH', 'ID', 'India', 'TK', 'RU', 'BR'] #['BR', 'CA']
	DA_GROUP_DESCRIPTION = str(DA_GROUP)
#	DA_GROUP_DESCRIPTION = 'Developed'
DA_WINDOW_DAYS = 30  # the number of days in the COHORT analysis
DA_COHORT_TYPE = 'CREATEDDAY'
#DA_COHORT_TYPE = 'PAYMENTDAY'
