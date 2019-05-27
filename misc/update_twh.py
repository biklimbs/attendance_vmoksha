import pandas as pd
import sys
import pymysql
from logger_config import *
import logger_config

#---Configuring log filename---
log_file=os.path.splitext(os.path.basename(__file__))[0]+".log"
log = logger_config.configure_logger('default', ""+DIR+""+LOG_DIR+"/"+log_file+"")


MY_SQL_HOST_NAME = 'spark.vmokshagroup.com'
MY_SQL_USER_NAME = 'root'
MY_SQL_PASSWORD = 'Vmmobility@1234'
MY_SQL_DB_NAME = 'vm_attendance'
ATTENDANCE_TABLE= "attendance_may"

#---Connection to "DATABASE_Exception"---
def connect_to_db():
	connection = pymysql.connect(host=MY_SQL_HOST_NAME,
                                     user=MY_SQL_USER_NAME,
                                     password=MY_SQL_PASSWORD,
                                     db=MY_SQL_DB_NAME,
                                     charset='utf8mb4',
                                     cursorclass=pymysql.cursors.DictCursor)
	return connection


def read_mysql():
	connection=connect_to_db()
	with connection.cursor() as cursor:
		try:
			# sql='SELECT distinct Name,Country,Email,Port_of_Destination,Phone_Mobile from '+INQUIRY_TABLE+' where Local_Time>NOW()-1;'
			sql='SELECT * from '+ATTENDANCE_TABLE+';'
			print(sql)
			cursor.execute(sql)
			data=cursor.fetchall()
			df=pd.DataFrame(data)
			cursor.close()
		except Exception as e:
			log.error(e)
	return df

def update_table(df):
	connection=connect_to_db()
	with connection.cursor() as cursor:
		try:
			for i,row in df.iterrows():
				# sql='update '+ATTENDANCE_TABLE+' set twh_float=\"'+str(df.at[i,'twh_float'])+'\" where employee_id=\"'+str(df.at[i,'employee_id'])+'\" and employee_name=\"'+str(df.at[i,'employee_name'])+'\" and date=\"'+str(df.at[i,'date'])+'\" and twh=\"'+str(df.at[i,'twh'])+'\";'
				sql='update '+ATTENDANCE_TABLE+' set employee_name=\"'+str(df.at[i,'employee_name'])+'\",in_time_float=\"'+str(df.at[i,'in_time_float'])+'\",out_time_float=\"'+str(df.at[i,'out_time_float'])+'\",twh_float=\"'+str(df.at[i,'twh_float'])+'\" where employee_id=\"'+str(df.at[i,'employee_id'])+'\" and employee_name=\"'+str(df.at[i,'employee_name'])+'\" and date=\"'+str(df.at[i,'date'])+'\";'
				cursor.execute(sql)
				print('updating',sql)
				connection.commit()
			cursor.close()
		except Exception as e:
			log.error(e)



def replace_value(i):
	i=str(i)
	test,temp=i.split('s ',1)
	temp=temp[0:5]
	temp=temp.replace(':','.')
	return temp

def capital(i):
	i=i.title()
	return i

def main():
	df=read_mysql()
	df["employee_name"]=df['employee_name'].apply(capital)
	df["in_time_float"]=df["in_time"].apply(replace_value)
	df["out_time_float"]=df["out_time"].apply(replace_value)
	df["twh_float"]=df['twh'].apply(replace_value)
	update_table(df)



if __name__=="__main__":
	main()