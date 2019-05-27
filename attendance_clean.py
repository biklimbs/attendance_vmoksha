import pandas as pd
import os,sys
sys.path.append('/usr/local/lib/python3.5/dist-packages')
import pymysql.cursors
from time import sleep
from datetime import datetime
import re
from termcolor import colored
import warnings
warnings.filterwarnings("ignore")
from logger_config import *
import logger_config

#---Configuring log filename---
log_file=os.path.splitext(os.path.basename(__file__))[0]+".log"
log = logger_config.configure_logger('default', ""+DIR+""+LOG_DIR+"/"+log_file+"")

#---Server credentials---
HOST= "spark.vmokshagroup.com"
USER="root"
PASSWORD="Vmmobility@1234"
SOURCE_DATABASE='vm_attendance'

#---DB TABLE NAMES---
ATTENDANCE_RAW_TABLE='attendance_raw'
EMPLOYEE_DETAILS_TABLE="employee_details"
ATTENDANCE_CLEAN_TABLE="attendance_clean"

#---Sql queries---
EMPLOYEE_DETAILS_QUERY="SELECT * FROM "+EMPLOYEE_DETAILS_TABLE
ATTENDANCE_RAW_QUERY="SELECT * FROM "+ATTENDANCE_RAW_TABLE+" where clean_flag=0"
SQL_LEFT="\""
SQL_RIGHT="\","

#---Connection to "DATABASE_Exception"---
def connect_to_db():
	connection = pymysql.connect(host=HOST,
                                     user=USER,
                                     password=PASSWORD,
                                     db=SOURCE_DATABASE,
                                     charset='utf8mb4',
                                     cursorclass=pymysql.cursors.DictCursor)
	return connection

#---Read the data from  table in the db---
def read_data_from_db(connection,sql_query):
    '''
    Returns data in dataframe from table based on given query.
    '''
    with connection.cursor() as cursor:
        try:
            sql=sql_query
            #print(sql)
            cursor.execute(sql)
            if cursor.rowcount > 0:
                df=pd.DataFrame(cursor.fetchall())
            else:
                df=pd.DataFrame()	
        except Exception as e:
            log.error(e)
    connection.close()
    return df

#---Splits employee id---
def split_employee_id(temp):
    try:
        temp=temp.split("/")[1].strip()
    except Exception as e:
        log.error(colored(str(e),"red"))
    return temp   

#---Creates employee id by adding "VM/"---
def create_employee_id(temp):
    try:
        temp="VM/"+temp
    except Exception as e:
        log.error(colored(str(e),"red"))
    return temp

#---Returns employee deails df---
def get_employee_details():
    try:
        connection=connect_to_db()
        employee_df=read_data_from_db(connection,EMPLOYEE_DETAILS_QUERY)
    except Exception as e:
        log.error(colored(str(e),"red"))
        employee_df=pd.DataFrame()
    return employee_df

#---Returns attendance raw data---
def get_attendance_raw_data():
    try:
        connection=connect_to_db()
        attendance_df=read_data_from_db(connection,ATTENDANCE_RAW_QUERY)
        attendance_df['employee_org_id']=attendance_df['employee_id']
        attendance_df['employee_id'] = attendance_df['employee_id'].apply(create_employee_id)
    except Exception as e:
        log.error(colored(str(e),"red"))
        attendance_df=pd.DataFrame()
    return attendance_df


#---Merges two df into one df---
def merge_df(employee_df,attendance_df):
    try:
        result_df=pd.merge(attendance_df,employee_df[["employee_id","gender","account"]],on="employee_id")
    except Exception as e:
        log.error(colored(str(e),"red"))    
    return result_df

#---Inserts into database---
def insert_into_db(df):
    connection=connect_to_db()
    with connection.cursor() as cursor:
        for index,row in df.iterrows():
            try:
                #print(df.at[index,"Employee ID"],df.at[index,"Employee Name"],df.at[index,"Gender"],df.at[index,"Shift"],str(df.iloc[index,date_column]),str(df.iloc[index,date_column+1]),str(df.iloc[index,date_column+2]),str(correct_date))			                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                  
                sql = "INSERT INTO "+ATTENDANCE_CLEAN_TABLE+" (`employee_id`, `employee_name`, `gender`,`shift`, `date`, `in_time`, `out_time`, `work_duration`, `over_time`, `total_duration`, `attendance_status`, `remarks`,`in_time_float`,`out_time_float`,`twh_float`) values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
                data=(str(df.at[index,"employee_id"]),str(df.at[index,"employee_name"]),str(df.at[index,"gender"]),str(df.at[index,"shift"]),str(df.at[index,"date"]),str(df.at[index,"in_time"]),str(df.at[index,"out_time"]),str(df.at[index,"work_duration"]),str(df.at[index,"over_time"]),str(df.at[index,"total_duration"]),str(df.at[index,"attendance_status"]),df.at[index,"remarks"],str(df.at[index,"in_time_float"]),str(df.at[index,"out_time_float"]),str(df.at[index,"twh_float"]))
                cursor.execute(sql,data)
                connection.commit()
                log.info(colored("Inserting into table","cyan"))
                update_db(str(df.at[index,"employee_org_id"]),str(df.at[index,"date"]))
            except pymysql.err.IntegrityError as e:
                log.error(colored(str(e),"red"))
            except Exception as e:
                log.error(colored(str(e),"red"))	
    return "updated Successfully"

#---This function creates remarks in the table---
def update_db(emp_id,attendance_date):
    connection=connect_to_db()
    with connection.cursor() as cursor:
        try:			
            sql ="update "+ATTENDANCE_RAW_TABLE+" set clean_flag=1 where employee_id= "+SQL_LEFT+str(emp_id)+SQL_LEFT+" and date="+SQL_LEFT+str(attendance_date)+SQL_LEFT
            cursor.execute(sql)
            connection.commit()
            log.info(colored("Updating table","yellow"))
        except Exception as e:
            log.error(colored(str(e),"red"))
    connection.close()

#---Modifies employee name case to "camel case"--- 
def format_name_case(temp):
    try:
        temp=temp.title()
    except Exception as e:
        log.error(colored(str(e),"red"))
    return temp

#---Converts time into hours and minutes---
def convert_time(temp):
    try:
        #print(temp)
        temp=str(temp)
        hr,min,sec=temp.split(':')
        temp=float(hr+"."+min)
    except Exception as e:
        temp=0.0
        log.error(colored(str(e),"red"))
    return temp

#---Converts "P","A" to "present","full leave"---
def convert_attendance_status(temp):
    temp=str(temp).strip().lower()
    try:
        if temp=="½p":
            temp="half leave"
        elif temp=="p":
            temp="present"
        elif temp=="a":
            temp="full leave"
        else:
            pass
    except Exception as e:
        log.error(colored(str(e),"red"))
    return temp

#---Cleans df and format column value---
def clean_df(result_df):
    result_df['employee_name'] = result_df['employee_name'].apply(format_name_case)
    #Converts time to proper format
    result_df['over_time']=pd.to_datetime(result_df['over_time']).dt.time
    result_df['in_time']=pd.to_datetime(result_df['in_time']).dt.time
    result_df['out_time']=pd.to_datetime(result_df['out_time']).dt.time
    result_df['work_duration']=pd.to_datetime(result_df['work_duration']).dt.time
    result_df['total_duration']=pd.to_datetime(result_df['total_duration']).dt.time

    result_df['in_time_float'] = result_df['in_time'].apply(convert_time)
    result_df['out_time_float'] = result_df['out_time'].apply(convert_time)
    result_df['twh_float'] = result_df['total_duration'].apply(convert_time)

    result_df['attendance_status'] = result_df['status'].apply(convert_attendance_status)
    return result_df

#---Main function---
def main():
    try:
        employee_df=get_employee_details()
        attendance_df=get_attendance_raw_data()
        if attendance_df.empty:
            log.error(colored("Dataframe is empty","red"))
        else:
            result_df=merge_df(employee_df,attendance_df)
            result_df=clean_df(result_df)
            insert_into_db(result_df)
    except Exception as e:
        log.error(colored(str(e),"red"))
    
#---Main function called---
if __name__=="__main__":
	main()
