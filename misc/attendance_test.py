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

#---File name---
EXCEL_FILE="attendence_may2019.xlsx"

#---Server credentials---
HOST= "spark.vmokshagroup.com"
USER="root"
PASSWORD="Vmmobility@1234"
SOURCE_DATABASE='vm_attendance'

#---DB TABLE NAMES---
TABLE_NAME='attendance_may'

#---Connection to "DATABASE_Exception"---
def connect_to_db():
	connection = pymysql.connect(host=HOST,
                                     user=USER,
                                     password=PASSWORD,
                                     db=SOURCE_DATABASE,
                                     charset='utf8mb4',
                                     cursorclass=pymysql.cursors.DictCursor)
	return connection

#---Inserts into database---
def insert_into_db(df,date_df,table_name):
    connection=connect_to_db()
    with connection.cursor() as cursor:
        for index,row in df.iterrows():
            #---Date repeats 3 times---
            j=0
            for i in range(int(len(list(date_df))/3)):
                correct_date=get_date_format(i+1)
                if i==0:
                    date_column=i+5   
                else:
                    date_column=date_column+3
                try:
                    #print(df.at[index,"Employee ID"],df.at[index,"Employee Name"],df.at[index,"Gender"],df.at[index,"Shift"],str(df.iloc[index,date_column]),str(df.iloc[index,date_column+1]),str(df.iloc[index,date_column+2]),str(correct_date))			                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                  
                    sql = "INSERT INTO "+TABLE_NAME+" (`employee_id`, `employee_name`, `gender`, `date`, `in_time`, `out_time`, `twh`) values(%s,%s,%s,%s,%s,%s,%s)"
                    data=(str(df.at[index,"Employee ID"]),str(df.at[index,"Employee Name"]),str(df.at[index,"Gender"]),str(correct_date),str(df.iloc[index,date_column]),str(df.iloc[index,date_column+1]),str(df.iloc[index,date_column+2]))
                    cursor.execute(sql,data)
                    connection.commit()
                    log.info(colored("Inserting into table","cyan"))
                    pass
                except pymysql.err.IntegrityError as e:
                    log.error(colored(str(e),"red"))
                except Exception as e:
                    log.error(colored(str(e),"red"))	
    return "updated Successfully"	

#---Returns date in correct format---
def get_date_format(temp_day):
    date_string="2019-05-"+str(temp_day)
    date_object = datetime.strptime(date_string, "%Y-%m-%d")
    #correct_date=datetime.strptime(dt_string, "%Y-%m-%d")
    return date_object

#---Gives month---
def get_month_columns(df):
    date_df=df.drop(['Sl. No','Employee ID','Employee Name','Gender','Shift'],axis=1)
    return date_df

#---Main function---
def main():
    xls=pd.ExcelFile(EXCEL_FILE)
    for sheetname in xls.sheet_names:
        try:
            #df=xls.parse(sheetname,header=None)
            df=xls.parse(sheetname)
            df.fillna(value="",inplace=True)
            log.info(colored("Number of rows: "+str(len(df)),"yellow"))  
            date_df=get_month_columns(df)
            #print(df)
            insert_into_db(df,date_df,sheetname)
            #print(len(list(date_df)))
        except Exception as e:
            log.error(colored(str(e),"red"))

#---Main function called---
if __name__=="__main__":
	main()

