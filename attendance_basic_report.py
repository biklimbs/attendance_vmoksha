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
#sys.path.append("./test")
#from test_1.google_drive_api import *
#print(test_1.google_drive_api.__path__)
#import test.google_drive_api
from google_drive_api import *

#---Configuring log filename---
log_file=os.path.splitext(os.path.basename(__file__))[0]+".log"
log = logger_config.configure_logger('default', ""+DIR+""+LOG_DIR+"/"+log_file+"")

#---File name---
EXCEL_FILE="./attendance_file/attendance.xls"

#---Server credentials---
HOST= "spark.vmokshagroup.com"
USER="root"
PASSWORD="Vmmobility@1234"
SOURCE_DATABASE='vm_attendance'

#---DB TABLE NAMES---
TABLE_NAME='attendance_raw'

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
def insert_into_db(df):
    connection=connect_to_db()
    with connection.cursor() as cursor:
        for index,row in df.iterrows():
            try:
                #print(df.at[index,"Employee ID"],df.at[index,"Employee Name"],df.at[index,"Gender"],df.at[index,"Shift"],str(df.iloc[index,date_column]),str(df.iloc[index,date_column+1]),str(df.iloc[index,date_column+2]),str(correct_date))			                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                  
                sql = "INSERT INTO "+TABLE_NAME+" (`employee_id`, `employee_name`, `shift`, `date`, `in_time`, `out_time`, `break_duration`, `work_duration`, `over_time`, `total_duration`, `status`, `remarks`) values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
                data=(str(df.at[index,"Employee Code"]),str(df.at[index,"Employee Name"]),str(df.at[index,"Shift"]),str(df.at[index,"sheet_date"]),str(df.at[index,"A. InTime"]),str(df.at[index,"A.OutTime"]),str(df.at[index,"D. Break D"]),str(df.at[index,"W. Duration"]),str(df.at[index,"OT"]),str(df.at[index,"T Duration"]),str(df.at[index,"Status"]),df.at[index,"Remarks"])
                cursor.execute(sql,data)
                connection.commit()
                log.info(colored("Inserting into table","cyan"))
                pass
            except pymysql.err.IntegrityError as e:
                log.error(colored(str(e),"red"))
            except Exception as e:
                log.error(colored(str(e),"red"))	
    return "updated Successfully"	

#---Converts given file to Dataframe---
def read_excel_file(file_name):
    df = pd.read_excel(file_name)
    return df

#---Returns date of sheet---
def get_sheet_date(df):
    #print(df)
    date_index=df.index[df['Unnamed: 5'].str.match('\d{1,2}-[a-zA-Z]+-\d{4}',na=False)].values[0]
    sheet_date=df.at[date_index,"Unnamed: 5"]
    date_obj=datetime.strptime(sheet_date,"%d-%b-%Y")
    return date_obj

#---Cleans Dataframe and converts into proper format---
def clean_df(df):
    """
    This function takes dataframe and cleans all unwanted rows and columns.
    Sets column name and returns modified dataframe.
    """
    sheet_date=get_sheet_date(df)
    column_index=df.index[df["Unnamed: 3"] == 'Employee Code'].values[0]#Checks for "Employee Code" in column "Unnamed: 3" and gets row number if it is present
    df=df.drop(df.index[0:column_index])#deletes all row above column index
    df=df.dropna(axis="columns",how="all")#Drops all column where it is filled with "NULL" values
    df = df.reset_index(drop=True)#Resetting index
    df.columns=df.loc[0]#Setting column name with value from first row
    df=df.drop(df.index[0])
    df = df.reset_index(drop=True)
    df["sheet_date"]=sheet_date
    return df

#---Main function---
def main():
    download_status=download_file_from_drive()
    if download_status:
        df=read_excel_file(EXCEL_FILE)
        df=clean_df(df)
        df = df.replace({pd.np.nan: None})
        status=insert_into_db(df)
        log.info(colored(str(status),"yellow"))
        try:
            os.remove(ATTENDANCE_FILE_DOWNLOAD+"/attendance.xls")
        except Exception as e:
            log.error(colored(str(e),"red"))
    else:
        log.error(colored("file not downloaded","red"))
    
#---Main function called---
if __name__=="__main__":
	main()
