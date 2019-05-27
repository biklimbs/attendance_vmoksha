'''
This file contains all the basic function required for accessing Database.
'''
#---Connection to "DATABASE_Exception"---
def connect_to_db():
    '''
    Creates connection object for specified database and returns connection object.
    '''
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

#---Updating table---
def update_db(df):
    connection=connect_to_db()
    with connection.cursor() as cursor:
        for index,row in df.iterrows():
            try:			
                sql ="update "+AS_ONEPRICE_PREPROCESS_TABLE+" set remarks_flag=1,remarks="+SQL_LEFT+str(df.at[index,"remarks"])+SQL_LEFT+" where link= "+SQL_LEFT+str(df.at[index,"link"])+SQL_LEFT
                cursor.execute(sql)
                connection.commit()
                log.info(colored("Updating table","yellow"))
            except Exception as e:
                log.error(colored(str(e),"red"))
    connection.close()