import pandas  as pd
from google.cloud import bigquery
#from sqlalchemy import create_engine
import  pyodbc
import time
import datetime
import logging 

def  bigquery_authtentication():
    
    client = bigquery.Client.from_service_account_json(r'bigquerykey.json' )
    
    
    return client


def  querying_bigquery(client,dataset,table_name,sqlquery):
    ### used only when the  default table is not enough and we need to create a custom table
    job_config = bigquery.QueryJobConfig()

    # Set the destination table. Here, dataset_id is a string, such as:
    # dataset_id = 'your_dataset_id'
    table_ref = client.dataset(dataset).table(table_name)
   # job_config.allow_large_results = True
   # job_config.destination = table_ref
    # The write_disposition specifies the behavior when writing query results
    # to a table that already exists. With WRITE_TRUNCATE, any existing rows
    # in the table are overwritten by the query results.
  # Set configuration.query.createDisposition
    #job_config.create_disposition = 'CREATE_IF_NEEDED'

        # Set configuration.query.writeDisposition
    #job_config.write_disposition = 'WRITE_APPEND'

    query_job = client.query(sqlquery,job_config=job_config)
    
    return query_job.to_dataframe()

 


def big_query_download(client,startdate,enddate,startdate2,enddate2):
        sql="""
         -- standard sql
  select *

from `datasetname.ga_sessions_*`, unnest(hits) as hits, unnest(hits.product) as product

where   ( ( _table_suffix between '"""+startdate+"""' and '""" +enddate+ """' ) or ( _table_suffix between '"""+startdate2+"""' and '""" +enddate2+ """' ))  

 
 
   """
        a=querying_bigquery(client,'dataset', 'tablename',sql)
        return a
    
 
    
if __name__ == '__main__':
    ##creating and preparing the logging file
    myapp="""\\"""+ str(datetime.datetime.now().strftime("%Y-%m-%d_%H-%M-%S"))  +".log"
    
    path=r"C:\logs" 
    
    logger = logging.getLogger('bigquery')
    hdlr = logging.FileHandler( path+myapp )
    formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s')
    hdlr.setFormatter(formatter)
    logger.addHandler(hdlr) 
    print(time.asctime( time.localtime(time.time()) ))
    logger.setLevel(logging.DEBUG)
     
     
    try:
        
            client= bigquery_authtentication()
            print('connected to bigquery')
            logger.info('connected to bigquery')
            print('downloading big query data')
            now = datetime.datetime.now()+datetime.timedelta(days=-30)
            startdate=str(now.year)+str('{num:02d}'.format(num=now.month))+str('{num:02d}'.format(num=now.day))
            now = datetime.datetime.now()+datetime.timedelta(days=-1)
            enddate=str(now.year)+str('{num:02d}'.format(num=now.month))+str('{num:02d}'.format(num=now.day))
    #print(str(now.year)+str('{num:02d}'.format(num=now.month))+str('{num:02d}'.format(num=now.day)))
            #startdate="20180901"
            #enddate="20180930"
            logger.info( startdate)
            logger.info(enddate)
            now = datetime.datetime.now()+datetime.timedelta(days=-365)
            startdate2=str(now.year)+str('{num:02d}'.format(num=now.month))+str('{num:02d}'.format(num=now.day))
            now = datetime.datetime.now()+datetime.timedelta(days=-350)
            enddate2=str(now.year)+str('{num:02d}'.format(num=now.month))+str('{num:02d}'.format(num=now.day)) 
            logger.info(startdate2)
            logger.info(enddate2)
            print(time.asctime( time.localtime(time.time()) ))
            a=big_query_download(client,startdate,enddate,startdate2,enddate2)    
            print(time.asctime( time.localtime(time.time()) ))
            print('Finished downloading big query data')
            logger.info('Finished downloading big query data')
            bigqueryflag='downloaded'
        else:
            logger.error('sql connection failed')
            
    except Exception as e:
         logger.error(e)
    
     