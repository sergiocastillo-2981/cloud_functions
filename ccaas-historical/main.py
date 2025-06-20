# TTEC Digital 2023
#
# This function is Triggered by HTTP/Scheduler
# It transfers historical data from CCAIP to bigquery, doing a series API call.
# 
# Version   Date        Autor       Comment
#  1        15-06-2023  JArias      Initial version of the function
#  2        07-10-2023  JArias      intalation in the lab 
#  3        03-01-2024  SCastillo   Updates to fix misspellings, DateTime columns conversions.
#  4        06-18-2024  SCastillo   Update to parse the message coming from PubSub
#                                   Message can be like write_truncate,table_name
#  5        01-22-2025  Ravi        Added error Handling while Authenticating to API to catch exceptions
#  6        02-21-2025  SCastillo   Added Stage tables for calls and chats
#  7        04-30-2025  SCastillo   Converted the function to be Triggered by HTTP/Scheduler instead of PubSub

## Imports Section ##
import base64

from cloudevents.http import CloudEvent
import functions_framework

import requests
from requests.auth import HTTPBasicAuth
import json
import csv
import datetime
from datetime import date, datetime, timedelta, timezone
import sys

from collections import OrderedDict
import os
import array
import random
import pandas as pd
import numpy as np #test
import pytz
from google.cloud import bigquery
from google.cloud import error_reporting


def secrets():
    try:
        #Referece to enviromental variable
        my_secret_value = os.environ['secret']
        
        #parcing the variables 
        result = dict((a.strip(), b.strip())  
                    for a, b in (element.split(':')  
                        for element in my_secret_value.split(', '))) 

    except:
        print("An exception occurred in secrets") 
    #debug
    #print (result)
    #/debug
    #return (result['project'],result['dataset'],result['user'],result['psw'],result['config_table']) disabled temporarily
    return (result['project'],result['dataset'],result['user'],result['psw'],result['config_table']) 


@functions_framework.http
def hello_http(request):
    """HTTP Cloud Function.
    Args:
        request (flask.Request): The request object.
        <https://flask.palletsprojects.com/en/1.1.x/api/#incoming-request-data>
    Returns:
        The response text, or any set of values that can be turned into a
        Response object using `make_response`
        <https://flask.palletsprojects.com/en/1.1.x/api/#flask.make_response>.
    
    request_json = request.get_json(silent=True)
    request_args = request.args

    if request_json and 'name' in request_json:
        name = request_json['name']
    elif request_args and 'name' in request_args:
        name = request_args['name']
    else:
        name = 'World'
    return 'Hello {}!'.format(name)
    """
    print("************************************************         CF00-Starting NEW instance of the function          ************************************************")
    
    action = ""
    
    #secrets 
    project,dataset,user,psw,config_table = secrets()
    
    #creating client instances
    bigquery_client = bigquery.Client()
    client = error_reporting.Client()
    

    #Creating Logging variables
    api_name= "Test"
    description = "this is a test"
    records_process = 0
    start_datetime = "2023-04-01T00:00:00.000Z"
    end_datetime = "2023-04-01T00:00:00.000Z"
    last_run_time_str = "2023-04-01T00:00:00.000Z"
    last_bq_time_str = "2023-04-01T00:00:00.000Z"
    json_object = [1]
    
    #creating configuration variables 
    link = " "
    last_run_datetime_str = datetime.now().strftime("%Y-%m-%dT%H:%M:%S.000Z")
    datetime_column = " "
    
    #Logging 1st step 
    description= 'CF01-Finding APIs list and configuration data'
    print(description)

    if action == "write_truncate":
        extra_where = f" and name='{action_table}'"
    else:
        extra_where=''
           
    #sample of Query query = f"CALL {dataset}.p_merge_emails('{action}')"
    #list of the active APIs
    #query = """SELECT name, link, `project`, dataset, datetime_column, last_bq_datetime_str, first_startdatetime,instance_id,instance_name FROM `"""+ config_table +"""` where type= 'historical'"""
    query = f"SELECT name, link, `project`, dataset, datetime_column, last_bq_datetime_str, concat( STRING(DATE(first_startdatetime)),'T00:00:00.000Z') first_startdatetime,instance_id,instance_name FROM `{config_table}` where type= 'historical' {extra_where}"
    job_config = bigquery.QueryJobConfig()
    print(query)
    api_list = bigquery_client.query(query, job_config=job_config)

    #Logging 1st step 
    description= 'CF02-Iterating thru all the active tables to retrieve APIs and configuration data'
    print(description)

    for row in api_list:
        name = row.name
        link = row.link
        project = row.project
        dataset = row.dataset
        datetime_column  = row.datetime_column
        last_bq_datetime_str = row.last_bq_datetime_str
        first_startdatetime = row.first_startdatetime
        instance_id = row.instance_id
        instance_name = row.instance_name
        
        print(f'CF03-Start processing API: -------------------------------------- { name.upper() } --------------------------------------')
        api_name=name        

        table_name = "t_" + name   
        table_id = ""+project+"."+dataset+"."+table_name+""
        
        #If table is empty or we are re-creating the table(write_truncate) we need to reset the startdatetime so the API Call can retrieve all the records
        if str(last_bq_datetime_str) == 'None' or action == "write_truncate":
            last_bq_datetime_str = first_startdatetime
        
        #print(last_bq_datetime_str)
        print("Start DateTime for table "+ table_name + " " +last_bq_datetime_str)

        #debug
        #print(type(last_bq_datetime_str))        
        
        #We need to convert the Start Date Time to datetime type to add one second and then convert back to string#

        #String to datetime format , removing the last character(Z) with the [:-1] and adding one second, so it will not repete the last record
        start_datetime = datetime.fromisoformat(last_bq_datetime_str[:-1]).astimezone(timezone.utc)
        start_datetime = start_datetime + timedelta(seconds=1)
        
        #Moving back to string format to be inserted in the link for the API call
        start_datetime = start_datetime.strftime('%Y-%m-%dT%H:%M:%S.000Z')
        print("New Start DateTime(One Second added):"+ str(start_datetime))

        #Saving Current Date Time
        last_run_datetime_str = datetime.now().strftime("%Y-%m-%dT%H:%M:%S.000Z")
        
        page = 0

        #print('Moving 1 records from: ' + start_datetime)

        # replacing timeframe variables
        if start_datetime < datetime.now().strftime("%Y-%m-%dT%H:%M:%S.000Z"):
            api_urlv = link.replace("@Variable1", start_datetime)
            page = 1 #debug
        
        json_object = [1]

        my_error_flag = 0 #This flag is for control , if error happens inside loop, to avoid execute the merge.

        #New 2/21/2025: Adding the Merge to avoid Duplicates
        #Change the table_id to the stage version for calls, chats and emails                         
        if name == "calls" or name == "chats" or name == "emails" :
            table_id = table_id+'_stage'  

        while json_object != []:
            #print("Page:"+str(page))
            #adding the page number to the link
            api_url = api_urlv + "&page="+ str(page)            

            description= "CF04-"+str(page)+"-Calling the API: "+name+", Page:"+str(page)
            print(description)
            print("URL: "+api_url)

            api_name=name            

            #Make the request to the API
            try:
                response = requests.get(api_url,auth = HTTPBasicAuth(user, psw),timeout=(30, 60))
                #print(api_url)
                #01-22-2025  moved the Json Block response load from outside of the try to inside try except and added to capture the exceptions and set the json object to null.
                
                # your object
                json_object = response.json()
                print("JSON Object returned when calling the API:")
                print(json_object)

            except requests.exceptions.RequestException as e:
                print("CF04-"+str(page)+"-Exception: An exception occurred in API calling"+str(e))
                json_object=[]

            

            #checking the status of the request
            if response.status_code > 400:
                description= "CF04-"+str(page)+"-Exception: Error on calling the API: " + str(response.status_code)
                print(description)
                api_name=name
                
                # raise Exception("API call was not completed")
           
            
            #pandas dataframe
            df = pd.DataFrame(json_object) 
            count_row = df.shape[0]
            print('Number of Records Retrieved:'+str(count_row))
            
            if count_row == 0 or json_object == []:                
                json_object = []
                description= "CF04-"+str(page)+"-a-No new values to be inserted"
                print(description)
                api_name=name
                
            else:                
                #SergioC 11-27-23 Converting to DateTime Columns depending on de name of the table
                match name:
                    case "calls" :    #This is to convert the values to string since sometimes the api returns int sometimes string                        
                        df['dispositions'] = df['dispositions'].apply(lambda lst: [   
                            { **d,  # Keep all existing fields
                                'ujet_code_id': np.nan if not d.get('ujet_code_id') else d['ujet_code_id'],
                                'ujet_list_id': np.nan if not d.get('ujet_list_id') else d['ujet_list_id']
                             }
                                for d in lst] if lst else []  # Replace entire list with NaN if it's empty
                            )
                
                df.insert(0,"instance_id",instance_id)
                df.insert(1,"instance_name",instance_name)

                #debug
                #print('Data frame')
                #print(df)
                #/debug

                end_datetime = list(df[datetime_column].tail(1))

                last_record_end_datetime = end_datetime[0]

                print("Last Record End DateTime:"+str(last_record_end_datetime))
                print("Last Run DateTime:"+last_run_datetime_str)

                if last_record_end_datetime == last_run_datetime_str and count_row == 0 :
                    json_object = []
                    description= "CF04-"+str(page)+"-b-Only one duplicated record to insert, so we skip it"
                    print(description)
                    api_name=name
                
                else:
                    try:
                        print ("CF04-"+str(page)+"-b-Preparing to insert, Action: "+action)
                        # Set up a job configuration
                        job_config = bigquery.LoadJobConfig(autodetect=True) #autodetect=True
                        
                        #If explicit action is write_truncate the table will be recreated otherwise write_append adds records                        
                        if action == "write_truncate":
                            #If want to recreate the table use WRITE_TRUNCATE 
                            if page == 1:
                                job_config.write_disposition = bigquery.WriteDisposition.WRITE_TRUNCATE 
                            else:
                                job_config.write_disposition = bigquery.WriteDisposition.WRITE_APPEND
                            
                        else:
                            job_config.write_disposition = bigquery.WriteDisposition.WRITE_APPEND

                         
                        
                        # Submit the job
                        job = bigquery_client.load_table_from_dataframe(df, table_id, job_config=job_config)
                        
                        #waiting for the Job to finish
                        job.result()                       
                        
                        #print("CF04-"+str(page)+"-c-Status of inserting dataframe into stage historical table: "+job.state+" ,Rows Inserted:"+str(job.output_rows))
                        print(f'CF04-{str(page)}-c-Status of inserting dataframe into table {table_id} : {job.state} ,Rows Inserted:'+str(job.output_rows))

                        #DEBUG
                        #job = job.result()
                        #job = bigquery_client.get_job(job.job_id, location=job.location)
                        #print(f"{job.location}:{job.job_id}")
                        #print(f"Type: {job.job_type}")
                        #print(f"State: {job.state}")
                        #print(f"Created: {job.created.isoformat()}")
                        #/DEBUG
                        
                        api_name=name 

                    except Exception as error:
                        # Handling the exception
                        print(f"CF04-{page}-c-Exception: An exception occurred: {error} ")                        
                        client.report_exception() #Logging the error on Error Reporting
                        my_error_flag = 1 #set error flag to skip the merge
                        break #If error happens on any page, it should break the While
                        
                        #api_name=name
            if page == 30:
                description= 'Got into the limit of 30 000 records'
                print(description)
                json_object = []
            page += 1

        #New 2/21/2025: Adding the Merge to avoid Duplicates
        if my_error_flag != 1:
            #Execute the merge for calls, chats and emails
            if name == "calls" or name == "chats" or name == "emails" :
                print("CF05-Preparing to execute Merge")
                query = f"CALL {dataset}.p_merge_historical('{action}','{name}')"
                job_config = bigquery.QueryJobConfig()
                print(query)
                merge_job = bigquery_client.query(query, job_config=job_config)        
                #waiting for the Job to finish
                merge_job.result()                 
                print(f'CF05-a Status of Merging {name} into historical table: {merge_job.state}' )
            
            
    
    if my_error_flag != 1:
        print("************************************************         CF06-Ending instance of the function          ************************************************")                
    else:
        print("************************************************         CF06-Ending instance of the function with errors          ************************************************")
    return ("CF06-Ending instance of the function")
