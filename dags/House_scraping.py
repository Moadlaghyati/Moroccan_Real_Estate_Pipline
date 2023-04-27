import requests 
from bs4 import BeautifulSoup
import pandas as pd
from datetime import timedelta  


import os
import boto3
import psycopg2

from datetime import date

from airflow import DAG
# We need to import the operators used in our tasks
from airflow.operators.python_operator import PythonOperator
# We then import the days_ago function
from airflow.utils.dates import days_ago 





def ScrapeData():
    All_Titles = []
    All_Prices = []
    All_Adresses = []
    All_chambers = []

    today = date.today()
    scrapedate = today.strftime("%m-%d-%y")
    page = 1
    while page!= 5:
        url = f"https://www.mubawab.ma/fr/sc/appartements-a-vendre:p:{page}"
        r = requests.get(url)
        s = BeautifulSoup(r.content , 'html.parser')


        Containers = s.find_all('li', {'class':'listingBox w100'})
        #print(Containers.find_all('span' , {'class':'priceTag '}))
            
        Titles = [title.find_all('a')[0].text for title in Containers] 
        All_Titles.append(clean(Titles))

        #getting Prices from mubawab 

        Price = [container.find_all('span' , {'class':'priceTag'})[0].text.replace("\t", "") for container in Containers] 
        All_Prices.append(clean(Price))

        #Extracting Appartement Adresse 

        Adresse = [Adr.find_all('h3')[0].text for Adr in Containers] 
        All_Adresses.append(clean(Adresse))

        #Getting number of chambers and space

        Chamber = [cham.find_all('p')[0].text for cham in Containers] 
        All_chambers.append(clean(Chamber))

        page +=1

    All_Titles = [item for sublist in All_Titles for item in sublist]
    All_Prices = [item for sublist in All_Prices for item in sublist]
    All_chambers = [item for sublist in All_chambers for item in sublist]
    All_Adresses = [item for sublist in All_Adresses for item in sublist]

    df = pd.DataFrame({'Titles' : All_Titles  , 'Adresses' : All_Adresses ,'Chambers' : All_chambers , 'Prices' : All_Prices})
    print(df)
    df['Titles'] = df['Titles'].str.replace(',','.')
    df['Chambers'] = df['Chambers'].str.replace(',','.')
    df['Adresses'] = df['Adresses'].str.replace(',','.')
    
    df.to_csv(dag_path+'/raw_data/' + scrapedate + '-Appartements.csv' , header=None,index= True)


#Cleanig Extracted Data 

d = { "\n": "", "\t" : "" , "..." : "" , "  ":"" ,"," : ""}

def replace_all(text, dic):
    for i, j in dic.items():
        text = text.replace(i, j)
    return text

def clean(list):
    new_list = []
    for L in list:
        L = L.replace(u'\xa0', '')
        new_list.append(replace_all(L,d))
    return new_list

# get dag directory path
dag_path = os.getcwd()


#uploading the data into s3 bucket
def load_s3_data():
    today = date.today()
    Sdate = today.strftime("%m-%d-%y")
    session = boto3.Session(
    aws_access_key_id="",
    aws_secret_access_key="",)
    s3 = session.resource('s3')
    
    s3.meta.client.upload_file(dag_path + '/raw_data/'  + Sdate +'-Appartements.csv','appartementscraping', Sdate +'-Appartements.csv')
    print("Data Loaded successfully")
    

def download_s3_data():
    
        s3 = boto3.client('s3',aws_access_key_id="",
        aws_secret_access_key="")

        get_last_modified = lambda obj: int(obj['LastModified'].strftime('%S'))

        objs = s3.list_objects_v2(Bucket='appartementscraping')['Contents']
        last_added = [obj['Key'] for obj in sorted(objs, key=get_last_modified)][-1]

        session = boto3.Session(
        aws_access_key_id="",
        aws_secret_access_key="",)
        s3 = session.resource('s3')
    
        s3.Bucket('appartementscraping').download_file(last_added , dag_path + '/processed_data/' + last_added)
        print("Data downlaoded successfully")


def load_data():
    #establishing the connection
    conn = psycopg2.connect(
        database="App_WebScraping", user='airflow', password='airflow', host='postgres' , port = '5432'
        )
    cursor = conn.cursor()
    cursor.execute("select version()")

    data = cursor.fetchone()
    print("Connection established to: ", data)

    s3 = boto3.client('s3',aws_access_key_id="",
    aws_secret_access_key="")

    get_last_modified = lambda obj: int(obj['LastModified'].strftime('%S'))
        
    objs = s3.list_objects_v2(Bucket='appartementscraping')['Contents']
    last_added = [obj['Key'] for obj in sorted(objs, key=get_last_modified)][-1]
    
    command = ("""  
        
        CREATE TABLE IF NOT EXISTS WebScraped_Appartemets2 (
            Appt_id INTEGER ,
            Title VARCHAR(50000),
            Adresses VARCHAR(50000),
            Room_space VARCHAR(10000),
            Price VARCHAR(1000)         
        );   """
        )
        
    print(command)
    cursor.execute(command)

    #print(last_added)
    f = open(dag_path+'/processed_data/' + last_added , 'r', encoding='utf8')

    try:
        cursor.copy_from(f, 'webscraped_appartemets2', sep= ",")
        conn.commit()
        print("Data inserted using copy_from_datafile() successfully....")
    except (Exception, psycopg2.DatabaseError) as err:
        #os.remove(dag_path+'/processed_data/'+last_added)
            # pass exception to function
        print(psycopg2.DatabaseError)
        print(Exception)
        print(err)
        cursor.close()
    conn.commit()
    conn.close()





# initializing the default arguments that we'll pass to our DAG
default_args = {
    'owner': 'airflow',
    'start_date': days_ago(5)
}

ingestion_dag = DAG(
    'AppScraping_dag',
    default_args=default_args,
    description='Aggregates booking records for data analysis',
    schedule_interval=timedelta(minutes=1),
    catchup=False
)

task_1 = PythonOperator(
    task_id='ScrapeData',
    python_callable=ScrapeData,
    dag=ingestion_dag,
)

task_2 = PythonOperator(
    task_id='load_s3_data',
    python_callable=load_s3_data,
    dag=ingestion_dag,
)

task_3 = PythonOperator(
    task_id='download_s3_data',
    python_callable=download_s3_data,
    dag=ingestion_dag,
)


task_4 = PythonOperator(
    task_id='load_data',
    python_callable=load_data,
    dag=ingestion_dag,
)

#task_3
task_1 >> task_2 >> task_3 >> task_4  
