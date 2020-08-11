from airflow import DAG
from airflow.operators import PythonOperator
from datetime import datetime
import psycopg2
import requests


dag_second_assignment = DAG(
	dag_id = 'second_assignment',
	start_date = datetime(2020,8,11), # 적당히 조절
	schedule_interval = '20 23 * * *')  # 적당히 조절

# Redshift connection 함수
def get_Redshift_connection():
    host = "grepp-data.cduaw970ssvt.ap-northeast-2.redshift.amazonaws.com"
    redshift_user = "nublu1234"
    redshift_pass = "Nublu12341!"
    port = 5439
    dbname = "dev"
    conn = psycopg2.connect("dbname={dbname} user={user} host={host} password={password} port={port}".format(
        dbname=dbname,
        user=redshift_user,
        password=redshift_pass,
        host=host,
        port=port
        ))
    conn.set_session(autocommit=True)
    return conn.cursor()


def etl():
    def extract(url):
        f = requests.get(link)
        return (f.text)

    def transform(text):
        lines = text.split('\n')
        return lines

    def load(data):
        cur = get_Redshift_connection()
        sql_truncate = "TRUNCATE TABLE nublu1234.name_gender;"
        cur.execute(sql_truncate)
        sql_begin_transaction = "BEGIN;"
        print(sql_begin_transaction)
        cur.execute(sql_begin_transaction)
        for r in data:
            if r != '':
                (name, gender) = r.split(",")
                # print(name, "-", gender)                                      
                sql = "INSERT INTO nublu1234.name_gender VALUES ('{name}', '{gender}');".format(name=name, gender=gender)
                # print(sql)
                cur.execute(sql)                                                
                sql_commit = "END;"
                print(sql_commit) 
                cur.execute(sql_commit)
     
    link = "https://s3-geospatial.s3-us-west-2.amazonaws.com/name_gender.csv" 
              
    return load(transform(extract(link))[1:])
   


task = PythonOperator(
	task_id = 'perform_etl',
	python_callable = etl,
	dag = dag_second_assignment)
  
# task가 하나 밖에 없는 경우 아무 것도 하지 않아도 그냥 실행됨
