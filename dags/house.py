from datetime import timedelta, datetime
import psycopg2
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from sqlalchemy import create_engine
import pandas as pd

default_args = {
    'owner': 'maleda',
    'depends_on_past': False,
    'start_date': datetime.now(),
    'email': ['mal.bt27@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

dag = DAG(
    dag_id='house_data1',
    default_args=default_args,
    description='create and insert',
    schedule_interval=timedelta(days=1),
)


def house_19128_sql():
    df = pd.read_csv('/Users/mtessema/Desktop/house/19128.csv', encoding='ISO-8859-1')
    engine = create_engine('mysql+pymysql://root:zipcoder@localhost:3306/project_data')
    df.to_sql(name='house_19128', con=engine, index=False, if_exists='replace')


t1 = PythonOperator(
    task_id='house_19128_sql',
    provide_context=False,
    python_callable=house_19128_sql,
    dag=dag,
)


def house_19129_sql():
    df = pd.read_csv('/Users/mtessema/Desktop/house/19129.csv', encoding='ISO-8859-1')
    engine = create_engine('mysql+pymysql://root:zipcoder@localhost:3306/project_data')
    df.to_sql(name='house_19129', con=engine, index=False, if_exists='replace')


t2 = PythonOperator(
    task_id='house_19129_sql',
    provide_context=False,
    python_callable=house_19129_sql,
    dag=dag,
)


def house_19130_sql():
    df = pd.read_csv('/Users/mtessema/Desktop/house/19130.csv', encoding='ISO-8859-1')
    engine = create_engine('mysql+pymysql://root:zipcoder@localhost:3306/project_data')
    df.to_sql(name='house_19130', con=engine, index=False, if_exists='replace')


t3 = PythonOperator(
    task_id='house_19130_sql',
    provide_context=False,
    python_callable=house_19130_sql,
    dag=dag,
)


def house_19131_sql():
    df = pd.read_csv('/Users/mtessema/Desktop/house/19131.csv', encoding='ISO-8859-1')
    engine = create_engine('mysql+pymysql://root:zipcoder@localhost:3306/project_data')
    df.to_sql(name='house_19131', con=engine, index=False, if_exists='replace')


t4 = PythonOperator(
    task_id='house_19131_sql',
    provide_context=False,
    python_callable=house_19131_sql,
    dag=dag,
)


def house_19132_sql():
    df = pd.read_csv('/Users/mtessema/Desktop/house/19132.csv', encoding='ISO-8859-1')
    engine = create_engine('mysql+pymysql://root:zipcoder@localhost:3306/project_data')
    df.to_sql(name='house_19132', con=engine, index=False, if_exists='replace')


t5 = PythonOperator(
    task_id='house_19132_sql',
    provide_context=False,
    python_callable=house_19132_sql,
    dag=dag,
)


def house_19133_sql():
    df = pd.read_csv('/Users/mtessema/Desktop/house/19133.csv', encoding='ISO-8859-1')
    engine = create_engine('mysql+pymysql://root:zipcoder@localhost:3306/project_data')
    df.to_sql(name='house_19133', con=engine, index=False, if_exists='replace')


t6 = PythonOperator(
    task_id='house_19133_sql',
    provide_context=False,
    python_callable=house_19133_sql,
    dag=dag,
  
)

def house_19134_sql():
    df = pd.read_csv('/Users/mtessema/Desktop/house/19134.csv', encoding='ISO-8859-1')
    engine = create_engine('mysql+pymysql://root:zipcoder@localhost:3306/project_data')
    df.to_sql(name='house_19134', con=engine, index=False, if_exists='replace')


t7 = PythonOperator(
    task_id='house_19134_sql',
    provide_context=False,
    python_callable=house_19134_sql,
    dag=dag,
)


def house_19135_sql():
    df = pd.read_csv('/Users/mtessema/Desktop/house/19135.csv', encoding='ISO-8859-1')
    engine = create_engine('mysql+pymysql://root:zipcoder@localhost:3306/project_data')
    df.to_sql(name='house_19135', con=engine, index=False, if_exists='replace')


t8 = PythonOperator(
    task_id='house_19135_sql',
    provide_context=False,
    python_callable=house_19135_sql,
    dag=dag,
)


def house_19136_sql():
    df = pd.read_csv('/Users/mtessema/Desktop/house/19136.csv', encoding='ISO-8859-1')
    engine = create_engine('mysql+pymysql://root:zipcoder@localhost:3306/project_data')
    df.to_sql(name='house_19136', con=engine, index=False, if_exists='replace')


t9 = PythonOperator(
    task_id='house_19136_sql',
    provide_context=False,
    python_callable=house_19136_sql,
    dag=dag,
)


def house_19137_sql():
    df = pd.read_csv('/Users/mtessema/Desktop/house/19137.csv', encoding='ISO-8859-1')
    engine = create_engine('mysql+pymysql://root:zipcoder@localhost:3306/project_data')
    df.to_sql(name='house_19137', con=engine, index=False, if_exists='replace')


t10 = PythonOperator(
    task_id='house_19137_sql',
    provide_context=False,
    python_callable=house_19137_sql,
    dag=dag,
)


def house_19138_sql():
    df = pd.read_csv('/Users/mtessema/Desktop/house/19138.csv', encoding='ISO-8859-1')
    engine = create_engine('mysql+pymysql://root:zipcoder@localhost:3306/project_data')
    df.to_sql(name='house_19138', con=engine, index=False, if_exists='replace')


t11 = PythonOperator(
    task_id='house_19138_sql',
    provide_context=False,
    python_callable=house_19138_sql,
    dag=dag,
)


def house_19139_sql():
    df = pd.read_csv('/Users/mtessema/Desktop/house/19139.csv', encoding='ISO-8859-1')
    engine = create_engine('mysql+pymysql://root:zipcoder@localhost:3306/project_data')
    df.to_sql(name='house_19139', con=engine, index=False, if_exists='replace')


t12 = PythonOperator(
    task_id='house_19139_sql',
    provide_context=False,
    python_callable=house_19139_sql,
    dag=dag,
)


def house_19140_sql():
    df = pd.read_csv('/Users/mtessema/Desktop/house/19140.csv', encoding='ISO-8859-1')
    engine = create_engine('mysql+pymysql://root:zipcoder@localhost:3306/project_data')
    df.to_sql(name='house_19140', con=engine, index=False, if_exists='replace')


t13 = PythonOperator(
    task_id='house_19140_sql',
    provide_context=False,
    python_callable=house_19140_sql,
    dag=dag,
)


def house_19141_sql():
    df = pd.read_csv('/Users/mtessema/Desktop/house/19141.csv', encoding='ISO-8859-1')
    engine = create_engine('mysql+pymysql://root:zipcoder@localhost:3306/project_data')
    df.to_sql(name='house_19141', con=engine, index=False, if_exists='replace')


t14 = PythonOperator(
    task_id='house_19141_sql',
    provide_context=False,
    python_callable=house_19141_sql,
    dag=dag,
)


def house_19142_sql():
    df = pd.read_csv('/Users/mtessema/Desktop/house/19142.csv', encoding='ISO-8859-1')
    engine = create_engine('mysql+pymysql://root:zipcoder@localhost:3306/project_data')
    df.to_sql(name='house_19142', con=engine, index=False, if_exists='replace')


t15 = PythonOperator(
    task_id='house_19142_sql',
    provide_context=False,
    python_callable=house_19142_sql,
    dag=dag,
)


def house_19143_sql():
    df = pd.read_csv('/Users/mtessema/Desktop/house/19143.csv', encoding='ISO-8859-1')
    engine = create_engine('mysql+pymysql://root:zipcoder@localhost:3306/project_data')
    df.to_sql(name='house_19143', con=engine, index=False, if_exists='replace')


t16 = PythonOperator(
    task_id='house_19143_sql',
    provide_context=False,
    python_callable=house_19143_sql,
    dag=dag,
)


def house_19144_sql():
    df = pd.read_csv('/Users/mtessema/Desktop/house/19144.csv', encoding='ISO-8859-1')
    engine = create_engine('mysql+pymysql://root:zipcoder@localhost:3306/project_data')
    df.to_sql(name='house_19144', con=engine, index=False, if_exists='replace')


t17 = PythonOperator(
    task_id='house_19144_sql',
    provide_context=False,
    python_callable=house_19144_sql,
    dag=dag,
)


def house_19145_sql():
    df = pd.read_csv('/Users/mtessema/Desktop/house/19145.csv', encoding='ISO-8859-1')
    engine = create_engine('mysql+pymysql://root:zipcoder@localhost:3306/project_data')
    df.to_sql(name='house_19145', con=engine, index=False, if_exists='replace')


t18 = PythonOperator(
    task_id='house_19145_sql',
    provide_context=False,
    python_callable=house_19145_sql,
    dag=dag,
)


def house_19146_sql():
    df = pd.read_csv('/Users/mtessema/Desktop/house/19146.csv', encoding='ISO-8859-1')
    engine = create_engine('mysql+pymysql://root:zipcoder@localhost:3306/project_data')
    df.to_sql(name='house_19146', con=engine, index=False, if_exists='replace')


t18 = PythonOperator(
    task_id='house_19146_sql',
    provide_context=False,
    python_callable=house_19146_sql,
    dag=dag,
)


def house_19147_sql():
    df = pd.read_csv('/Users/mtessema/Desktop/house/19147.csv', encoding='ISO-8859-1')
    engine = create_engine('mysql+pymysql://root:zipcoder@localhost:3306/project_data')
    df.to_sql(name='house_19147', con=engine, index=False, if_exists='replace')


t19 = PythonOperator(
    task_id='house_19147_sql',
    provide_context=False,
    python_callable=house_19147_sql,
    dag=dag,
)


def house_19148_sql():
    df = pd.read_csv('/Users/mtessema/Desktop/house/19148.csv', encoding='ISO-8859-1')
    engine = create_engine('mysql+pymysql://root:zipcoder@localhost:3306/project_data')
    df.to_sql(name='house_19148', con=engine, index=False, if_exists='replace')


t20 = PythonOperator(
    task_id='house_19148_sql',
    provide_context=False,
    python_callable=house_19148_sql,
    dag=dag,
)


def house_19149_sql():
    df = pd.read_csv('/Users/mtessema/Desktop/house/19149.csv', encoding='ISO-8859-1')
    engine = create_engine('mysql+pymysql://root:zipcoder@localhost:3306/project_data')
    df.to_sql(name='house_19149', con=engine, index=False, if_exists='replace')


t21 = PythonOperator(
    task_id='house_19149_sql',
    provide_context=False,
    python_callable=house_19149_sql,
    dag=dag,
)


def house_19150_sql():
    df = pd.read_csv('/Users/mtessema/Desktop/house/19150.csv', encoding='ISO-8859-1')
    engine = create_engine('mysql+pymysql://root:zipcoder@localhost:3306/project_data')
    df.to_sql(name='house_19150', con=engine, index=False, if_exists='replace')


t22 = PythonOperator(
    task_id='house_19150_sql',
    provide_context=False,
    python_callable=house_19150_sql,
    dag=dag,
)


def house_19151_sql():
    df = pd.read_csv('/Users/mtessema/Desktop/house/19151.csv', encoding='ISO-8859-1')
    engine = create_engine('mysql+pymysql://root:zipcoder@localhost:3306/project_data')
    df.to_sql(name='house_19151', con=engine, index=False, if_exists='replace')


t23 = PythonOperator(
    task_id='house_19151_sql',
    provide_context=False,
    python_callable=house_19151_sql,
    dag=dag,
)


def house_19152_sql():
    df = pd.read_csv('/Users/mtessema/Desktop/house/19152.csv', encoding='ISO-8859-1')
    engine = create_engine('mysql+pymysql://root:zipcoder@localhost:3306/project_data')
    df.to_sql(name='house_19152', con=engine, index=False, if_exists='replace')


t24 = PythonOperator(
    task_id='house_19152_sql',
    provide_context=False,
    python_callable=house_19152_sql,
    dag=dag,
)


def house_19153_sql():
    df = pd.read_csv('/Users/mtessema/Desktop/house/19153.csv', encoding='ISO-8859-1')
    engine = create_engine('mysql+pymysql://root:zipcoder@localhost:3306/project_data')
    df.to_sql(name='house_19153', con=engine, index=False, if_exists='replace')


t25 = PythonOperator(
    task_id='house_19153_sql',
    provide_context=False,
    python_callable=house_19153_sql,
    dag=dag,
)


def house_19154_sql():
    df = pd.read_csv('/Users/mtessema/Desktop/house/19154.csv', encoding='ISO-8859-1')
    engine = create_engine('mysql+pymysql://root:zipcoder@localhost:3306/project_data')
    df.to_sql(name='house_19154', con=engine, index=False, if_exists='replace')


t62 = PythonOperator(
    task_id='house_19154_sql',
    provide_context=False,
    python_callable=house_19154_sql,
    dag=dag,
)

import os
import glob
import pandas as pd
import numpy as np

def all_atonce():

    path = "/Users/mtessema/Desktop/house/"
    allFiles = glob.glob(os.path.join(path,"*.csv"))


    np_array_list = []
    for file_ in allFiles:
        df = pd.read_csv(file_,index_col=None, header=0)
        np_array_list.append(df.as_matrix())

    comb_np_array = np.vstack(np_array_list)
    big_frame = pd.DataFrame(comb_np_array)

    big_frame.columns = ["ZIP Code","Year","Month", "SalesCount", "AvgSalesPrice"]
    engine = create_engine('mysql+pymysql://root:zipcoder@localhost:3306/project_data')
    big_frame.to_sql(name='house_pa', con=engine, index=False, if_exists='replace')

t63 = PythonOperator(
    task_id='all_atonce',
    provide_context=False,
    python_callable=all_atonce,
    dag=dag,
)