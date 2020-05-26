from datetime import timedelta, datetime
import os
import yfinance as yf
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from sqlalchemy import create_engine, engine, table

mysqluser = os.environ.get('mysql_user')
mysqlkey = os.environ.get('mysql_key')

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
    dag_id='yahoo_house_vnq',
    default_args=default_args,
    description='yahoo request VNQ',
    schedule_interval=timedelta(days=1),
)

t0 = DummyOperator(
    task_id='yahoo_finance_start',
    retries=1,
    dag=dag
)


def get_vnq_yahoo():
    vnq_df = yf.download('VNQ',
                         start='2015-01-01',
                         # end='2020-04-24',
                         progress=False)
    infile = vnq_df.to_csv("/Users/mtessema/Desktop/VNQ/vnq_2015_present.csv", index=True)
    return infile


t1 = PythonOperator(
    task_id='get_vnq_yahoo',
    provide_context=False,
    python_callable=get_vnq_yahoo,
    dag=dag,
)


def vnq_to_sql():
    import pandas as pd
    df = pd.read_csv('/Users/mtessema/Desktop/VNQ/vnq_2015_present.csv', encoding='ISO-8859-1')
    engine = create_engine("mysql+pymysql://" + mysqluser + ":" + mysqlkey + "@localhost:3306/project_data")
    df.to_sql(name='vnq_5y', con=engine, index=False, if_exists='replace')


t2 = PythonOperator(
    task_id='vnq_to_sql',
    provide_context=False,
    python_callable=vnq_to_sql,
    dag=dag,
)


def change_data_type_vnq_5y():
    engine = create_engine("mysql+pymysql://" + mysqluser + ":" + mysqlkey + "@localhost:3306/project_data")
    conn = engine.connect()
    conn.execute("ALTER TABLE vnq_5y CHANGE Date Date DATE NULL;")


t3 = PythonOperator(
    task_id='change_data_type_vnq_5y',
    provide_context=False,
    python_callable=change_data_type_vnq_5y,
    dag=dag,
)


def chart_vnq_5y():
    import plotly.graph_objects as go
    import pandas as pd

    engine = create_engine("mysql+pymysql://" + mysqluser + ":" + mysqlkey + "@localhost:3306/project_data")
    dt5_df = pd.read_sql('select * from vnq_5y', engine)
    fig = go.Figure(data=[go.Candlestick(x=dt5_df['Date'],
                                         open=dt5_df['Open'],
                                         high=dt5_df['High'],
                                         low=dt5_df['Low'],
                                         close=dt5_df['Close'])])

    fig.show()
    # fig.savefig('5year.png')


t4 = PythonOperator(
    task_id='chart_vnq_5y',
    provide_context=False,
    python_callable=chart_vnq_5y,
    dag=dag,
)


def create_table_2015y():
    engine = create_engine("mysql+pymysql://" + mysqluser + ":" + mysqlkey + "@localhost:3306/project_data")
    conn = engine.connect()
    conn.execute("""
                drop table if exists y2015;
                """)
    conn.execute("""
                create table y2015 as 
                SELECT * from vnq_5y
                Where year(Date) = '2015';
                """)


t5 = PythonOperator(
    task_id='create_table_2015y',
    provide_context=False,
    python_callable=create_table_2015y,
    dag=dag,
)


def chart_table_2015y():
    import plotly.graph_objects as go
    import pandas as pd

    engine = create_engine("mysql+pymysql://" + mysqluser + ":" + mysqlkey + "@localhost:3306/project_data")
    dt2015_df = pd.read_sql('select * from y2015', engine)
    fig = go.Figure(data=[go.Candlestick(x=dt2015_df['Date'],
                                         open=dt2015_df['Open'],
                                         high=dt2015_df['High'],
                                         low=dt2015_df['Low'],
                                         close=dt2015_df['Close'])])

    fig.show()


t6 = PythonOperator(
    task_id='chart_table_2015y',
    provide_context=False,
    python_callable=chart_table_2015y,
    dag=dag,
)


def create_table_2016y():
    engine = create_engine("mysql+pymysql://" + mysqluser + ":" + mysqlkey + "@localhost:3306/project_data")
    conn = engine.connect()
    conn.execute("""
                drop table if exists y2016;
                """)
    conn.execute("""
                create table y2016 as 
                SELECT * from vnq_5y
                Where year(Date) = '2016';
    
    """)


t7 = PythonOperator(
    task_id='create_table_2016y',
    provide_context=False,
    python_callable=create_table_2016y,
    dag=dag,
)


def chart_table_2016y():
    import plotly.graph_objects as go
    import pandas as pd

    engine = create_engine("mysql+pymysql://" + mysqluser + ":" + mysqlkey + "@localhost:3306/project_data")
    dt2016_df = pd.read_sql('select * from y2016', engine)
    fig = go.Figure(data=[go.Candlestick(x=dt2016_df['Date'],
                                         open=dt2016_df['Open'],
                                         high=dt2016_df['High'],
                                         low=dt2016_df['Low'],
                                         close=dt2016_df['Close'])])
    fig.show()


t8 = PythonOperator(
    task_id='chart_table_2016y',
    provide_context=False,
    python_callable=chart_table_2016y,
    dag=dag,
)


def create_table_2017y():
    engine = create_engine("mysql+pymysql://" + mysqluser + ":" + mysqlkey + "@localhost:3306/project_data")
    conn = engine.connect()
    conn.execute("""
                drop table if exists y2017;
                """)
    conn.execute("""
                create table y2017 as 
                SELECT * from vnq_5y
                Where year(Date) = '2017';
    
    """)


t9 = PythonOperator(
    task_id='create_table_2017y',
    provide_context=False,
    python_callable=create_table_2017y,
    dag=dag,
)


def chart_table_2017y():
    import plotly.graph_objects as go
    import pandas as pd

    engine = create_engine("mysql+pymysql://" + mysqluser + ":" + mysqlkey + "@localhost:3306/project_data")
    dt2017_df = pd.read_sql('select * from y2017', engine)
    fig = go.Figure(data=[go.Candlestick(x=dt2017_df['Date'],
                                         open=dt2017_df['Open'],
                                         high=dt2017_df['High'],
                                         low=dt2017_df['Low'],
                                         close=dt2017_df['Close'])])

    fig.show()


t10 = PythonOperator(
    task_id='chart_table_2017y',
    provide_context=False,
    python_callable=chart_table_2017y,
    dag=dag,
)


def create_table_2018y():
    engine = create_engine("mysql+pymysql://" + mysqluser + ":" + mysqlkey + "@localhost:3306/project_data")
    conn = engine.connect()
    conn.execute("""
                drop table if exists y2018;
                """)
    conn.execute("""
                create table y2018 as 
                SELECT * from vnq_5y
                Where year(Date) = '2018';
    
    """)


t11 = PythonOperator(
    task_id='create_table_2018y',
    provide_context=False,
    python_callable=create_table_2018y,
    dag=dag,
)


def chart_table_2018y():
    import plotly.graph_objects as go
    import pandas as pd

    engine = create_engine("mysql+pymysql://" + mysqluser + ":" + mysqlkey + "@localhost:3306/project_data")
    dt2018_df = pd.read_sql('select * from y2018', engine)
    fig = go.Figure(data=[go.Candlestick(x=dt2018_df['Date'],
                                         open=dt2018_df['Open'],
                                         high=dt2018_df['High'],
                                         low=dt2018_df['Low'],
                                         close=dt2018_df['Close'])])
    fig.show()


t12 = PythonOperator(
    task_id='chart_table_2018y',
    provide_context=False,
    python_callable=chart_table_2018y,
    dag=dag,
)


def create_table_2019y():
    engine = create_engine("mysql+pymysql://" + mysqluser + ":" + mysqlkey + "@localhost:3306/project_data")
    conn = engine.connect()
    conn.execute("""
                drop table if exists y2019;"""
                 )
    conn.execute("""
                create table y2019 as 
                SELECT * from vnq_5y
                Where year(Date) = '2019';
    
    """)


t13 = PythonOperator(
    task_id='create_table_2019y',
    provide_context=False,
    python_callable=create_table_2019y,
    dag=dag,
)


def chart_table_2019y():
    import plotly.graph_objects as go
    import pandas as pd

    engine = create_engine("mysql+pymysql://" + mysqluser + ":" + mysqlkey + "@localhost:3306/project_data")
    dt2019_df = pd.read_sql('select * from y2019', engine)
    fig = go.Figure(data=[go.Candlestick(x=dt2019_df['Date'],
                                         open=dt2019_df['Open'],
                                         high=dt2019_df['High'],
                                         low=dt2019_df['Low'],
                                         close=dt2019_df['Close'])])

    fig.show()


t14 = PythonOperator(
    task_id='chart_table_2019y',
    provide_context=False,
    python_callable=chart_table_2019y,
    dag=dag,
)


def create_table_2020y():
    engine = create_engine("mysql+pymysql://" + mysqluser + ":" + mysqlkey + "@localhost:3306/project_data")
    conn = engine.connect()
    conn.execute("""
                drop table if exists y2020 ;"""
                 )
    conn.execute("""
                create table y2020 as 
                SELECT * from vnq_5y
                Where year(Date) = '2020';
    
    """)


t15 = PythonOperator(
    task_id='create_table_2020y',
    provide_context=False,
    python_callable=create_table_2020y,
    dag=dag,
)


def chart_table_2020y():
    import plotly.graph_objects as go
    import pandas as pd

    engine = create_engine("mysql+pymysql://" + mysqluser + ":" + mysqlkey + "@localhost:3306/project_data")
    dt2020_df = pd.read_sql('select * from y2020', engine)
    fig = go.Figure(data=[go.Candlestick(x=dt2020_df['Date'],
                                         open=dt2020_df['Open'],
                                         high=dt2020_df['High'],
                                         low=dt2020_df['Low'],
                                         close=dt2020_df['Close'])])
    fig.show()


t16 = PythonOperator(
    task_id='chart_table_2020y',
    provide_context=False,
    python_callable=chart_table_2020y,
    dag=dag,
)

t_z = DummyOperator(
    task_id='yahoo_finance_done',
    retries=1,
    dag=dag

)

t0 >> t1 >> t2 >> t3 >> t4
t5.set_upstream(t3)
t6.set_upstream(t5)
t7.set_upstream(t3)
t8.set_upstream(t7)
t9.set_upstream(t3)
t10.set_upstream(t9)
t11.set_upstream(t3)
t12.set_upstream(t11)
t13.set_upstream(t3)
t14.set_upstream(t13)
t15.set_upstream(t3)
t16.set_upstream(t15)
t_z.set_upstream(t16)
t_z.set_upstream(t14)
t_z.set_upstream(t12)
t_z.set_upstream(t10)
t_z.set_upstream(t8)
t_z.set_upstream(t6)
t_z.set_upstream(t4)
