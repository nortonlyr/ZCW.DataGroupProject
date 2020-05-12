from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from datetime import timedelta
import pandas as pd
from airflow import DAG
from sqlalchemy import create_engine
import requests
from datetime import datetime
import os


mysqluser = os.environ.get('mysql_user')
mysqlkey = os.environ.get('mysql_key')


default_args = {
    'owner': 'Norton_Li',
    'start_date': datetime.now(),
    #'start_date': datetime.days_ago(2),
    'email': ['nortonlyr@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1)
}


dag = DAG(
    dag_id='philadelphia_housing_data_study_v2',
    default_args=default_args,
    description='Philadelphia Housing Data Study',
    )


t0 = DummyOperator(
    task_id='phi_data_etl_start',
    retries=1,
    dag=dag
)

###########################################
#t1: Philly Park & Recreaction

path_phi_pr = os.path.join(os.path.dirname(os.path.dirname(__file__)),'data/phi_parks_recreations.csv')
path_etl_phi_pr = os.path.join(os.path.dirname(os.path.dirname(__file__)),'data/phi_parks_recreations_2.csv')

def get_phi_pr():
    """
    get the Philadelphia parks and recreations data
    """
    url = 'http://data-phl.opendata.arcgis.com/datasets/d52445160ab14380a673e5849203eb64_0.csv'
    response = requests.get(url)
    with open(path_phi_pr, 'wb') as f:
        f.write(response.content)


t1a = PythonOperator(
    task_id='get_phi_parks_recreations',
    python_callable=get_phi_pr,
    provide_context=False,
    dag=dag,
)


def phi_pr_etl():
    """
    Preliminary data cleaning from parks and recreations file and drop some unused columns
    """
    phi_parks_recreations_df = pd.read_csv(path_phi_pr)
    phi_parks_recreations_df_2 = phi_parks_recreations_df.drop(columns=['PARENT_NAME', 'NESTED', 'OFFICIAL_NAME',
                                                                        'LABEL', 'ALIAS', 'DPP_ASSET_ID',
                                                                        'ALIAS_ADDRESS', 'PPR_OPS_DISTRICT', 'COMMENTS',
                                                                        'COUNCIL_DISTRICT', 'POLICE_DISTRICT',
                                                                        'CITY_SCALE_MAPS', 'LOCAL_SCALE_MAPS',
                                                                        'PROGRAM_SITES', 'ADDRESS_BRT', 'Shape__Area',
                                                                        'Shape__Length'])
    phi_parks_recreations_df_2 = phi_parks_recreations_df_2.set_index('OBJECTID')
    phi_parks_recreations_df_2.to_csv(path_etl_phi_pr)


t1b = PythonOperator(
    task_id='phi_parks_recreations_etl',
    python_callable=phi_pr_etl,
    provide_context=False,
    dag=dag,
)


def phi_pr_csv_to_mysql():
    conn = create_engine("mysql+pymysql://" + mysqluser + ":" + mysqlkey + "@localhost:3306/data_group_project")
    df = pd.read_csv(path_etl_phi_pr, delimiter=',')
    df.to_sql(name='parks_recreations', con=conn, schema='data_group_project', if_exists='replace')


t1c = PythonOperator(
        task_id='phi_parks_recreations_csv_to_mysql',
        python_callable=phi_pr_csv_to_mysql,
        dag=dag,
)


###############################################
#t2: Philly Police Departments

path_phi_pd = os.path.join(os.path.dirname(os.path.dirname(__file__)),'data/phi_police_departments.csv')
path_etl_phi_pd = os.path.join(os.path.dirname(os.path.dirname(__file__)),'data/phi_police_departments_2.csv')

def get_phi_pd():
    """
    get the Philadelphia police departments data
    """
    url = 'https://drive.google.com/u/0/uc?id=1RDOu9I2G7ht8Kp5R_qKgG3LxiB_GXvYD&export=download'
    response = requests.get(url)
    with open(path_phi_pd, 'wb') as f:
        f.write(response.content)


t2a = PythonOperator(
    task_id='get_phi_police_departments',
    python_callable=get_phi_pd,
    provide_context=False,
    dag=dag,
)


def phi_pd_etl():
    """
     Preliminary data cleaning from police departments file and drop some unused columns
    """
    phi_police_departments_df = pd.read_csv(path_phi_pd)

    phi_police_departments_df_2 = phi_police_departments_df.drop(columns=['TELEPHONE_NUMBER', 'RULEID'])
    phi_police_departments_df_2 = phi_police_departments_df_2.set_index('OBJECTID')
    phi_police_departments_df_2.to_csv(path_etl_phi_pd)


t2b = PythonOperator(
    task_id='phi_police_departments_etl',
    python_callable=phi_pd_etl,
    provide_context=False,
    dag=dag,
)


def phi_pd_csv_to_mysql():
    conn = create_engine("mysql+pymysql://" + mysqluser + ":" + mysqlkey + "@localhost:3306/data_group_project")
    df = pd.read_csv(path_etl_phi_pd, delimiter=',')
    df.to_sql(name='police_departments', con=conn, schema='data_group_project', if_exists='replace')


t2c = PythonOperator(
        task_id='phi_police_departments_csv_to_mysql',
        python_callable=phi_pd_csv_to_mysql,
        dag=dag,
)


###############################################
#t3: Philly Hospitals

path_phi_hp = os.path.join(os.path.dirname(os.path.dirname(__file__)),'data/phi_hospitals.csv')
path_etl_phi_hp = os.path.join(os.path.dirname(os.path.dirname(__file__)),'data/phi_hospitals_2.csv')

def get_phi_hp():
    """
    get the Philadelphia hospitals data
    """
    url = 'http://data-phl.opendata.arcgis.com/datasets/df8dc18412494e5abbb021e2f33057b2_0.csv'
    response = requests.get(url)
    with open(path_phi_hp, 'wb') as f:
        f.write(response.content)


t3a = PythonOperator(
    task_id='get_phi_hospitals',
    python_callable=get_phi_hp,
    provide_context=False,
    dag=dag,
)

def phi_hospitals_etl():
    """
    Preliminary data cleaning from hospitals file and drop some unused columns
    """
    phi_hospitals_df = pd.read_csv(path_phi_hp)

    phi_hospitals_df_2 = phi_hospitals_df.drop(columns=['PHONE_NUMBER','STATE'])
    phi_hospitals_df_2 = phi_hospitals_df_2.set_index('OBJECTID')
    phi_hospitals_df_2.to_csv(path_etl_phi_hp)


t3b = PythonOperator(
    task_id='phi_hospitals_etl',
    python_callable=phi_hospitals_etl,
    provide_context=False,
    dag=dag,
)


def phi_hp_csv_to_mysql():
    conn = create_engine("mysql+pymysql://" + mysqluser + ":" + mysqlkey + "@localhost:3306/data_group_project")
    df = pd.read_csv(path_etl_phi_hp, delimiter=',')
    df.to_sql(name='hospitals', con=conn, schema='data_group_project', if_exists='replace')


t3c = PythonOperator(
        task_id='phi_hospitals_to_mysql',
        python_callable=phi_hp_csv_to_mysql,
        dag=dag,
)


###############################################
#t4: Philly Schools

path_phi_schl = os.path.join(os.path.dirname(os.path.dirname(__file__)),'data/phi_schools.csv')
path_etl_phi_schl = os.path.join(os.path.dirname(os.path.dirname(__file__)),'data/phi_schools_2.csv')

def get_phi_schl():
    """
    get the Philadelphia school data
    """
    url = 'http://data.phl.opendata.arcgis.com/datasets/d46a7e59e2c246c891fbee778759717e_0.csv'
    response = requests.get(url)
    with open(path_phi_schl, 'wb') as f:
        f.write(response.content)


t4a = PythonOperator(
    task_id='get_phi_schools',
    python_callable=get_phi_schl,
    provide_context=False,
    dag=dag,
)


def phi_schl_etl():
    """
    Preliminary data cleaning from schools file and drop some unused columns
    """
    phi_schools_df = pd.read_csv(path_phi_schl)

    phi_schools_df_2 = phi_schools_df.drop( columns=['AUN', 'SCHOOL_NUM', 'LOCATION_ID', 'SCHOOL_NAME_LABEL',
                                                     'PHONE_NUMBER', 'ACTIVE', 'ENROLLMENT'])
    phi_schools_df_2 = phi_schools_df_2.set_index('OBJECTID')
    phi_schools_df_2 .to_csv(path_etl_phi_schl)


t4b = PythonOperator(
    task_id='phi_schools_etl',
    python_callable=phi_schl_etl,
    provide_context=False,
    dag=dag,
)


def phi_schl_csv_to_mysql():
    conn = create_engine("mysql+pymysql://" + mysqluser + ":" + mysqlkey + "@localhost:3306/data_group_project")
    df = pd.read_csv(path_etl_phi_schl, delimiter=',')
    df.to_sql(name='schools', con=conn, schema='data_group_project', if_exists='replace')


t4c = PythonOperator(
        task_id='phi_schools_csv_to_mysql',
        python_callable=phi_schl_csv_to_mysql,
        dag=dag,
)


###############################################
#t5: Philly crime rate 2019

path_phi_crime = os.path.join(os.path.dirname(os.path.dirname(__file__)),'data/phi_crime_rate_2019.csv')
path_etl_phi_crime = os.path.join(os.path.dirname(os.path.dirname(__file__)),'data/phi_crime_rate_2019_2.csv')


def get_phi_cr():
    """
    get the Philadelphia crime rate (2019) data
    """
    url = 'https://drive.google.com/u/0/uc?id=13sqTHNHBuZbPdz5b5kJ9ZqW3JqkGpytI&export=download'
    response = requests.get(url)
    with open(path_phi_crime, 'wb') as f:
        f.write(response.content)


t5a = PythonOperator(
    task_id='get_phi_crime',
    python_callable=get_phi_cr,
    provide_context=False,
    dag=dag,
)


def phi_cr_etl():
    """
    Preliminary data cleaning from crime (2019) file and drop some unused columns
    """
    phi_crime_rate_2019_df = pd.read_csv(path_phi_crime)

    phi_crime_rate_2019_df_2 = phi_crime_rate_2019_df.drop(columns=['dc_key', 'psa', 'dispatch_date_time',
                                                                    'dispatch_time','hour_', 'location_block',
                                                                    'ucr_general'])
    phi_crime_rate_2019_df_2 = phi_crime_rate_2019_df_2.set_index('objectid')
    phi_crime_rate_2019_df_2.to_csv(path_etl_phi_crime)


t5b = PythonOperator(
    task_id='phi_crime_etl',
    python_callable=phi_cr_etl,
    provide_context=False,
    dag=dag,
)


def phi_cr_csv_to_mysql():
    conn = create_engine("mysql+pymysql://" + mysqluser + ":" + mysqlkey + "@localhost:3306/data_group_project")
    df = pd.read_csv(path_etl_phi_crime, delimiter=',')
    df.to_sql(name='crimes', con=conn, schema='data_group_project', if_exists='replace')


t5c = PythonOperator(
        task_id='phi_crimes_to_mysql',
        python_callable=phi_cr_csv_to_mysql,
        dag=dag,
)


###############################################
#t6: Philly Fire Departments

path_phi_fd = os.path.join(os.path.dirname(os.path.dirname(__file__)),'data/phi_fire_departments.csv')
path_etl_phi_fd = os.path.join(os.path.dirname(os.path.dirname(__file__)),'data/phi_fire_departments_2.csv')

def get_phi_fd():
    """
    get the Philadelphia fire departments data
    """
    url = 'https://drive.google.com/u/0/uc?id=15PZpu6mFYkGDlKia1VUW5bfVzbIf6n0j&export=download'
    response = requests.get(url)
    with open(path_phi_fd, 'wb') as f:
        f.write(response.content)


t6a = PythonOperator(
    task_id='get_phi_fire_departments',
    python_callable=get_phi_fd,
    provide_context=False,
    dag=dag,
)


def phi_fd_etl():
    """
    Preliminary data cleaning from fire departments file and drop some unused columns
    """
    phi_fire_departments_df = pd.read_csv(path_phi_fd )

    phi_fire_departments_df_2 = phi_fire_departments_df.drop(columns=['FIRESTA_', 'ENG', 'LAD', 'BC', 'MED', 'SPCL',
                                                                      'SPCL2', 'SPCL3', 'ACTIVE'])
    phi_fire_departments_df_2 = phi_fire_departments_df_2.set_index('OBJECTID')
    phi_fire_departments_df_2.to_csv(path_etl_phi_fd)


t6b = PythonOperator(
    task_id='phi_fire_departments_etl',
    python_callable=phi_fd_etl,
    provide_context=False,
    dag=dag,
)


def phi_fd_csv_to_mysql():
    conn = create_engine("mysql+pymysql://" + mysqluser + ":" + mysqlkey + "@localhost:3306/data_group_project")
    df = pd.read_csv(path_etl_phi_fd, delimiter=',')
    df.to_sql(name='fire_departments', con=conn, schema='data_group_project', if_exists='replace')


t6c = PythonOperator(
        task_id='phi_fire_departments_csv_to_mysql',
        python_callable=phi_fd_csv_to_mysql,
        dag=dag,
)


###############################################
#t7: Philly Health Centers

path_phi_hc = os.path.join(os.path.dirname(os.path.dirname(__file__)),'data/phi_health_centers.csv')
path_etl_phi_hc = os.path.join(os.path.dirname(os.path.dirname(__file__)),'data/phi_health_centers_2.csv')

def get_phi_hc():
    """
    get the Philadelphia health centers data
    """
    url = 'https://drive.google.com/u/0/uc?id=10dj1gTM8jew83sqNYKtgdhnmkI_qK_Gn&export=download'
    response = requests.get(url)
    with open(path_phi_hc, 'wb') as f:
        f.write(response.content)


t7a = PythonOperator(
    task_id='get_phi_health_centers',
    python_callable=get_phi_hc,
    provide_context=False,
    dag=dag,
)


def phi_hc_etl():
    """
    Preliminary data cleaning from health centers file and drop some unused columns
    """
    phi_health_centers_df = pd.read_csv(path_phi_hc)

    phi_health_centers_df_2 = phi_health_centers_df.drop(columns=['PHONE', 'WEBSITE', 'DENTAL_PHONE'])
    phi_health_centers_df_2 = phi_health_centers_df_2.set_index('OBJECTID')
    phi_health_centers_df_2 .to_csv(path_etl_phi_hc)


t7b = PythonOperator(
    task_id='phi_health_centers_etl',
    python_callable=phi_hc_etl,
    provide_context=False,
    dag=dag,
)


def phi_hc_csv_to_mysql():
    conn = create_engine("mysql+pymysql://" + mysqluser + ":" + mysqlkey + "@localhost:3306/data_group_project")
    df = pd.read_csv(path_etl_phi_hc, delimiter=',')
    df.to_sql(name='health_centers', con=conn, schema='data_group_project', if_exists='replace')


t7c = PythonOperator(
        task_id='phi_health_centers_csv_to_mysql',
        python_callable=phi_hc_csv_to_mysql,
        dag=dag,
)


###############################################
#t8: Philly Healthy Corner Stores

path_phi_hcs = os.path.join(os.path.dirname(os.path.dirname(__file__)),'data/phi_healthy_corner_stores.csv')
path_etl_phi_hcs = os.path.join(os.path.dirname(os.path.dirname(__file__)),'data/phi_healthy_corner_stores_2.csv')

def get_phi_hcs():
    """
    get the Philadelphia healthy corner stores data
    """
    url = 'https://drive.google.com/u/0/uc?id=1Vy0ueSOesxsMRp0LIqvQ9A2jl19HSG6Q&export=download'
    response = requests.get(url)
    with open(path_phi_hcs, 'wb') as f:
        f.write(response.content)


t8a = PythonOperator(
    task_id='get_phi_healthy_corner_stores',
    python_callable=get_phi_hcs,
    provide_context=False,
    dag=dag,
)


def phi_hcs_etl():
    """
    Preliminary data cleaning from healthy corner stores file and drop some unused columns
    """
    phi_healthy_corner_stores_df = pd.read_csv(path_phi_hcs)

    phi_healthy_corner_stores_df_2 = phi_healthy_corner_stores_df.drop(columns=['OFFICIAL_STORE_NAME'])
    phi_healthy_corner_stores_df_2 = phi_healthy_corner_stores_df_2.set_index('OBJECTID')
    phi_healthy_corner_stores_df_2 .to_csv(path_etl_phi_hcs)


t8b = PythonOperator(
    task_id='phi_healthy_corner_stores_etl',
    python_callable=phi_hcs_etl,
    provide_context=False,
    dag=dag,
)


def phi_hcs_csv_to_mysql():
    conn = create_engine("mysql+pymysql://" + mysqluser + ":" + mysqlkey + "@localhost:3306/data_group_project")
    df = pd.read_csv(path_etl_phi_hcs, delimiter=',')
    df.to_sql(name='healthy_corner_stores', con=conn, schema='data_group_project', if_exists='replace')


t8c = PythonOperator(
        task_id='phi_healthy_corner_stores_csv_to_mysql',
        python_callable=phi_hcs_csv_to_mysql,
        dag=dag,
)


###############################################
#t9: Philly OPA Properties Public Data

path_phi_pp = os.path.join(os.path.dirname(os.path.dirname(__file__)),'data/phi_opa_properties_public.csv')
path_etl_phi_pp = os.path.join(os.path.dirname(os.path.dirname(__file__)),'data/phi_opa_properties_public_2.csv')

def get_phi_pp():
    """
    get the Philadelphia opa properties public data
    """
    url = 'https://phl.carto.com/api/v2/sql?q=SELECT+*,+ST_Y(the_geom)+AS+lat,+ST_X(the_geom)+AS+lng+FROM+opa_properties_public&filename=opa_properties_public&format=csv&skipfields=cartodb_id'
    response = requests.get(url)
    with open(path_phi_pp, 'wb') as f:
        f.write(response.content)


t9a = PythonOperator(
    task_id='get_phi_properties_public',
    python_callable=get_phi_pp,
    provide_context=False,
    dag=dag,
)


def phi_pp_etl():
    """
    Preliminary data cleaning from properties public file and drop some unused columns
    """
    phi_opa_properties_public_df = pd.read_csv(path_phi_pp)

    phi_opa_properties_public_df_2 = phi_opa_properties_public_df.drop(columns=['the_geom', 'assessment_date',
                                    'beginning_point', 'house_number','book_and_page', 'zoning','census_tract',
                                    'mailing_address_1', 'mailing_address_2', 'mailing_care_of', 'mailing_street',
                                    'other_building', 'the_geom_webmercator', 'recording_date', 'sale_date',
                                    'registry_number', 'owner_1', 'owner_2', 'parcel_number', 'parcel_shape',
                                    'basements', 'objectid', 'building_code', 'year_built_estimate', 'house_extension',
                                    'cross_reference', 'date_exterior_condition', 'sewer', 'site_type','state_code',
                                    'street_designation','street_name', 'street_direction', 'mailing_zip',
                                    'mailing_city_state', 'market_value_date'])
    phi_opa_properties_public_df_2.to_csv(path_etl_phi_pp)


t9b = PythonOperator(
    task_id='phi_properties_public_etl',
    python_callable=phi_pp_etl,
    provide_context=False,
    dag=dag,
)


def phi_pp_csv_to_mysql():
    conn = create_engine("mysql+pymysql://" + mysqluser + ":" + mysqlkey + "@localhost:3306/data_group_project")
    df = pd.read_csv(path_etl_phi_pp, delimiter=',')
    df.to_sql(name='properties_public', con=conn, schema='data_group_project', if_exists='replace')


t9c = PythonOperator(
        task_id='phi_properties_public_csv_to_mysql',
        python_callable=phi_pp_csv_to_mysql,
        dag=dag,
)


###############################################
#t10: Philly Universities and Colleges

path_phi_uc = os.path.join(os.path.dirname(os.path.dirname(__file__)),'data/phi_universities_colleges.csv')
path_etl_phi_uc = os.path.join(os.path.dirname(os.path.dirname(__file__)),'data/phi_universities_colleges_2.csv')

def get_phi_uc():
    """
    get the Philadelphia universities and colleges
    """
    url = 'http://data.phl.opendata.arcgis.com/datasets/8ad76bc179cf44bd9b1c23d6f66f57d1_0.csv'
    response = requests.get(url)
    with open(path_phi_uc, 'wb') as f:
        f.write(response.content)


t10a = PythonOperator(
    task_id='get_phi_universities_colleges',
    python_callable=get_phi_uc,
    provide_context=False,
    dag=dag,
)


def phi_uc_etl():
    """
    Preliminary data cleaning from universities and colleges file and drop some unused columns
    """
    phi_universities_colleges_df = pd.read_csv(path_phi_uc)

    phi_universities_colleges_df_2 = phi_universities_colleges_df.drop(columns=['PARCEL_ID', 'BRT_ID', 'Shape__Area',
                                                                                'TENCODE_ID', 'Shape__Length'])
    phi_universities_colleges_df_2.to_csv(path_etl_phi_uc)


t10b = PythonOperator(
    task_id='phi_universities_colleges_etl',
    python_callable=phi_uc_etl,
    provide_context=False,
    dag=dag,
)


def phi_uc_csv_to_mysql():
    conn = create_engine("mysql+pymysql://" + mysqluser + ":" + mysqlkey + "@localhost:3306/data_group_project")
    df = pd.read_csv(path_etl_phi_hc, delimiter=',')
    df.to_sql(name='universities_colleges', con=conn, schema='data_group_project', if_exists='replace')


t10c = PythonOperator(
        task_id='phi_universities_colleges_csv_to_mysql',
        python_callable=phi_uc_csv_to_mysql,
        dag=dag,
)


t_end = DummyOperator(
    task_id='phi_data_storage_done',
    retries=1,
    dag=dag
)

t0 >> t1a >> t1b >> t1c >> t_end
t0 >> t2a >> t2b >> t2c >> t_end
t0 >> t3a >> t3b >> t3c >> t_end
t0 >> t4a >> t4b >> t4c >> t_end
t0 >> t5a >> t5b >> t5c >> t_end
t0 >> t6a >> t6b >> t6c >> t_end
t0 >> t7a >> t7b >> t7c >> t_end
t0 >> t8a >> t8b >> t8c >> t_end
t0 >> t9a >> t9b >> t9c >> t_end
t0 >> t10a >> t10b >> t10c >> t_end