# IMPORT MODULE
import json
import pandas as pd
import numpy as np

# IMPORT SCRIPT
from connection.mysql import MySQL
from connection.postgresql import PostgreSQL

# IMPORT SQL
from sql.query import create_table_dim, create_table_fact


with open ('credential.json', "r") as cred:                                         # cred = open('credential.json', 'r')
        credential = json.load(cred)

def insert_raw_data():
    mysql_auth = MySQL(credential['mysql_lake'])
    engine, engine_conn = mysql_auth.connect()

    data = pd.read_csv('D:/all about python/testing2/data/covid_19_indonesia_time_series_all.csv')

    data.columns = [x.lower() for x in data.columns.to_list()]
    data.to_sql(name='db_testing2', con=engine, if_exists="replace", index=False)
    engine.dispose()

def create_star_schema(schema):
  postgre_auth = PostgreSQL(credential['postgresql_warehouse'])
  conn, cursor = postgre_auth.connect(conn_type='cursor')

  query_dim = create_table_dim(schema=schema)
  cursor.execute(query_dim)
  conn.commit()

#   query_fact = create_table_fact(schema=schema)
#   cursor.execute(query_fact)
#   conn.commit()

  cursor.close()
  conn.close()

def insert_dim_location(data):
    column_start = ["location iso code", "location", "location level"]
    column_end = ["location_code", "location", "location_level"]

    data = data[column_start]
    data = data.drop_duplicates(column_start)
    data.columns = column_end

    return data

def insert_dim_province(data):
    column_start = ["location iso code", "province", "island", "time zone", "special status"]
    column_end = ["location_code", "province", "island", "time_zone", "special_status"]

    data = data[column_start]
    data = data.drop_duplicates(column_start)
    data.columns = column_end

    return data

def insert_dim_province_population_level(data):
    column_start = ["location iso code", "total regencies", \
        "total cities", "total districts", "total urban villages", "total rural villages"]
    column_end = ["location iso code", "population_level"]

    data = data[column_start]
    data = data.melt(id_vars=["location iso code"], var_name="population_level", value_name="total")
    data = data.groupby(by=['location iso code', 'population_level']).sum()
    data = data.reset_index()
    data = data[column_end]
    data = data.rename({'location iso code': 'location_code'}, axis=1)
    data['population_level'] = data['population_level'].apply(lambda x: x[6:])

    return data    

def insert_dim_province_population_total(data):
    column_start = ["location iso code", "population", "population density"]
    column_end = ["location_code", "population", "population_density"]

    data = data[column_start]
    data = data.drop_duplicates(column_start)
    data.columns = column_end

    return data    

def insert_dim_province_detail_level(data):
    column_start = ["location iso code", "area (km2)", \
        "longitude", "latitude", "population"]
    column_end = ["location_code", "area", "longitude", "latitude"]

    data = data[column_start]
    data = data.drop_duplicates(column_start)
    data = data.rename({'location iso code': 'location_code'}, axis=1)
    data = data.rename({'area (km2)': 'area'}, axis=1)
    data = data[column_end]

    return data        

def insert_dim_case(data):
    column_start = ["new cases", "new deaths", \
         "new recovered", "new active cases", \
         "total cases", "total deaths", "total recovered", \
         "total active cases"]
    column_end = ["id", "status_name", "status_detail", "status"]

    data = data[column_start]
    data = data[:1]
    data = data.melt(var_name="status", value_name="total")
    data = data.drop_duplicates("status").sort_values("status")
    
    data['id'] = np.arange(1, data.shape[0]+1)
    data[['status_name', 'status_detail']] = data['status'].str.split(' ', n=1, expand=True)
    data = data[column_end]

    return data

def insert_fact_province_daily(data, dim_case):
    column_start = ["date", "location iso code", "new cases per million", "total cases per million", "new cases", "new deaths", \
         "new recovered", "new active cases", "total cases", "total deaths", "total recovered", \
         "total active cases"]
    column_end = ['date', 'location_code', 'new_cases_per_million', 'total_cases_per_million', 'status', 'total']

    # AGGREGATE
    data = data[column_start]
    data = data.melt(id_vars=["date", "location iso code", "new cases per million", 'total cases per million'], \
         var_name="status", value_name="total").sort_values(["date", "location iso code", "status"])
    data = data.groupby(by=['date', 'location iso code', 'new cases per million', 'total cases per million', 'status']).sum()
    data = data.reset_index()

    # REFORMAT
    data.columns = column_end
    data['id'] = np.arange(1, data.shape[0]+1)

    # MERGE
    dim_case = dim_case.rename({'id': 'case_id'}, axis=1)
    data = pd.merge(data, dim_case, how='inner', on='status')
    
    data = data[['date', "id", "case_id", 'location_code', 'new_cases_per_million', 'total_cases_per_million', 'status', 'total']]
    
    return data

def insert_fact_province_monthly(data, dim_case):
    column_start = ["date", "location iso code", "new cases per million", "total cases per million", "new cases", "new deaths", \
         "new recovered", "new active cases", "total cases", "total deaths", "total recovered", \
         "total active cases"]
    column_end = ['date', 'location_code', 'new_cases_per_million', 'total_cases_per_million', 'status', 'total']

    # AGGREGATE
    data = data[column_start]
    data['date'] = data['date'].apply(lambda x: x[0:1] + x[-5:11])
    data = data.melt(id_vars=["date", "location iso code", "new cases per million", 'total cases per million'], \
         var_name="status", value_name="total").sort_values(["date", "location iso code", "status"])
    data = data.groupby(by=['date', 'location iso code', 'new cases per million', 'total cases per million', 'status']).sum()
    data = data.reset_index()

    # REFORMAT
    data.columns = column_end
    data['id'] = np.arange(1, data.shape[0]+1)

    # MERGE
    dim_case = dim_case.rename({'id': 'case_id'}, axis=1)
    data = pd.merge(data, dim_case, how='inner', on='status')
    
    data = data[['date', "id", "case_id", 'location_code', 'new_cases_per_million', 'total_cases_per_million', 'status', 'total']]
    
    return data

def insert_raw_to_warehouse(schema):
    mysql_auth = MySQL(credential['mysql_lake'])
    engine, engine_conn = mysql_auth.connect()
    data = pd.read_sql(sql='db_testing2', con=engine)
    engine.dispose()

    # filter needed column
    column = ["date", "location iso code", "location", "new cases", "new deaths", "new recovered", \
         "new active cases", "total cases", "total deaths", "total recovered", "total active cases", \
            "location level", "city or regency", "province", "country", "continent", "island", "time zone", \
                "special status", "total regencies", "total cities", "total districts", "total urban villages", \
                     "total rural villages", "area (km2)", "population", "population density", "longitude", "latitude", \
                          "new cases per million", "total cases per million", "new deaths per million", "total deaths per million", \
                               "case fatality rate", "case recovered rate", "growth factor of new cases", "growth factor of new deaths"]
    data = data[column]

    dim_location = insert_dim_location(data)
    dim_province = insert_dim_province(data)
    dim_province_population_level = insert_dim_province_population_level(data)
    dim_province_population_total = insert_dim_province_population_total(data)
    dim_province_detail_level = insert_dim_province_detail_level(data)
    dim_case = insert_dim_case(data)

    fact_province_daily = insert_fact_province_daily(data, dim_case)
    fact_province_monthly = insert_fact_province_monthly(data, dim_case)

    postgre_auth = PostgreSQL(credential['postgresql_warehouse'])
    engine, engine_conn = postgre_auth.connect(conn_type='engine')

    dim_location.to_sql('dim_location', schema=schema, con=engine, index=False, if_exists='replace')
    dim_province.to_sql('dim_province', schema=schema, con=engine, index=False, if_exists='replace')
    dim_province_population_level.to_sql('dim_province_population_level', schema=schema, con=engine, index=False, if_exists='replace')
    dim_province_population_total.to_sql('dim_province_population_total', schema=schema, con=engine, index=False, if_exists='replace')
    dim_province_detail_level.to_sql('dim_province_detail_level', schema=schema, con=engine, \
         index=False, if_exists='replace')
    dim_case.to_sql('dim_case', schema=schema, con=engine, index=False, if_exists='replace')

    fact_province_daily.to_sql('fact_province_daily', schema=schema, con=engine, index=False, if_exists='replace')  
    fact_province_monthly.to_sql('fact_province_monthly', schema=schema, con=engine, index=False, if_exists='replace')  

    engine.dispose()

if __name__ == '__main__':
  insert_raw_data()
  create_star_schema(schema='handra')
  insert_raw_to_warehouse(schema='handra')