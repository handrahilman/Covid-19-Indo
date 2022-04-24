def create_table_dim(schema):
  query = f"""
  CREATE TABLE IF NOT EXISTS {schema}.dim_location (
    location_code text primary key,
    location text);
  CREATE TABLE IF NOT EXISTS {schema}.dim_province (
    province text primary key,
    location text,
    location_level text);
  CREATE TABLE IF NOT EXISTS {schema}.dim_province_population_level (
      location_code text primary key,
      population_level text);
  CREATE TABLE IF NOT EXISTS {schema}.dim_province_population_total (
      location_code text primary key,
      population_density int,
      population int);
  CREATE TABLE IF NOT EXISTS {schema}.dim_province_detail_level (
      location_code text primary key,
      area int,
      longitude int,
      latitude int);
  CREATE TABLE IF NOT EXISTS {schema}.dim_case (
      id SERIAL primary key,
      status_name text,
      status_detail text,
      status text);
  """

  return query

  
def create_table_fact(schema):
  query = f"""
  CREATE TABLE IF NOT EXISTS {schema}.fact_province_daily (
    date text,
    id SERIAL,
    case_id primary key,
    location_code text,
    new_cases_per_million float,
    total_cases_per_million float,
    status text,
    total bigint);
  """

  return query