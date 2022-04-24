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