def create_table_dim(schema):
  query = f"""
  CREATE TABLE IF NOT EXISTS {schema}.dim_location (
    location_code text primary key,
    location text;
  CREATE TABLE IF NOT EXISTS {schema}.dim_province (
      location_code text primary key,
      location text;
      location_level text;
  CREATE TABLE IF NOT EXISTS {schema}.dim_population_detail (
      location_code text primary key,
      population_level text;
  CREATE TABLE IF NOT EXISTS {schema}.dim_case (
      id SERIAL primary key,
      status_name text,
      status_detail text,
      status text);
  """

  return query