"""Sql Statements to Create Tables in redshift"""

CREATE_HAPPINESS_SQL = """
CREATE TABLE IF NOT EXISTS happiness (
  Country varchar(30),
    Region varchar(50),
    Happiness_Rank INTEGER,
    Happiness_Score DECIMAL(8,5),
    Lower_Confidence DECIMAL(8,5),
    Upper_Confidence DECIMAL(8,5),
    Economy DECIMAL(8,5),
    Family DECIMAL(8,5),
    Health DECIMAL(8,5),
    Freedom DECIMAL(8,5),
    Trust_on_gov DECIMAL(8,5),
    generosity DECIMAL(8,5),
    Dystopia DECIMAL(8,5))
"""

CREATE_MODE_SQL = """
CREATE TABLE IF NOT EXISTS mode_dim (
  id INTEGER,
  mode VARCHAR(20),
  PRIMARY KEY(id))
  DISTSTYLE ALL;
"""

CREATE_VISA_SQL = """
CREATE TABLE IF NOT EXISTS visa_dim (
  id INTEGER,
  visa VARCHAR(20),
  PRIMARY KEY(id))
  DISTSTYLE ALL;
"""

CREATE_COUNTRY_SQL = """
CREATE TABLE IF NOT EXISTS country (
  id INTEGER,
  country VARCHAR(50))
"""

CREATE_HAPPINESS_DIMENSION_SQL = """
CREATE TABLE IF NOT EXISTS happiness_dim (
  id INTEGER,
  country VARCHAR(80),
  region VARCHAR(80),
  happiness_score DECIMAL(8,5),
  economy DECIMAL(8,5),
  PRIMARY KEY(id))
  DISTSTYLE ALL;
"""

LOAD_HAPPINESS_DIM_SQL = """
INSERT INTO happiness_dim (id, country, region, happiness_score, economy)
SELECT c.id, c.country, h.region, h.happiness_score, h.economy
FROM country c
JOIN happiness h
ON c.country = h.country
"""

CREATE_IMMIGRATION_FACT_SQL = """
CREATE TABLE IF NOT EXISTS immigration_fact (
  id INTEGER,
  arrival_date DATE,
  departure_date DATE,
  arrival_country INTEGER,
  age INTEGER,
  gender VARCHAR(1),
  visa_type INTEGER,
  mode INTEGER,
  airline VARCHAR(5),
  duration INTEGER)
"""

CREATE_RESULT_SQL = """
CREATE TABLE IF NOT EXISTS result AS
SELECT 
	i.id,
    arrival_date,
    departure_date,
    arrival_country,
    age,
    gender,
    v.visa,
    m.mode,
    airline,
    duration,
    country,
    region
FROM
immigration_fact i
JOIN happiness_dim h ON i.arrival_country=h.id
JOIN mode_dim m ON i.mode=m.id
JOIN visa_dim v ON i.visa_type=v.id;
"""

UNLOAD_RESULT_SQL = """
unload ('select * from result')   
to 's3://capstone-demo-project/analytic-results/result.csv' 
iam_role 'arn:aws:iam::259456894260:role/myRedshiftRole';
"""
