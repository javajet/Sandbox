-- Databricks notebook source
--%fs ls /databricks-datasets/COVID/covid-19-data/us-counties.csv

-- COMMAND ----------

--%fs head /databricks-datasets/COVID/covid-19-data/us-counties.csv

-- COMMAND ----------

--%sh ls /dbfs/databricks-datasets/COVID/covid-19-data/

-- COMMAND ----------

--SELECT * FROM csv.`/databricks-datasets/COVID/covid-19-data/us-counties.csv`

-- COMMAND ----------

CREATE INCREMENTAL LIVE TABLE covid_raw
COMMENT "The raw covid dataset, ingested from /databricks-datasets."
TBLPROPERTIES ("quality" = "bronze")
--AS SELECT * FROM csv.`/databricks-datasets/COVID/covid-19-data/us-counties.csv`
--OPTIONS (header "true")
AS SELECT * FROM cloud_files("/databricks-datasets/COVID/covid-19-data/us-counties.csv*", "csv", map("header", "true"))


-- COMMAND ----------

CREATE LIVE TABLE covid_clean(
  CONSTRAINT valid_location EXPECT (county IS NOT NULL and state IS NOT NULL and fips IS NOT NULL),
  CONSTRAINT valid_date EXPECT (date IS NOT NULL),
  CONSTRAINT valid_reporting EXPECT (cases >= 0) ON VIOLATION FAIL UPDATE
)
COMMENT "Covid dataset with cleaned-up datatypes / column names and data quality expectations."
TBLPROPERTIES ("quality" = "silver")
AS SELECT
  CAST (date AS DATE) AS date,
  state,
  county,
  fips,
  CAST(cases AS INT) AS cases,
  CAST (deaths AS INT) AS deaths
FROM live.covid_raw

-- COMMAND ----------

CREATE LIVE TABLE top_cases
COMMENT "A table of the 10 counties with the highest number of covid cases."
TBLPROPERTIES ("quality" = "gold")
AS SELECT
  county,
  state,
  SUM(cases) as total_cases
FROM live.covid_clean
GROUP BY state, county
ORDER BY total_cases DESC
LIMIT 10

-- COMMAND ----------

CREATE LIVE TABLE top_deaths
COMMENT "A list of the top 5 states by covid deaths."
TBLPROPERTIES ("quality" = "gold")
AS SELECT
  state,
  SUM(deaths) as total_deaths
FROM live.covid_clean
GROUP BY state
ORDER BY 2 DESC
LIMIT 5
