# Databricks notebook source
# MAGIC %md # Getting a variable
# MAGIC

# COMMAND ----------

#type
#query_type = "highest"
#create widget 
dbutils.widgets.dropdown("query_type", "highest", ["highest", "lowest"])
#get values of widget
query_type = dbutils.widgets.get("query_type")
#print
print(f"query_type: {query_type}")

var = "DESC" if query_type == "highest" else "ASC"
print(f"var: {var}")

# COMMAND ----------

# MAGIC %md # creating a query
# MAGIC

# COMMAND ----------

salary_rank= f"""WITH RankedSalaries AS (
    SELECT 
        e_id,
        e_name,
        e_dept_id,
        e_salary,
        rank() OVER (PARTITION BY e_dept_id ORDER BY e_salary {var}) AS rank
    FROM exercise.my_schema.e_table
)
SELECT 
   e_id,
   e_name,
   e_dept_id,
   COALESCE(e_salary,0) as e_salary
FROM RankedSalaries
WHERE rank = 1""" 
print(salary_rank)

# COMMAND ----------

# MAGIC %md # executing the query

# COMMAND ----------

df = spark.sql(salary_rank)
print(df)

# COMMAND ----------

# MAGIC %md ##display the result

# COMMAND ----------

display(df)
