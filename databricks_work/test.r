# Databricks notebook source
data <- collect(sql(sqlContext, "select * from 0407_downtown_rec_df_2 tables"))
data

# COMMAND ----------

