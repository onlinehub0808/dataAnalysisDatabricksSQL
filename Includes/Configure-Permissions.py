# Databricks notebook source
# MAGIC %run ./Configure-Permissions-Setup

# COMMAND ----------

DA.workspace.do_for_all_users(DA.update_user_grants)

