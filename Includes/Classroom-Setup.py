# Databricks notebook source
# MAGIC %run ./_common

# COMMAND ----------

DA = DBAcademyHelper(**helper_arguments)

if dbgems.get_parameter(GENERATING_DOCS, False) == False:
    # Setup the environment only if we are generating docs
    DA.reset_environment()
    DA.init(install_datasets=True, create_db=True)

    # This is here to facilitate asycronous testing where each test
    # run is using a different database and thus needs to be reseeded
    DA.populate_database(DA.db_name, verbose=True)

DA.publisher = Publisher(DA)

DA.conclude_setup()

