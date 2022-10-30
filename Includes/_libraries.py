# Databricks notebook source
def __validate_libraries():
    import requests
    sites = [
        "https://github.com/databricks-academy/dbacademy",
        "https://pypi.org/simple/overrides",  # Slated for removal
        "https://pypi.org/simple/deprecated", # Slated for removal
        "https://pypi.org/simple/wrapt",      # Slated for removal
    ]
    for site in sites:
        try:
            response = requests.get(site)
            error = f"Unable to access GitHub or PyPi resources (HTTP {response.status_code} for {site})."
            assert response.status_code == 200, "{error} Please see the \"Troubleshooting | {section}\" section of the \"Version Info\" notebook for more information.".format(error=error, section="Cannot Install Libraries")
        except Exception as e:
            if type(e) is AssertionError: raise e
            error = f"Unable to access GitHub or PyPi resources ({site})."
            raise AssertionError("{error} Please see the \"Troubleshooting | {section}\" section of the \"Version Info\" notebook for more information.".format(error=error, section="Cannot Install Libraries")) from e
            
__validate_libraries()

# COMMAND ----------

version = spark.conf.get("dbacademy.library.version", "v1.0.39")

if not version.startswith("v"): library_url = f"git+https://github.com/databricks-academy/dbacademy@{version}"
else: library_url = f"https://github.com/databricks-academy/dbacademy/releases/download/{version}/dbacademy-{version[1:]}-py3-none-any.whl"

default_command = f"install --quiet --disable-pip-version-check {library_url}"
pip_command = spark.conf.get("dbacademy.library.install", default_command)

if pip_command != default_command:
    print(f"WARNING: Using alternative library installation:\n| default: %pip {default_command}\n| current: %pip {pip_command}")

# COMMAND ----------

# MAGIC %pip $pip_command
