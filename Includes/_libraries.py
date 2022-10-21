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

# MAGIC %pip install --quiet --disable-pip-version-check \
# MAGIC git+https://github.com/databricks-academy/dbacademy@v1.0.25
