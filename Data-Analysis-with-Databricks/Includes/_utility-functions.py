# Databricks notebook source
# MAGIC %pip install \
# MAGIC git+https://github.com/databricks-academy/dbacademy-gems \
# MAGIC git+https://github.com/databricks-academy/dbacademy-rest \
# MAGIC --quiet --disable-pip-version-check

# COMMAND ----------

_course_code = "dawd"
_naming_params = {"course": _course_code}
_data_source_name = "data-analysis-with-databricks/v01"

# COMMAND ----------

class Paths():
    
    def __init__(self, working_dir):
        self.working_dir = working_dir
        self.user_db = f"{working_dir}/database.db"

    def exists(self, path):
        try: return len(dbutils.fs.ls(path)) >= 0
        except Exception:return False

    def print(self, padding="  "):
        max_key_len = 0
        for key in self.__dict__: max_key_len = len(key) if len(key) > max_key_len else max_key_len
        for key in self.__dict__:
            label = f"{padding}DA.paths.{key}:"
            print(label.ljust(max_key_len+13) + DA.paths.__dict__[key])
        
    def __repr__(self):
        return self.__dict__.__repr__().replace(", ", ",\n").replace("{","").replace("}","").replace("'","")

# COMMAND ----------

class DBAcademyHelper():
    
    def __init__(self):
        import re, time
        from dbacademy.dbrest import DBAcademyRestClient

        # Defined at the top of this file, exterinally for easier reference
        global _course_code, _naming_params, _data_source_name
        self.course_code = _course_code
        self.naming_params = _naming_params
        self.data_source_name = _data_source_name
        
        self.start = int(time.time())

        self.naming_template = "da-{da_name}@{da_hash}-{course}"
        self.publisher = Publisher(self.naming_template, self.naming_params)
        
        self.client = DBAcademyRestClient()

        self.usernames = [r.get("userName") for r in self.client.scim().users().list()]
        self.usernames.sort()

        # Define username
        self.username = spark.sql("SELECT current_user()").first()[0]
        self.clean_username = re.sub("[^a-zA-Z0-9]", "_", self.username)

        self.working_dir_prefix = f"dbfs:/mnt/dbacademy-users/{self.username}/{self.course_code}"
        
        working_dir = self.working_dir_prefix
        self.paths = Paths(working_dir)

    def cleanup(self):
        for stream in spark.streams.active:
            print(f"Stopping the stream \"{stream.name}\"")
            stream.stop()
            try: stream.awaitTermination()
            except: pass # Bury any exceptions

        if self.paths.exists(self.paths.working_dir):
            print(f"Removing the working directory \"{self.paths.working_dir}\"")
            dbutils.fs.rm(self.paths.working_dir, True)

    def conclude_setup(self):
        import time
        
        for key in self.paths.__dict__:
            spark.conf.set(f"da.paths.{key.lower()}", self.paths.__dict__[key])
        
        print("\nPredefined Paths:")
        self.paths.print()

        print(f"\nSetup completed in {int(time.time())-self.start} seconds")

    @staticmethod
    def monkey_patch(function_ref, delete=True):
        """
        This function "monkey patches" the specified function to the DBAcademyHelper class. While not 100% necissary, this
        pattern does allow each function to be defined in it's own cell which makes authoring notebooks a little bit easier.
        """
        import inspect
        
        signature = inspect.signature(function_ref)
        assert "self" in signature.parameters, f"""Missing the required parameter "self" in the function "{function_ref.__name__}()" """
        
        setattr(DBAcademyHelper, function_ref.__name__, function_ref)
        if delete: 
            del function_ref

# COMMAND ----------

def install_datasets(self, reinstall=False):
    import time

    min_time = "3 min"
    max_time = "10 min"
    
    self.paths.datasets = f"dbfs:/mnt/dbacademy-datasets/{self.data_source_name}"
    self.data_source_uri = f"wasbs://courseware@dbacademy.blob.core.windows.net/{self.data_source_name}"
    print(f"The source for this dataset is\n{self.data_source_uri}/\n")

    print(f"Your dataset directory is\n{self.paths.datasets}\n")
    existing = self.paths.exists(self.paths.datasets)

    if not reinstall and existing:
        print(f"Skipping install of existing dataset.")
        self.validate_datasets()
        return 

    # Remove old versions of the previously installed datasets
    if existing:
        print(f"Removing previously installed datasets from\n{self.paths.datasets}")
        dbutils.fs.rm(self.paths.datasets, True)

    print(f"""Installing the datasets to {self.paths.datasets}""")

    print(f"""\nNOTE: The datasets that we are installing are located in Washington, USA - depending on the
          region that your workspace is in, this operation can take as little as {min_time} and 
          upwards to {max_time}, but this is a one-time operation.""")

    files = dbutils.fs.ls(self.data_source_uri)
    print(f"\nInstalling {len(files)} datasets: ")
    
    install_start = int(time.time())
    for f in files:
        start = int(time.time())
        print(f"Copying /{f.name[:-1]}", end="...")

        dbutils.fs.cp(f"{self.data_source_uri}/{f.name}", f"{self.paths.datasets}/{f.name}", True)
        print(f"({int(time.time())-start} seconds)")

    print()
    self.validate_datasets()
    print(f"""\nThe install of the datasets completed successfully in {int(time.time())-install_start} seconds.""")  

DBAcademyHelper.monkey_patch(install_datasets)

# COMMAND ----------

# def validate_path(self, expected, path):
#     files = dbutils.fs.ls(path)
#     message = f"Expected {expected} files, found {len(files)} in {path}"
#     for file in files:
#         message += f"\n{file.path}"
#     assert len(files) == expected, message 
    
# DBAcademyHelper.monkey_patch(validate_path)

# COMMAND ----------

def list_r(self, path, prefix=None, results=None):
    if prefix is None: prefix = path
    if results is None: results = list()
    
    files = dbutils.fs.ls(path)
    for file in files:
        data = file.path[len(prefix):]
        results.append(data)
        if file.isDir(): 
            self.list_r(file.path, prefix, results)
        
    results.sort()
    return results

def validate_datasets(self):
    import time
    start = int(time.time())
    print(f"\nValidating the local copy of the datsets", end="...")
    
    local_files = self.list_r(self.paths.datasets)
    remote_files = self.list_r(self.data_source_uri)

    for file in local_files:
        if file not in remote_files:
            print(f"\n  - Found extra file: {file}")
            print(f"  - This problem can be fixed by reinstalling the datasets")
            raise Exception("Validation failed - see previous messages for more information.")

    for file in remote_files:
        if file not in local_files:
            print(f"\n  - Missing file: {file}")
            print(f"  - This problem can be fixed by reinstalling the datasets")
            raise Exception("Validation failed - see previous messages for more information.")
        
    print(f"({int(time.time())-start} seconds)")
    
DBAcademyHelper.monkey_patch(list_r)
DBAcademyHelper.monkey_patch(validate_datasets)

# COMMAND ----------

class Publisher():
    def __init__(self, naming_template, naming_params):
        self.steps = list()
        self.naming_template = naming_template
        self.naming_params = naming_params
    
    def add_validation(self, instructions, statements, label, expected, length):
        # Convert single statement to the required list
        if statements is None: statements = []
        if type(statements) == str: statements = [statements]
        
        step = Validation(len(self.steps)+1, instructions, label, expected, length, self.naming_template, self.naming_params)
        
        step.add("USE {db_name};")
        
        for statement in statements:
            step.add(statement)
            
        self.steps.append(step)
        return step
        
    def add_step(self, include_use, instructions, statements):
        # Convert single statement to the required list
        if statements is None: statements = []
        if type(statements) == str: statements = [statements]
        
        step = Step(len(self.steps)+1, instructions, self.naming_template, self.naming_params)
        
        if include_use:
            step.add("USE {db_name};")
            
        for statement in statements:
            step.add(statement)
            
        self.steps.append(step)
        return step
        
    def find_repo(self):
        from dbacademy import dbgems
        
        for i in range(1, 10):
            repo_path = dbgems.get_notebook_dir(offset=-i)
            status = DA.client.workspace().get_status(repo_path)
            if status.get("object_type") == "REPO": return status
        assert status.get("object_type") == "REPO", f"Repo not found."
        return status

    def publish(self, lesson_name=None, include_inputs=True):
        import re, os
        from dbacademy import dbgems
        
        # if dbgems.get_tag("jobId") is not None:
        #     print("Skipping publish while running as a job")
        #     return ""
        
        lesson_name = dbgems.get_notebook_name() if lesson_name is None else lesson_name
        file_name = re.sub("[^a-zA-Z0-9]", "-", lesson_name)+".html"
        for i in range(10): file_name = file_name.replace("--", "-")
        
        html = self.to_html(lesson_name, include_inputs)

        repo_path = self.find_repo().get("path")
        target_file = f"/Workspace{repo_path}/docs/{file_name}"
        print(f"Target:  {target_file}")
        
        target_dir = "/".join(target_file.split("/")[:-1])
        if not os.path.exists(target_dir):
            os.makedirs(target_dir)
        
        with open(target_file, "w") as w:
            w.write(html)
        
        url = f"https://urban-umbrella-8e9e1ba3.pages.github.io/{file_name}"
        displayHTML(f"<a href='{url}' target='_blank'>GitHub Page</a> (Changes must be committed before seeing the new page)")
        
        return html
        
    def to_html(self, lesson_name, include_inputs):
        from datetime import date
        
        html = f"""<!DOCTYPE html><html lang="en"><head><meta charset="utf-8"/><title>{lesson_name}</title>
            <link href='https://fonts.googleapis.com/css?family=DM Sans' rel='stylesheet'>
            <style>
                h2 {{
                    color: #ff3621;
                }}
                h3 {{
                    margin-left: 15px
                }}
                ol {{
                    margin-left: -50px; 
                    font-family:sans-serif; 
                    color: #618794;
                }}
                td {{
                    padding: 5px;
                    border-bottom: 1px solid #ededed;
                }}
                
                tr.selected td {{
                    color: white;
                    background-color: red;
                }}
                tbody.main_table td {{ 
                    background-color: #D1E2FF; 
                }}
                .monofont {{font-family: monospace; font-size: 14px}}
                .content {{max-width: 800px; margin: auto; padding-left: 50px}}
                .image-icon-inline {{display:inline; vertical-align: middle; margin: 0 10px 0 10px}}
                .instructions-div {{padding-left: 40px}}
            </style>
        </head><body onload=loaded(); style="background-color: #f9f7f4; font-family: 'DM Sans', serif;">"""
        html += """<div class="content">"""
        
        html += """<img src="https://s3.us-west-2.amazonaws.com/files.training.databricks.com/images/db-academy-rgb-1200px_no_bg.png" 
                        alt="Databricks Learning" 
                        style="width: 600px; margin-left: 100px; margin-right: 100px">\n"""
        html += "<hr/>\n"
        
        html += f"<h1>{lesson_name}</h1>"
        
        html += f"""
        <p>The two fields below are used to customize queries used in this course. Enter your schema (database) name and username, and press "Enter" to populate necessary information in the queries on this page.</p>
        <table>
            <tr>
                <td style="white-space:nowrap">Schema Name:&nbsp;</td>
                <td><input id="db_name" type="text" style="width:40em" onchange="update();"></td>
            </tr><tr>
                <td style="white-space:nowrap">Username:&nbsp;</td>
                <td><input id="username" type="text" style="width:40em" onchange="update();"></td>
            </tr>
        </table>"""
        
        for step in self.steps:
            html += "<hr/>\n"
            html += step.to_html(None, validate=False)
        
        # The ID elements that must be updated at runtime
        ids = [s.id for s in self.steps]  
        
        # F-strings complicate how to compose the following block of code so we inject the
        # following tokens to skirt various escaping issues related to f-strings
        username_token = "{username}"     # The runtime token for the username
        db_name_token = "{db_name}"       # The runtime token for the db_name
        
        # Create the Javascript used to update the HTMl at runtime
        html += f"""<script type="text/javascript">
            function answerIs(self, expected) {{
                if (self.value === "") {{
                    self.style.backgroundColor="#ffffff";
                }} else if (expected.includes(self.value.toLowerCase())) {{
                    self.style.backgroundColor="#7ffe78";
                }} else {{
                    self.style.backgroundColor="#ffb9bb";
                }}
            }}
            function loaded() {{
                let data = document.cookie;
                if (data != null && data.trim() != "") {{
                    parts = data.split(";");
                    for (i = 0; i < parts.length; i++) {{
                        let key_value = parts[i].trim();
                        let key = key_value.split("=")[0].trim();
                        let value = key_value.split("=")[1].trim();

                        if (key == "{DA.course_code}_db_name") document.getElementById("db_name").value = value;
                        else if (key == "{DA.course_code}_username") document.getElementById("username").value = value;
                        else console.log("Unknown cookie: "+key);
                    }}
                }}
                update();
            }}
            function update() {{      
                let db_name = document.getElementById("db_name").value;
                let username = document.getElementById("username").value;
                let ids = {ids};

                if (db_name === "" || username === "" || db_name === null || username === null) {{
                    for (let i = 0; i < ids.length; i++) {{
                        document.getElementById(ids[i]+"-ta").disabled = true;
                        document.getElementById(ids[i]+"-btn").disabled = true;

                        let ba = document.getElementById(ids[i]+"-backup");
                        document.getElementById(ids[i]+"-ta").value = ba.value
                    }}
                }} else {{
                    for (let i = 0; i < ids.length; i++) {{
                        document.getElementById(ids[i]+"-ta").disabled = false;
                        document.getElementById(ids[i]+"-btn").disabled = false;

                        let ba = document.getElementById(ids[i]+"-backup");
                        let value = ba.value.replaceAll("{db_name_token}", db_name)
                                            .replaceAll("{username_token}", username);

                        document.getElementById(ids[i]+"-ta").value = value

                        document.cookie = "{DA.course_code}_db_name="+db_name;
                        document.cookie = "{DA.course_code}_username="+username;
                    }}
                }}
            }}
        </script>"""
        
        if include_inputs == False:
            html += f"""<script type="text/javascript">
                    document.getElementById("db_name").value = "n/a";
                    document.getElementById("username").value = "n/a";
            </script>"""
        
        html += f"""
        <hr/>
        <div>
            <p>© {date.today().year} Databricks, Inc. All rights reserved.<br>
               Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="https://www.apache.org/">Apache Software Foundation</a>.<br>
               <br>
               <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | 
               <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="https://help.databricks.com/">Support</a>
            </p>
        </div>"""
            
        html += """</div></body></html>"""
        return html

# COMMAND ----------

class Step():
    def __init__(self, number:int, instructions:str, naming_template:str, naming_params:dict):
        
        assert type(number) == int, f"Expected the parameter \"number\" to be of type int, found {type(number)}"
        assert instructions is None or type(instructions) == str, f"Expected the parameter \"instructions\" to be of type str, found {type(instructions)}"

        assert type(naming_params) == dict, f"Expected the parameter \"naming_params\" to be of type dict, found {type(naming_params)}"

        self.width = 800
        self.widthpx = f"{self.width}px"
        
        self.number = number
        self.id = f"step-{number}"
        self.instructions = instructions

        self.naming_template = naming_template
        self.naming_params = naming_params
        
        self.statements = []
        
    def add(self, statement):
        assert statement is not None, "The statement must be specified"
        statement = statement.strip()
        if statement != "":
            assert statement.endswith(";"), f"Exepcted statement to end with a semi-colon:\n{statement}"
            assert statement.count(";") == 1, f"Found more than one statement in the specified SQL string:\n{statement}"
        self.statements.append(statement)
    
    def update_statement(self, statement, username, validate):
        import re
        
        db_name = None if username is None else self.to_db_name(username, self.naming_template, self.naming_params)
        
        if db_name is not None:
            # Replace db_name only if db_name (and thus username) was specified
            statement = statement.replace("{db_name}", db_name)
            
        # Update the working_dir variable
        working_dir = f"dbfs:/mnt/dbacademy-users/{username}/{DA.course_code}"
        statement = statement.replace("{working_dir}", DA.paths.working_dir).strip()

        # Update the username variable
        if username is not None:
            statement = statement.replace("{username}", username).strip()

        test = re.search(r"{\s*[\w\.]+\s*}", statement)
        if validate and test is not None: 
            raise Exception(f"Found existing mustache template: {test}")
        
        return statement.strip()
    
    def to_html(self, username, validate=True):
        sql = ""
        for i, statement in enumerate(self.statements):
            sql += self.update_statement(statement, username, validate=validate)
            sql += "\n"
            
        sql = sql.strip()
        row_count = len(sql.split("\n"))
        
        html = f"""<div id="{self.id}-wrapper" style="width:{self.widthpx}">"""
        if self.instructions is not None:
            instructions = self.instructions
            if "{step}" in instructions: instructions = instructions.format(step=self.number)
            html += f"""<div id="{self.id}-instruction" style="margin-bottom:1em">{instructions}</div>"""
        
        html += f"""
        <div style="width:{self.widthpx};{" display: none;" if sql == "" else ""}">
            <textarea id="{self.id}-ta" style="width:{self.width-20}px; padding:10px" rows="{row_count}">{sql}</textarea>
            <textarea id="{self.id}-backup" style="display:none;">{sql}</textarea>
        </div>
        <div style="width:{self.widthpx}; text-align:right;{" display: none;" if sql == "" else ""}">
            <button id="{self.id}-btn" type="button"  onclick="
                let ta = document.getElementById('{self.id}-ta');
                ta.select();
                ta.setSelectionRange(0, ta.value.length);
                navigator.clipboard.writeText(ta.value);">Copy</button>
        </div>
        """
        
        return html+"</div>"
    
    def render(self, username):
        displayHTML(self.to_html(username))
    
    def execute(self, username):
        db_name = self.to_db_name(username, self.naming_template, self.naming_params)
        for statement in self.statements:
            if len(statement) == 0:
                print(f"")
            else:
                statement = self.update_statement(statement, username, validate=True)
                print(f"Executing:\n{statement}")
                print("-"*80)
                display(spark.sql(statement))
                
    @staticmethod
    def to_db_name(username, naming_template, naming_params):
        import re
        if "{da_hash}" in naming_template:
            assert naming_params.get("course", None) is not None, "The template is employing da_hash which requires course to be specified in naming_params"
            course = naming_params["course"]
            da_hash = abs(hash(f"{username}-{course}")) % 10000
            naming_params["da_hash"] = da_hash

        naming_params["da_name"] = username.split("@")[0]
        db_name = naming_template.format(**naming_params)    
        return re.sub("[^a-zA-Z0-9]", "_", db_name)

# COMMAND ----------

class Validation():
    def __init__(self, number:int, instructions:str, label:str, expected:str, length:int, naming_template:str, naming_params:dict):
        
        assert type(number) == int, f"Expected the parameter \"number\" to be of type int, found {type(number)}"
        assert instructions is None or type(instructions) == str, f"Expected the parameter \"instructions\" to be of type str, found {type(instructions)}"
        assert label is None or type(label) == str, f"Expected the parameter \"label\" to be of type str, found {type(label)}"
        assert expected is None or type(expected) == str, f"Expected the parameter \"expected\" to be of type str, found {type(expected)}"
        assert type(length) == int, f"Expected the parameter \"length\" to be of type str, found {type(length)}"

        assert type(naming_template) == str, f"Expected the parameter \"naming_template\" to be of type str, found {type(naming_template)}"
        assert type(naming_params) == dict, f"Expected the parameter \"naming_params\" to be of type dict, found {type(naming_params)}"

        self.width = 800
        self.widthpx = f"{self.width}px"
        
        self.number = number
        self.id = f"step-{number}"
        self.instructions = instructions
        self.label = label
        self.expected = expected
        self.length = length

        self.naming_params = naming_params
        self.naming_template = naming_template
        
        self.statements = []
        
    def add(self, statement):
        assert statement is not None, "The statement must be specified"
        statement = statement.strip()
        if statement != "":
            assert statement.endswith(";"), f"Exepcted statement to end with a semi-colon:\n{statement}"
            assert statement.count(";") == 1, f"Found more than one statement in the specified SQL string:\n{statement}"
        self.statements.append(statement)
    
    def to_html(self, username, validate=True):
        sql = ""
        db_name = None if username is None else self.to_db_name(username, self.naming_template, self.naming_params)
        for i, statement in enumerate(self.statements):
            if db_name is None: sql += statement.strip()
            else: sql += statement.replace("{db_name}", db_name).strip()
            sql += "\n"
        sql = sql.strip()
        row_count = len(sql.split("\n"))
        
        html = f"""<div id="{self.id}-wrapper" style="width:{self.widthpx}">"""
        if self.instructions is not None:
            instructions = self.instructions
            if "{step}" in instructions: instructions = instructions.format(step=self.number)
            html += f"""<div id="{self.id}-instruction" style="margin-bottom:1em">{instructions}</div>"""
        
        html += f"""
        <div width="100%">
            <table width="100%">
            <tbody class="main_table">
            <tr>
                <td style="width: 75%; text-align:left;">{self.label} </td>
                <td style="width: 25%; text-align:center;"><input type="text" onchange="answerIs(this, ['{self.expected}']);" style="background-color: rgb(255, 255, 255);">
            </td>
            </tr>    
            </tbody>
            </table>
        </div>
        """
        
        html += f"""
        <div style="width:{self.widthpx}; display: none;">
            <textarea id="{self.id}-ta" style="width:{self.width-20}px; padding:10px" rows="{row_count}">{sql}</textarea>
            <textarea id="{self.id}-backup" style="display:none;">{sql}</textarea>
        </div>
        <div style="width:{self.widthpx}; text-align:right; display: none;">
            <button id="{self.id}-btn" type="button"  onclick="
                let ta = document.getElementById('{self.id}-ta');
                ta.select();
                ta.setSelectionRange(0, ta.value.length);
                navigator.clipboard.writeText(ta.value);">Copy</button>
        </div>
        """
        
        return html+"</div>"
    
    def render(self, username):
        displayHTML(self.to_html(username))
    
    def execute(self, username):
        db_name = self.to_db_name(username, self.naming_template, self.naming_params)
        for statement in self.statements:
            if len(statement) == 0:
                print(f"")
            else:
                statement = statement.replace("{db_name}", db_name)
                print(f"Executing: {statement}")
                display(spark.sql(statement))
                
    @staticmethod
    def to_db_name(username, naming_template, naming_params):
        import re
        if "{da_hash}" in naming_template:
            assert naming_params.get("course", None) is not None, "The template is employing da_hash which requires course to be specified in naming_params"
            course = naming_params["course"]
            da_hash = abs(hash(f"{username}-{course}")) % 10000
            naming_params["da_hash"] = da_hash

        naming_params["da_name"] = username.split("@")[0]
        db_name = naming_template.format(**naming_params)    
        return re.sub("[^a-zA-Z0-9]", "_", db_name)

# COMMAND ----------

def install_toolbox(self):
    target_repo = f"/Repos/{DA.username}/dbacademy-toolbox"
    print(f"Installing \"DBAcademy Toolbox\" to \"{target_repo}\"")

    status = DA.client.workspace().get_status(target_repo)
    if status is not None:
        print(f" - Removing existing instance")
        repo_id = status["object_id"]
        DA.client.repos().delete(repo_id)

    DA.client.repos().create(target_repo, url="https://github.com/databricks-academy/dbacademy-toolbox.git")
    print(f" - Installed succesfully")
    
DBAcademyHelper.monkey_patch(install_toolbox)

# COMMAND ----------

def print_instructions(self):
    from dbacademy import dbgems
    
    displayHTML(f"""
    <h1>Setup Steps</h1>
    <ol>
        <li>Configure User Permissions</li>
        <ol>
            <li>Open the <a href="/?o={dbgems.get_workspace_id()}#setting/accounts" target="_blank">Admin Console</a> from the <b>Data Science & Engineering</b> view</li>
            <li>Select the <a href="/?o={dbgems.get_workspace_id()}#setting/accounts/groups" target="_blank">Groups</a> tab
            <li>Select the <b>users</b> group
            <li>Select the <b>Entitlements</b> tab
            <li>Select only the <b>Databricks SQL access</b> entitlement
        </ol><br/>
        <li>Open the notebook <a href="/#workspace/Repos/{DA.username}/dbacademy-toolbox/DBAcademy-Toolbox/Classroom/Manage Datasets?o={dbgems.get_workspace_id()}" target="_blank">Manage Datasets</a> from the toolbox</li>
        <ol>
            <li>Click <b>Run All</b> to initialize the notebook</li>
            <li>From the drop-down menu select the course <b>Data Analysis with Databricks</b>.</li>
            <li>Execute the task <b>Install Dataset</b></li>
        </ol><br/>

        <li>Open the notebook <a href="/#workspace/Repos/{DA.username}/dbacademy-toolbox/DBAcademy-Toolbox/Classroom/Manage SQL Endpoints?o={dbgems.get_workspace_id()}" target="_blank">Manage SQL Endpoints</a> from the toolbox</li>
        <ol>
            <li>Click <b>Run All</b> to initialize the notebook</li>
            <li>From the drop-down menu select the course <b>Data Analysis with Databricks</b>.</li>
            <li>Execute the task <b>Create SQL Endpoints</b>
            <li>Optionally, execute the task <b>Start SQL Endpoint</b>
        </ol><br/>

        <li>Open the notebook <a href="/#workspace/Repos/{DA.username}/dbacademy-toolbox/DBAcademy-Toolbox/Classroom/Manage Databases?o={dbgems.get_workspace_id()}" target="_blank">Manage Databases</a> from the toolbox</li>
        <ol>
            <li>Click <b>Run All</b> to initialize the notebook</li>
            <li>From the drop-down menu select the course <b>Data Analysis with Databricks</b>.</li>
            <li>Execute the task <b>Create Course-Specific Databases</b>
            <li>Execute the task <b>Grant Privileges</b> which requires opening the query in Databricks SQL to complete the operation.
        </ol><br/>

    </ol>
    """)    
    
DBAcademyHelper.monkey_patch(print_instructions)

# COMMAND ----------

def preload_student_databases(self):
    databases = [r[0] for r in spark.sql("SHOW DATABASES").collect()]

    for username in DA.usernames:
        db_name = Step.to_db_name(username, DA.naming_template, DA.naming_params)
        if db_name not in databases:
            print(f"Skipping creation of tables for \"{username}\"")
        else:
            print(f"Creating the table \"{db_name}.flight_delays\" for \"{username}\"")
            spark.sql(f"""DROP TABLE IF EXISTS {db_name}.temp_delays;""")
            spark.sql(f"""DROP TABLE IF EXISTS {db_name}.flight_delays;""")
            spark.sql(f"""CREATE TABLE {db_name}.temp_delays USING CSV OPTIONS (path "dbfs:/mnt/dbacademy-datasets/{self.data_source_name}/flights/departuredelays.csv", header "true", inferSchema "true");""")
            spark.sql(f"""CREATE TABLE {db_name}.flight_delays AS SELECT * FROM {db_name}.temp_delays;""")
            spark.sql(f"""DROP TABLE {db_name}.temp_delays;""")

            print(f"Creating the table \"{db_name}.sales\" for \"{username}\"")
            spark.sql(f"""DROP TABLE IF EXISTS {db_name}.sales;""")
            spark.sql(f"""CREATE TABLE {db_name}.sales AS
      SELECT * FROM delta.`wasbs://courseware@dbacademy.blob.core.windows.net/data-analysis-with-databricks/v01/sales`;""")

            print(f"Creating the table \"{db_name}.promo_prices\" for \"{username}\"")
            spark.sql(f"""DROP TABLE IF EXISTS {db_name}.promo_prices;""")
            spark.sql(f"""CREATE TABLE {db_name}.promo_prices AS
      SELECT * FROM delta.`wasbs://courseware@dbacademy.blob.core.windows.net/data-analysis-with-databricks/v01/promo_prices`;""")

            # TODO - Convert underlying source to Delta
            print(f"Creating the table \"{db_name}.sales_orders\" for \"{username}\"")
            spark.sql(f"""DROP TABLE IF EXISTS {db_name}.sales_orders;""")
            spark.sql(f"""CREATE TABLE {db_name}.sales_orders AS
      SELECT * FROM json.`wasbs://courseware@dbacademy.blob.core.windows.net/data-analysis-with-databricks/v01/sales_orders`;""")

            # TODO - Convert underlying source to Delta
            print(f"Creating the table \"{db_name}.loyalty_segments\" for \"{username}\"")
            spark.sql(f"""DROP TABLE IF EXISTS {db_name}.temp_loyalty_segments;""")
            spark.sql(f"""DROP TABLE IF EXISTS {db_name}.loyalty_segments;""")
            spark.sql(f"""CREATE TABLE {db_name}.temp_loyalty_segments USING CSV OPTIONS (path "dbfs:/mnt/dbacademy-datasets/{self.data_source_name}/loyalty_segments/loyalty_segment.csv", header "true", inferSchema "true");""")
            spark.sql(f"""CREATE TABLE {db_name}.loyalty_segments AS SELECT * FROM {db_name}.temp_loyalty_segments;""")
            spark.sql(f"""DROP TABLE {db_name}.temp_loyalty_segments;""")

            # TODO - Convert underlying source to Delta
            print(f"Creating the table \"{db_name}.customers\" for \"{username}\"")
            spark.sql(f"""DROP TABLE IF EXISTS {db_name}.temp_customers;""")
            spark.sql(f"""DROP TABLE IF EXISTS {db_name}.customers;""")
            spark.sql(f"""CREATE TABLE {db_name}.temp_customers USING CSV OPTIONS (path "dbfs:/mnt/dbacademy-datasets/{self.data_source_name}/customers/customers.csv", header "true", inferSchema "true");""")
            spark.sql(f"""CREATE TABLE {db_name}.customers AS SELECT * FROM {db_name}.temp_customers;""")
            spark.sql(f"""DROP TABLE {db_name}.temp_customers;""")

            # TODO - Convert underlying source to Delta
            print(f"Creating the table \"{db_name}.suppliers\" for \"{username}\"")
            spark.sql(f"""DROP TABLE IF EXISTS {db_name}.temp_suppliers;""")
            spark.sql(f"""DROP TABLE IF EXISTS {db_name}.suppliers;""")
            spark.sql(f"""CREATE TABLE {db_name}.temp_suppliers USING CSV OPTIONS (path "dbfs:/mnt/dbacademy-datasets/{self.data_source_name}/suppliers/suppliers.csv", header "true", inferSchema "true");""")
            spark.sql(f"""CREATE TABLE {db_name}.suppliers AS SELECT * FROM {db_name}.temp_suppliers;""")
            spark.sql(f"""DROP TABLE {db_name}.temp_suppliers;""")

            # TODO - Convert underlying source to Delta
            print(f"Creating the table \"{db_name}.source_suppliers\" for \"{username}\"")
            spark.sql(f"""DROP TABLE IF EXISTS {db_name}.temp_suppliers;""")
            spark.sql(f"""DROP TABLE IF EXISTS {db_name}.source_suppliers;""")
            spark.sql(f"""CREATE TABLE {db_name}.temp_suppliers USING CSV OPTIONS (path "dbfs:/mnt/dbacademy-datasets/{self.data_source_name}/suppliers/suppliers.csv", header "true", inferSchema "true");""")
            spark.sql(f"""CREATE TABLE {db_name}.source_suppliers AS SELECT * FROM {db_name}.temp_suppliers;""")
            spark.sql(f"""DROP TABLE {db_name}.temp_suppliers;""")

            # Leave underlying data as JSON for the COPY INTO lab
            print(f"Creating the table \"{db_name}.gym_logs\" for \"{username}\"")
            spark.sql(f"""CREATE OR REPLACE TABLE {db_name}.gym_logs (first_timestamp DOUBLE, gym Long, last_timestamp DOUBLE, mac STRING);""")
            spark.sql(f"""COPY INTO {db_name}.gym_logs FROM 'wasbs://courseware@dbacademy.blob.core.windows.net/data-analysis-with-databricks/v01/gym-logs' FILEFORMAT = JSON FILES = ('20191201_2.json');""")
            spark.sql(f"""COPY INTO {db_name}.gym_logs FROM 'wasbs://courseware@dbacademy.blob.core.windows.net/data-analysis-with-databricks/v01/gym-logs' FILEFORMAT = JSON FILES = ('20191201_3.json');""")        

DBAcademyHelper.monkey_patch(preload_student_databases)

# COMMAND ----------

def create_sql_endpoints(self):
    from dbacademy.dbrest.sql.endpoints import CLUSTER_SIZE_2X_SMALL
    
    self.client.sql().endpoints().create_user_endpoints(naming_template=self.naming_template,  # Required
                                                        naming_params=self.naming_params,      # Required
                                                        cluster_size = CLUSTER_SIZE_2X_SMALL,  # Required
                                                        enable_serverless_compute = False,     # Required
                                                        tags = {                                     
                                                            "dbacademy.course": "data-analysis-with-databricks",  # Tag the name of the course
                                                            "dbacademy.source": "data-analysis-with-databricks"   # Tag the name of the course
                                                        },  
                                                        users=self.usernames)                       # Restrict to the specified list of users

DBAcademyHelper.monkey_patch(create_sql_endpoints)

# COMMAND ----------

def create_user_specific_databases(self):
    for username in self.usernames:
        db_name = Step.to_db_name(username=username, naming_template=self.naming_template, naming_params=self.naming_params)
        db_path = f"dbfs:/mnt/dbacademy-users/{username}/{self.course_code}/database.db"

        print(f"Creating the database \"{db_name}\"\n   for \"{username}\" \n   at \"{db_path}\"\n")
        spark.sql(f"DROP DATABASE IF EXISTS {db_name} CASCADE;")
        spark.sql(f"CREATE DATABASE {db_name} LOCATION '{db_path}';")
        
DBAcademyHelper.monkey_patch(create_user_specific_databases)

# COMMAND ----------

def create_user_specific_grants(self):
    from dbacademy import dbgems
    
    sql = ""
    for username in self.usernames:
        db_name = Step.to_db_name(username=username, naming_template=self.naming_template, naming_params=self.naming_params)
        sql += f"GRANT ALL PRIVILEGES ON DATABASE `{db_name}` TO `{username}`;\n"
        sql += f"GRANT ALL PRIVILEGES ON ANY FILE TO `{username}`;\n"
        sql += f"ALTER DATABASE {db_name} OWNER TO `{username}`;\n"    
        sql += "\n"

    query_name = f"Instructor - Grant All Users - Data Analysis with Databricks"
    query = self.client.sql().queries().get_by_name(query_name)
    if query is not None:
        self.client.sql().queries().delete_by_id(query.get("id"))

    query = self.client.sql().queries().create(name=query_name, 
                                          query=sql[0:-1], 
                                          description="Grants the required access for all users to the databases for the course {course}",
                                          schedule=None, options=None, data_source_id=None)
    query_id = query.get("id")
    displayHTML(f"""Query created - follow this link to execute the grants in Databricks SQL</br></br>
                    <a href="/sql/queries/{query_id}/source?o={dbgems.get_workspace_id()}" target="_blank">{query_name}</a>""")
    
DBAcademyHelper.monkey_patch(create_user_specific_grants)

