# Databricks notebook source
# MAGIC %pip install \
# MAGIC git+https://github.com/databricks-academy/dbacademy-gems@c3032c2df47472f1600d368523f052d2920b406d \
# MAGIC git+https://github.com/databricks-academy/dbacademy-rest@e729b6dbb566de2958cba60fe4bd50e1b9e7f25b \
# MAGIC git+https://github.com/databricks-academy/dbacademy-helper@41a631ac489072439c49bc52d08a9229499a53f4 \
# MAGIC --quiet --disable-pip-version-check

# COMMAND ----------

# MAGIC %run ./_dataset_index

# COMMAND ----------

import time
from dbacademy_gems import dbgems
from dbacademy_helper import DBAcademyHelper, Paths

# The following attributes are externalized to make them easy
# for content developers to update with every new course.
helper_arguments = {
    "course_code" : "dawd",                               # The abreviated version of the course
    "course_name" : "data-analysis-with-databricks-sql",  # The full name of the course, hyphenated
    "data_source_name" : "data-analysis-with-databricks", # Should be the same as the course
    "data_source_version" : "v02",                        # New courses would start with 01
    "enable_streaming_support": False,                    # This couse uses stream and thus needs checkpoint directories
    "install_min_time" : "1 min",                         # The minimum amount of time to install the datasets (e.g. from Oregon)
    "install_max_time" : "5 min",                         # The maximum amount of time to install the datasets (e.g. from India)
    "remote_files": remote_files,                         # The enumerated list of files in the datasets
}

# Start a timer so we can 
# benchmark execution duration.
setup_start = int(time.time())

# COMMAND ----------

def to_dawd_database_name(self, username:str):
    return DBAcademyHelper.to_database_name(username, self.course_code)

DBAcademyHelper.monkey_patch(to_dawd_database_name)

# COMMAND ----------

class Publisher():
    def __init__(self):
        self.steps = list()
    
    def add_validation(self, include_use, instructions, test_code, statements, label, expected, length):
        # Convert single statement to the required list
        if statements is None: statements = []
        if type(statements) == str: statements = [statements]
        
        step = Validation(len(self.steps)+1, instructions, test_code, label, expected, length)
        
        if include_use:
            step.add("USE {db_name};")
        
        for statement in statements:
            step.add(statement)
            
        self.steps.append(step)
        return step
        
    def add_step(self, include_use, instructions, statements):
        # Convert single statement to the required list
        if statements is None: statements = []
        if type(statements) == str: statements = [statements]
        
        step = Step(len(self.steps)+1, instructions)
        
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
        
        if dbgems.get_cloud() != "AWS":
            print(f"Skipping publish while running {dbgems.get_cloud()}")
            return ""
        
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
                }} else if (self.value.toLowerCase().includes(expected)) {{
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
                        if (value != "n/a") {{
                            if (key == "{DA.course_code}_db_name") {{
                                document.getElementById("db_name").value = value;
                            }} else if (key == "{DA.course_code}_username") {{
                                document.getElementById("username").value = value;
                            }} else {{
                                console.log("Unknown cookie: "+key);
                            }}
                        }}
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
    def __init__(self, number:int, instructions:str):
        
        assert type(number) == int, f"Expected the parameter \"number\" to be of type int, found {type(number)}"
        assert instructions is None or type(instructions) == str, f"Expected the parameter \"instructions\" to be of type str, found {type(instructions)}"

        self.width = 800
        self.widthpx = f"{self.width}px"
        
        self.number = number
        self.id = f"step-{number}"
        self.instructions = instructions
        
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
        
        db_name = None if username is None else DA.to_dawd_database_name(username)
        
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
        db_name = DA.to_dawd_database_name(username)
        for statement in self.statements:
            if len(statement) == 0:
                print(f"")
            else:
                statement = self.update_statement(statement, username, validate=True)
                print(f"Executing:\n{statement}")
                print("-"*80)
                display(spark.sql(statement))


# COMMAND ----------

class Validation():
    def __init__(self, number:int, instructions:str, test_code:str, label:str, expected:str, length:int):
        
        assert type(number) == int, f"Expected the parameter \"number\" to be of type int, found {type(number)}"
        assert instructions is None or type(instructions) == str, f"Expected the parameter \"instructions\" to be of type str, found {type(instructions)}"
        assert test_code is None or type(test_code) == str, f"Expected the parameter \"test_code\" to be of type str, found {type(test_code)}"
        assert label is None or type(label) == str, f"Expected the parameter \"label\" to be of type str, found {type(label)}"
        assert expected is None or type(expected) == str, f"Expected the parameter \"expected\" to be of type str, found {type(expected)}"
        assert type(length) == int, f"Expected the parameter \"length\" to be of type str, found {type(length)}"

        self.width = 800
        self.widthpx = f"{self.width}px"
        
        self.number = number
        self.id = f"step-{number}"
        self.instructions = instructions
        self.test_code = test_code
        self.label = label
        self.expected = expected
        self.length = length

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
        db_name = None if username is None else DA.to_dawd_database_name(username)
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
        <div style="width:{self.widthpx};{" display: none;" if sql == "" else ""}">
            <textarea id="{self.id}-ta" style="width:{self.width-20}px; padding:10px" rows="{row_count}">{self.test_code}</textarea>
            <textarea id="{self.id}-backup" style="display:none;">{self.test_code}</textarea>
        </div>
        <div style="width:{self.widthpx}; text-align:right;{" display: none;" if sql == "" else ""}">
            <button id="{self.id}-btn" type="button"  onclick="
                let ta = document.getElementById('{self.id}-ta');
                ta.select();
                ta.setSelectionRange(0, ta.value.length);
                navigator.clipboard.writeText(ta.value);">Copy</button>
        </div>
        """
        
        html += f"""
        <div style="margin-top:20px;" width="100%">
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
        db_name = DA.to_dawd_database_name(username)
        for statement in self.statements:
            if len(statement) == 0:
                print(f"")
            else:
                statement = statement.replace("{db_name}", db_name)
                print(f"Executing: {statement}")
                display(spark.sql(statement))


