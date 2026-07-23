import os
import argparse
import json
import time
import hashlib
import snowflake.connector
from utility import get_modified_files, get_account_modified_files, extract_env, replace_warehouse_name, update_warehouse_size, revert_warehouse_size

# Set a few global variables here
_snowchange_version = '2.2.0'
_metadata_schema_name = 'DEPLOY'
_metadata_table_name = 'CHANGE_HISTORY'
_metadata_buildinfo_table_name = 'BUILD_INFORMATION'

orderfile = "order_file.txt"


env_dict_CO_SHARED = {"dev":"","prd":"","tst":""}
env_db_list_CO_SHARED = ["","",""]


exclude_files = ["sp_clone_from_prod_to_preprod.sql", "sp_clone_from_prod_to_devtest.sql"]


env_stage_list = {"COEDW_DEV":"@STAGE.DEV_CSV_STAGE","COEDW":"@STAGE.PRD_CSV_STAGE","COEDW_TEST":"@STAGE.TST_CSV_STAGE","COEDW_PREPROD":"@STAGE.UAT_CSV_STAGE"}

# Map environment to warehouse names
env_dict_warehouse = {
  "dev": {"ELT_DEV_TEST": "ELT_DEV_TEST", "ELT": "ELT_DEV_TEST"},
  "tst": {"ELT_DEV_TEST": "ELT_DEV_TEST", "ELT": "ELT_DEV_TEST"},
  "preprod": {"ELT_DEV_TEST": "ELT", "ELT": "ELT"},
  "prd": {"ELT_DEV_TEST": "ELT", "ELT": "ELT"}
  }


def get_snowflake_connection():
    snowflake_connection = snowflake.connector.connect(
      user=os.environ["SNOWFLAKE_USER"],
      account=os.environ["SNOWFLAKE_ACCOUNT"],
      role=os.environ["SNOWFLAKE_ROLE"],
      warehouse=os.environ["SNOWFLAKE_WAREHOUSE"],
      database=os.environ["SNOWFLAKE_DATABASE"],
      authenticator=os.environ["SNOWFLAKE_AUTHENTICATOR"],
      password=os.environ["SNOWSQL_PWD"]
    )
    return snowflake_connection


def snowchange(root_folder, snowflake_account, snowflake_user, snowflake_role, snowflake_warehouse, snowflake_database, change_history_table_override, build_id, build_start_time, vars, autocommit, verbose, account_level_file, pipeline_name, database_environment, build_info_table, last_success_build_id, current_head, access_token, repository_id, deployment_warehouse_size_dict):
    if "SNOWSQL_PWD" not in os.environ:
        raise ValueError("The SNOWSQL_PWD environment variable has not been defined")

    root_folder = os.path.abspath(root_folder)
    if not os.path.isdir(root_folder):
        raise ValueError("Invalid root folder: %s" % root_folder)

    print("snowchange version: %s" % _snowchange_version)
    print("Using root folder %s" % root_folder)
    print("Using variables %s" % vars)
    print("Using Snowflake account %s" % snowflake_account)
    print("Using Snowflake user %s" % snowflake_user)
    print("Using Snowflake role %s" % snowflake_role)
    print("Using Snowflake warehouse %s" % snowflake_warehouse)
    print("Using Snowflake database %s" % snowflake_database)

    # TODO: Is there a better way to do this without setting environment variables?
    os.environ["SNOWFLAKE_ACCOUNT"] = snowflake_account
    os.environ["SNOWFLAKE_USER"] = snowflake_user
    os.environ["SNOWFLAKE_ROLE"] = snowflake_role
    os.environ["SNOWFLAKE_WAREHOUSE"] = snowflake_warehouse
    os.environ["SNOWFLAKE_DATABASE"] = snowflake_database
    os.environ["SNOWFLAKE_AUTHENTICATOR"] = 'snowflake'

    # Get desired size from mapping
    deployment_warehouse_size = deployment_warehouse_size_dict.get(database_environment)

    if deployment_warehouse_size:
        original_size, size_changed = update_warehouse_size(
          user=snowflake_user,
          account=snowflake_account,
          role="CO_ADMIN",
          warehouse=snowflake_warehouse,
          database=snowflake_database,
          authenticator='snowflake',
          password=os.environ["SNOWSQL_PWD"],
          deployment_warehouse_size=deployment_warehouse_size
          )
    else:
        print(f"No warehouse size mapping found for environment '{database_environment}'. Skipping warehouse size update.")
        original_size = None
        size_changed = False
        print("Getting Snowflake Connection")

    snowflake_connection = get_snowflake_connection()

    scripts_applied = 0
    scripts_skipped = 0

    # Get the change history table details
    change_history_table = get_change_history_table_details(change_history_table_override)

    # Get build information table details
    buildid_info_table = get_build_information_table_details(build_info_table)

    # Find all scripts in the root folder (recursively) and sort them correctly
    if account_level_file == "1":
        all_v_scripts, all_r_scripts = get_all_scripts_recursively_account(root_folder, verbose, last_success_build_id, current_head, access_token, buildid_info_table, snowflake_connection, autocommit, repository_id, account_level_file, pipeline_name)
    else:
        all_v_scripts, all_r_scripts = get_all_scripts_recursively_coedw(root_folder, verbose, last_success_build_id, current_head, access_token, buildid_info_table, snowflake_connection, autocommit, repository_id, account_level_file, pipeline_name)

    try:

        if len(all_v_scripts) > 0:
            print('.....')
            print("V Scripts modified since last build")
            for key in range(len(all_v_scripts)):
                print(all_v_scripts[key]['script_name'])
        else:
            print("There are no V Scripts modified since the last build")
    except KeyError:
        print(all_v_scripts)

    try:
        if len(all_r_scripts) > 0:
            print('.....')
            print("R Scripts modified since last build")
            for key in range(len(all_r_scripts)):
                print(all_r_scripts[key]['script_name'])
        else:
            print("There are no R Scripts modified since the last build")
    except KeyError:
        print(all_r_scripts)

    # Loop through each script in order and apply any required changes (only to versioned scripts)
    if account_level_file == "1":
        for script in all_v_scripts.items():
            script_to_be_applied = script[1]
            if apply_change_script(snowflake_connection, script_to_be_applied, vars, change_history_table, autocommit, verbose, build_id, build_start_time, pipeline_name, database_environment):
              scripts_applied += 1
            else:
              scripts_skipped += 1
              
    else:
        for i in range(len(all_v_scripts)):
            script_to_be_applied = all_v_scripts[i]
            if apply_change_script(snowflake_connection, script_to_be_applied, vars, change_history_table, autocommit, verbose, build_id, build_start_time, pipeline_name, database_environment):
              scripts_applied += 1
            else:
              scripts_skipped += 1

    print(".....")

    # Loop through each script in order and apply any required changes (only to non-versioned scripts)
    for script in all_r_scripts.items():
        script_to_be_applied = script[1]
        if apply_change_script(snowflake_connection, script_to_be_applied, vars, change_history_table, autocommit, verbose, build_id, build_start_time, pipeline_name, database_environment):
          scripts_applied += 1
        else:
          scripts_skipped += 1

    if bool(all_r_scripts) == True or bool(all_v_scripts) == True:
        print("Doing post update task of adding build information to the DB ... ")
        update_build_info_table(snowflake_connection, buildid_info_table, autocommit, verbose, current_head, pipeline_name, build_start_time, all_r_scripts, all_v_scripts)

    if size_changed:
        revert_warehouse_size(
          user=snowflake_user,
          account=snowflake_account,
          role="CO_ADMIN",
          warehouse=snowflake_warehouse,
          database=snowflake_database,
          authenticator='snowflake',
          password=os.environ["SNOWSQL_PWD"],
          original_size=original_size,
          size_changed=size_changed
          )
    else:
        print("No warehouse size change detected. Revert not required.")

    print("Successfully applied %d change script(s)." % (scripts_applied))
    print(f"Skipped {scripts_skipped} script(s).")
    print("Closing Snowflake Connection")
    snowflake_connection.close()
    print("Completed successfully")


def get_all_scripts_recursively_coedw(root_directory, verbose, last_success_build_id, current_head, access_token, buildid_info_table, snowflake_connection, autocommit, repository_id, account_level_file, pipeline_name):
    return get_modified_files(
      current_head=current_head,
      snowflake_connection=snowflake_connection,
      autocommit=autocommit,
      verbose=verbose,
      buildid_info_table=buildid_info_table,
      execute_snowflake_query=execute_snowflake_query,
      root_directory=root_directory,
      access_token=access_token,
      orderfile=orderfile,
      last_success_build_id=last_success_build_id,
      repository_id=repository_id,
      folder_path='/coEDW/',
      account_level_file=account_level_file,
      pipeline_name=pipeline_name
      )


def get_all_scripts_recursively_account(root_directory, verbose, last_success_build_id, current_head, access_token, buildid_info_table, snowflake_connection, autocommit, repository_id, pipeline_name):
    return get_account_modified_files(
          current_head=current_head,
          snowflake_connection=snowflake_connection,
          autocommit=autocommit,
          verbose=verbose,
          buildid_info_table=buildid_info_table,
          execute_snowflake_query=execute_snowflake_query,
          root_directory=root_directory,
          access_token=access_token,
          last_success_build_id=last_success_build_id,
          repository_id=repository_id,
          pipeline_name=pipeline_name
      )


def get_build_information_table_details(build_info_table):
    # Start with the global defaults
    build_details = dict()
    build_details['database_name'] = os.environ["SNOWFLAKE_DATABASE"]
    build_details['schema_name'] = _metadata_schema_name.upper()
    build_details['buildinfo_table_name'] = _metadata_buildinfo_table_name.upper()

    # Then override the defaults if needed. The name could be in one, two or three parts notation.
    if build_info_table:
        table_parts = build_info_table.strip().split('.')

        if len(table_parts) == 1:
            build_details['buildinfo_table_name'] = table_parts[0].upper()
        elif len(table_parts) == 2:
            build_details['buildinfo_table_name'] = table_parts[1].upper()
            build_details['schema_name'] = table_parts[0].upper()
        elif len(table_parts) == 3:
            build_details['buildinfo_table_name'] = table_parts[2].upper()
            build_details['schema_name'] = table_parts[1].upper()
            build_details['database_name'] = table_parts[0].upper()
        else:
            raise ValueError("Invalid buildinfo table name: %s" % build_info_table)
    return build_details


def execute_snowflake_query(snowflake_connection, query, autocommit, verbose):

  external_stage = env_stage_list['COEDW_DEV']
  external_stage_rpl = external_stage
  db = os.environ.get("SNOWFLAKE_DATABASE")

  if db and db in env_stage_list:
    external_stage_rpl = env_stage_list[db]

  if external_stage in query and external_stage_rpl:
    print(f"execute_snowflake_query()- The db is: {db}, replacing  {external_stage} in query with {external_stage_rpl}")
    query = query.replace(external_stage, external_stage_rpl)
    print(f"execute_snowflake_query()- replaced query with : {external_stage_rpl}")

  if not autocommit:
    snowflake_connection.autocommit(False)

  try:
    snowflake_connection.execute_string(f"USE DATABASE {db}")
    res = snowflake_connection.execute_string(query)
    if not autocommit:
      snowflake_connection.commit()
    return res
  except Exception as e:
    if not autocommit:
      snowflake_connection.rollback()
    raise e

def get_change_history_table_details(change_history_table_override):
  # Start with the global defaults
  details = dict()
  details['database_name'] = os.environ["SNOWFLAKE_DATABASE"]
  details['schema_name'] = _metadata_schema_name.upper()
  details['table_name'] = _metadata_table_name.upper()

  # Then override the defaults if requested. The name could be in one, two or three part notation.
  if change_history_table_override is not None:
    table_name_parts = change_history_table_override.strip().split('.')

    if len(table_name_parts) == 1:
      details['table_name'] = table_name_parts[0].upper()
    elif len(table_name_parts) == 2:
      details['table_name'] = table_name_parts[1].upper()
      details['schema_name'] = table_name_parts[0].upper()
    elif len(table_name_parts) == 3:
      details['table_name'] = table_name_parts[2].upper()
      details['schema_name'] = table_name_parts[1].upper()
      details['database_name'] = table_name_parts[0].upper()
    else:
      raise ValueError("Invalid change history table name: %s" % change_history_table_override)

  return details


def execute_and_record_change(snowflake_connection, script, vars, change_history_table, autocommit, verbose, build_id, build_start_time, pipeline_name, database_environment):

  # First read the contents of the script
  with open(script['script_full_path'],'r') as content_file:
    filename = script['script_full_path'].split('/')[-1]  # Extract the filename from the full path
    content = content_file.read().strip()
    content = content[:-1] if content.endswith(';') else content
    if filename not in exclude_files:
      content = replace_env(content,database_environment)
 
  # Define a few other change related variables
  checksum = hashlib.sha224(content.encode('utf-8')).hexdigest()
  execution_time = 0
  status = 'Success'  

  # Execute the contents of the script
  if len(content) > 0:
    start = time.time()
    execute_snowflake_query(snowflake_connection, content, autocommit, verbose)
    end = time.time()
    execution_time = round(end - start)

  # Finally record this change in the change history table
  query = """INSERT INTO {0}.{1}.{2} (BUILD_ID, BUILD_START_TIME, DESCRIPTION, SCRIPT, SCRIPT_TYPE, CHECKSUM, EXECUTION_TIME, STATUS, INSTALLED_BY, INSTALLED_ON, SCRIPT_PATH, PIPELINE_NAME) 
           values ('{3}',to_timestamp_ntz('{4}', 'yyyymmddhh24miss'),'{5}','{6}','{7}','{8}','{9}','{10}', '{11}', CURRENT_TIMESTAMP, '{12}', '{13}');""".format(change_history_table['database_name'], 
                                                                                                  change_history_table['schema_name'], 
                                                                                                  change_history_table['table_name'], 
                                                                                                  build_id,
                                                                                                  build_start_time,
                                                                                                  script['script_description'], 
                                                                                                  script['script_name'], 
                                                                                                  script['script_type'], 
                                                                                                  checksum, 
                                                                                                  execution_time, 
                                                                                                  status, 
                                                                                                  os.environ["SNOWFLAKE_USER"],
                                                                                                  script['script_full_path'],
                                                                                                  pipeline_name
                                                                                                 )

  execute_snowflake_query(snowflake_connection, query, autocommit, verbose)


def apply_change_script(snowflake_connection, script, vars, change_history_table, autocommit, verbose, build_id, build_start_time, pipeline_name, database_environment):
    script_name = script['script_name']

    # Extract environment values from the script name
    env_values = extract_env(script_name)

    # Check if there are environment values to process
    if (env_values is None) or (env_values and any(database_environment == value for value in env_values)):
      print("Applying change script %s" % script['script_full_path'])
      execute_and_record_change(snowflake_connection, script, vars, change_history_table, autocommit, verbose, build_id, build_start_time, pipeline_name, database_environment)
      return True  # Script applied successfully
    else:
      print(f"Skipping change script {script['script_full_path']}")
      return False  # Script skipped

def replace_env(content, database_environment):

  env_db_lakehouse_replace = ""
  env_db_coedw_replace = ""
  env_db_system_integration_replace = ""
  env_db_CO_DATASCIENCELAB_replace = ""
  env_db_CO_PLANDATA_replace = ""
  env_db_CO_SHARED_replace = ""

  if database_environment and database_environment.strip() != "":
      database_environment = database_environment.strip() 
      env_db_lakehouse_replace = env_dict_lakehouse.get(database_environment)
      env_db_coedw_replace = env_dict_coedw.get(database_environment) 
      env_db_system_integration_replace = env_dict_system_integration.get(database_environment)
      env_db_CO_DATASCIENCELAB_replace = env_dict_CO_DATASCIENCELAB.get(database_environment)
      env_db_CO_PLANDATA_replace = env_dict_CO_PLANDATA.get(database_environment)
      env_db_CO_SHARED_replace = env_dict_CO_SHARED.get(database_environment)

  #Replace for LAKEHOUSE database,if exists
  for env_db in env_db_list_lakehouse:
    if (env_db != env_db_lakehouse_replace) and (f"{env_db}." in content):
      if env_db_lakehouse_replace and env_db_lakehouse_replace.strip() != "":
        content = content.replace(f"{env_db}.",(f"{env_db_lakehouse_replace.strip()}."))
        print(f"replace_env()- {env_db} exists, replaced with {env_db_lakehouse_replace}")

  # Replace for COEDW database, if exists
  for env_db in env_db_list_coedw:
    if (env_db != env_db_coedw_replace) and (f"{env_db}." in content) and (f"{env_db}" !="COEDW_PREPROD"):       
      if env_db_coedw_replace and env_db_coedw_replace.strip() != "":
        content = content.replace(f"{env_db}.", f"{env_db_coedw_replace.strip()}.")
        print(f"replace_env()- {env_db} exists, replaced with {env_db_coedw_replace}")

  #Replace for system_integration database,if exists
  for env_db in env_db_list_system_integration:
    if (env_db != env_db_system_integration_replace) and (f"{env_db}." in content):
      if env_db_system_integration_replace and env_db_system_integration_replace.strip() != "":
        content = content.replace(f"{env_db}.",(f"{env_db_system_integration_replace.strip()}."))
        print(f"replace_env()- {env_db} exists, replaced with {env_db_system_integration_replace}")

  #Replace for CO_DATASCIENCELAB database,if exists
  for env_db in env_db_list_CO_DATASCIENCELAB:
    if (env_db != env_db_CO_DATASCIENCELAB_replace) and (f"{env_db}." in content):
        if env_db_CO_DATASCIENCELAB_replace and env_db_CO_DATASCIENCELAB_replace.strip() != "":
          content = content.replace(f"{env_db}.",(f"{env_db_CO_DATASCIENCELAB_replace.strip()}."))
          print(f"replace_env()- {env_db} exists, replaced with {env_db_CO_DATASCIENCELAB_replace}")

  #Replace for CO_PLANDATA database,if exists
  for env_db in env_db_list_CO_PLANDATA:
    if (env_db != env_db_CO_PLANDATA_replace) and (f"{env_db}." in content):
        if env_db_CO_PLANDATA_replace and env_db_CO_PLANDATA_replace.strip() != "":
          content = content.replace(f"{env_db}.",(f"{env_db_CO_PLANDATA_replace.strip()}."))
          print(f"replace_env()- {env_db} exists, replaced with {env_db_CO_PLANDATA_replace}")
  
  #Replace for CO_SHARED database,if exists
  for env_db in env_db_list_CO_SHARED:
    if (env_db != env_db_CO_SHARED_replace) and (f"{env_db}." in content):
        if env_db_CO_SHARED_replace and env_db_CO_SHARED_replace.strip() != "":
          content = content.replace(f"{env_db}.",(f"{env_db_CO_SHARED_replace.strip()}."))
          print(f"replace_env()- {env_db} exists, replaced with {env_db_CO_SHARED_replace}")

  content = replace_warehouse_name(content, env_dict_warehouse, database_environment)

  return content


def update_build_info_table(snowflake_connection, buildid_info_table, autocommit, verbose, current_head, pipeline_name, build_start_time, all_r_scripts, all_v_scripts):
    all_scripts = ''
    all_scripts_list = []
    if len(all_v_scripts) > 0:
        for scripts in all_v_scripts.values():
            all_scripts_list.append(scripts['script_name'])
    if len(all_r_scripts) > 0:
        for scripts in all_r_scripts.values():
            all_scripts_list.append(scripts['script_name'])
    all_scripts = ','.join(all_scripts_list)

    query = "INSERT INTO {0}.{1}.{2} (SUCCESSFUL_BUILD_ID, PIPELINE_NAME, DATE, SQL_SCRIPTS) VALUES('{3}','{4}', to_timestamp_ntz('{5}', 'yyyymmddhh24miss'),'{6}');".format(buildid_info_table['database_name'], buildid_info_table['schema_name'], buildid_info_table['buildinfo_table_name'], current_head, pipeline_name, build_start_time, all_scripts)
    execute_snowflake_query(snowflake_connection, query, autocommit, verbose)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(prog='python snowdeploy.py', description='Apply schema changes to a Snowflake account.', formatter_class=argparse.RawTextHelpFormatter)
    parser.add_argument('-f', '--root-folder', type=str, default=".", help='The root folder for the database change scripts')
    parser.add_argument('-a', '--snowflake-account', type=str, help='The name of the snowflake account (e.g. abc123.east-us-2.azure)', required=True)
    parser.add_argument('-u', '--snowflake-user', type=str, help='The name of the snowflake user (e.g. DEPLOYER)', required=True)
    parser.add_argument('-r', '--snowflake-role', type=str, help='The name of the role to use (e.g. DEPLOYER_ROLE)', required=True)
    parser.add_argument('-w', '--snowflake-warehouse', type=str, help='The name of the warehouse to use (e.g. DEPLOYER_WAREHOUSE)', required=True)
    parser.add_argument('-d', '--snowflake-database', type=str, help='The name of the database to use (e.g. COEDW)', required=True)
    parser.add_argument('-c', '--change-history-table', type=str, help='Used to override the default name of the change history table (e.g. SNOWCHANGE.CHANGE_HISTORY)', required=True)
    parser.add_argument('-b', '--build-id', type=str, help='Id of the current build', required=True)
    parser.add_argument('-t', '--build-start-time', type=str, help='Start time of the current build (format - yyyymmddhh24miss)', required=True)
    parser.add_argument('--vars', type=json.loads, help='Define values for the variables to replaced in change scripts, given in JSON format (e.g. {"variable1": "value1", "variable2": "value2"})', required=False)
    parser.add_argument('-ac', '--autocommit', action='store_true')
    parser.add_argument('-v', '--verbose', action='store_true')
    parser.add_argument('-al', '--account_level_file', type=str, help='It helps to know files are account level or not', required=False)
    parser.add_argument('-pn', '--pipeline-name', type=str, required=False)
    parser.add_argument('-de', '--database-environment', type=str, help="Environment variable", required=False)
    parser.add_argument('-bi', '--build-info-table', type=str, help='The name of the Snowflake table for storing the build information', required=False)
    parser.add_argument('-lsi', '--last-success-build-id', type=str, help='Git commit number from the last successful build of the branch which is required for getting git diff files', required=False)
    parser.add_argument('-ch', '--current-head', type=str, help='Git commit number from the head of the branch which is required for getting git diff files', required=False)
    parser.add_argument('-st', '--access-token', type=str, help='Security access token', required=False)
    parser.add_argument('-rid', '--repository_id', type=str, help='Repository id', required=False)
    parser.add_argument('-dwhsd', '--deployment_warehouse_size_dict', type=json.loads, help='JSON dictionary mapping environments to warehouse sizes (e.g. {"dev": "SMALL", "prod": "MEDIUM"})', required=False)

    args = parser.parse_args()
    snowchange(args.root_folder, args.snowflake_account, args.snowflake_user, args.snowflake_role, args.snowflake_warehouse, args.snowflake_database, args.change_history_table, args.build_id, args.build_start_time, args.vars, args.autocommit, args.verbose, args.account_level_file, args.pipeline_name, args.database_environment, args.build_info_table, args.last_success_build_id, args.current_head, args.access_token, args.repository_id, args.deployment_warehouse_size_dict)
