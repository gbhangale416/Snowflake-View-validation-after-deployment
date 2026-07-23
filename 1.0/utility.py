import requests
import base64
import os
from datetime import datetime
import re
import snowflake.connector


def get_incremental_changes_list(current_head, last_success_build_id, root_directory, access_token, repository_id, account_level_file, pipeline_name):

    # Construct the API Base URL
    base_url = "https://dev.azure.com/CareOregonInc/coEDW_Analytics/_apis/git/repositories"

    # Update the repositories_url with repository_id
    repositories_url = f"{base_url}/{repository_id}/diffs/commits"
    # Define the authentication token
    authorization = str(base64.b64encode(bytes(':'+access_token, 'ascii')), 'ascii')

    # header for request to the commit search API
    headers = {
        'Content-type': 'application/json',
        'Accept': 'application/json, text/javascript',
        'Authorization': 'Basic '+authorization
    }
    incremental_changes_list = []
    # Set the initial skip value and maximum batch size
    skip = 0
    batch_size = 100

    while True:
        # Execute the git diff command using Azure DevOps API
        diff_command_url = f"{repositories_url}?&$top={batch_size}&$skip={skip}&baseVersion={last_success_build_id}&baseVersionType=commit&targetVersion={current_head}&targetVersionType=commit&api-version-6.0"
        changes_response = requests.get(diff_command_url, headers=headers)
        changes_response.raise_for_status()
        changes = changes_response.json().get("changes", [])
        print(f"Executing Azure API URL: {diff_command_url}")
        # Extract the modified file paths
        if len(changes) > 0:
            print(f"Iterating while loop - Changes Count: {len(changes)}")
            # changes_list = changes_response.json().get("changes", [])
            if account_level_file == "0":
                for change in changes:
                    is_folder = (change['item']).get("isFolder", False)
                    file_path = f"{root_directory}{change['item']['path']}"

                    if pipeline_name.startswith('coedw_pipeline_'):
                        # Skip Security folder if the CICD pipeline is coedw pipeline
                        if change["changeType"] in ["add", "edit, rename", "rename"] and not is_folder and 'coEDW/Security/' not in file_path:
                            incremental_changes_list.append(file_path)
                        if ("/V_" not in file_path and "/v_" not in file_path) and not is_folder and 'coEDW/Security/' not in file_path:
                            incremental_changes_list.append(file_path)
                    else:
                        # include all the scripts
                        if change["changeType"] in ["add", "edit, rename", "rename"] and not is_folder:
                            incremental_changes_list.append(file_path)
                        if ("/V_" not in file_path and "/v_" not in file_path) and not is_folder:
                            incremental_changes_list.append(file_path)
            else:
                for change in changes:
                    is_folder = (change['item']).get("isFolder", False)
                    if change["changeType"] in ["add", "edit, rename", "rename"] and not is_folder:
                        file = f"{root_directory}{change['item']['path']}"
                        if "/coEDW/post_prod_deployment/" not in file:
                            continue
                        incremental_changes_list.append(file)
        # Break the loop if the batch size is less than the maximum batch size
        if len(changes) < batch_size:
            print(f"Breaking the loop as batch size is less than the maximum batch size - {len(changes)}")
            return incremental_changes_list

        # Increment the skip value for the next batch
        skip += batch_size


def get_modified_files(current_head, snowflake_connection, autocommit, verbose, buildid_info_table, last_success_build_id, execute_snowflake_query, root_directory, access_token, orderfile, repository_id, pipeline_name, account_level_file='0', folder_path=None):
    order_list = []
    incremental_changes_list = []
    if orderfile is not None:
        with open(orderfile) as f:
            for line in f:
                order_list.append(line.strip())
    # build_numbers = getBuildInfo(snowflake_connection, autocommit, verbose, buildid_info_table, execute_snowflake_query)
    # if len(build_numbers) != 0:
    #     last_success_build_id = build_numbers[0][0]
    # else:
    #     last_success_build_id = last_success_build_id

    print(f"last_success_build_id: {last_success_build_id}")
    print(f"current_head: {current_head}")
    print(f"account_level_file value: {account_level_file}")

    incremental_changes_list = get_incremental_changes_list(current_head, last_success_build_id, root_directory, access_token, repository_id, account_level_file, pipeline_name)

    all_v_files = dict()
    all_r_files = dict()
    all_r_files_2 = dict()
    allnewfiles = dict()
    i = 0
    # Traverse the entire directory structure recursively
    if folder_path is not None:
        for (directory_path, directory_names, file_names) in os.walk(root_directory):
            for file_name in file_names:
                if not file_name.endswith('.sql'):
                    continue
                file_full_path = os.path.join(directory_path, file_name)
                if file_full_path not in incremental_changes_list:
                    continue
                if account_level_file == "0":
                    if re.compile(folder_path).search(file_full_path) is None:
                        continue
                elif account_level_file == "3":
                    if re.compile("/post_prod_deployment/").search(file_full_path) is None:
                        continue
                allnewfiles[file_full_path] = file_name

    new_order_len = None
    for order in order_list:
        if order.endswith("/"):
            order = order[:-1]
        new_order = order.split("/")
        new_order_len = len(new_order)
        for file in allnewfiles:
            new_file = file.split("/")
            del new_file[0:9]
            if new_file:
                new_file.pop()
            len_file = len(new_file)

            script = get_details(file, allnewfiles[file])
            if new_order_len == len_file and new_order == new_file:
                all_v_files[i] = script
                i = i + 1
            else:
                all_r_files_2[file] = script
    for i in all_v_files:
        if all_v_files[i]["script_full_path"] in all_r_files_2:
            del all_r_files_2[all_v_files[i]["script_full_path"]]
    all_r_files = all_r_files_2
    return all_v_files, all_r_files


def get_account_modified_files(current_head, snowflake_connection, autocommit, verbose, buildid_info_table, last_success_build_id, execute_snowflake_query, root_directory, access_token, repository_id, pipeline_name, account_level_file='0'):

    # build_numbers = getBuildInfo(snowflake_connection, autocommit, verbose, buildid_info_table, execute_snowflake_query)
    # if len(build_numbers) != 0:
    #     last_success_build_id = build_numbers[0][0]
    # else:
    #     last_success_build_id = last_success_build_id

    print(f"last_success_build_id: {last_success_build_id}")
    print(f"current_head: {current_head}")
    print(f"account_level_file value: {account_level_file}")

    incremental_changes_file = get_incremental_changes_list(current_head, last_success_build_id, root_directory, access_token, repository_id, account_level_file, pipeline_name)

    incremental_changes_list = []

    if incremental_changes_file is not None:
        with open(incremental_changes_file) as f:
            for line in f:
                incremental_changes_list.append(line.strip())

    all_v_files = dict()
    all_r_files = dict()

    # Walk the entire directory structure recursively
    for (directory_path, directory_names, file_names) in os.walk(root_directory):
        for file_name in file_names:
            if not file_name.endswith('.sql'):
                continue

            file_full_path = os.path.join(directory_path, file_name)
            file_modified_time = datetime.fromtimestamp(os.path.getmtime(file_full_path)).strftime('%Y%m%d%H%M%S%f')

            if not(file_full_path in incremental_changes_list):
                continue

            script_name_parts = re.search(r'^([Vv])_(.+)\.sql$', file_name.strip())

            # Add this script to our dictionary (as nested dictionary)
            script = dict()
            script['script_name'] = file_name
            script['script_full_path'] = file_full_path
            script['script_type'] = 'R' if script_name_parts is None else 'V'
            script['script_description'] = (os.path.splitext(file_name)[0] if script_name_parts is None else script_name_parts.group(2)).replace('_', ' ').capitalize()
            script['script_modified_time'] = file_modified_time

            if script['script_type'] == 'V':
                all_v_files[file_full_path] = script
            else:
                all_r_files[file_full_path] = script

    return all_v_files, all_r_files


def get_details(full_file_path, file_name):
    try:
        file_modified_time = datetime.fromtimestamp(os.path.getmtime(full_file_path)).strftime('%Y%m%d%H%M%S%f')
        script_name_parts = re.search(r'^([Vv])_(.+)\.sql$', file_name.strip())

        # Add this script to our dictionary (as nested dictionary)
        script = dict()
        script['script_name'] = file_name
        script['script_full_path'] = full_file_path
        script['script_type'] = 'R' if script_name_parts is None else 'V'
        script['script_description'] = (os.path.splitext(file_name)[0] if script_name_parts is None else script_name_parts.group(2)).replace('_', ' ').capitalize()
        script['script_modified_time'] = file_modified_time
        return script
    except Exception as error:
        return error


def getBuildInfo(snowflake_connection, autocommit, verbose, buildid_info_table, execute_snowflake_query):

    qry_build_info_tables = "SELECT SUCCESSFUL_BUILD_ID FROM {0}.{1}.{2} ORDER BY DATE DESC;".format(buildid_info_table['database_name'], buildid_info_table['schema_name'], buildid_info_table['buildinfo_table_name'])
    resultset = execute_snowflake_query(snowflake_connection, qry_build_info_tables, autocommit, verbose)
    tab_cursor = resultset[0]
    ret_build_id = tab_cursor.fetchall()
    return ret_build_id


def extract_env(script_name):  # Function to extract the env from the script_name

    matches = re.findall(r'\((DEV|TST|PREPROD|PRD)\)', script_name, re.IGNORECASE)  # Use re.IGNORECASE for case-insensitive matching

    if matches:
        return [match.lower() for match in matches]  # Convert to lowercase for consistency
    else:
        return None


def replace_warehouse_name(content, env_dict_warehouse, database_environment):
    # Get environment-specific mapping dictionary
    env_mapping = env_dict_warehouse.get(database_environment, {})

    # Regex pattern for all variants of WAREHOUSE assignment
    pattern = r'(?i)(WAREHOUSE\s*=\s*)(\w+)'

    # Replacement function
    def replace_match(match):
        prefix, warehouse_name = match.groups()
        warehouse_name = warehouse_name.strip()
        mapped_name = env_mapping.get(warehouse_name)

        if mapped_name:
            if warehouse_name != mapped_name:
                print(f'Replacing "{warehouse_name}" → "{mapped_name}" for env "{database_environment}"')
                return f'{prefix}{mapped_name}'
            else:
                print(f'No change needed: "{warehouse_name}" already correct for env "{database_environment}"')
        else:
            print(f'No mapping found for "{warehouse_name}" in env "{database_environment}"')
        return match.group(0)

    # Replace all warehouse names using replacement function
    updated_content = re.sub(pattern, replace_match, content)
    return updated_content


def update_warehouse_size(user, account, role, warehouse, database, authenticator, password, deployment_warehouse_size):
    conn = snowflake.connector.connect(
        user=user,
        account=account,
        role=role,
        warehouse=warehouse,
        database=database,
        authenticator=authenticator,
        password=password
        )
    cursor = conn.cursor()

    cursor.execute(f"SHOW WAREHOUSES LIKE '{warehouse}'")
    warehouse_info = cursor.fetchone()
    current_warehouse_size = warehouse_info[3]
    print(f"Current warehouse size: {current_warehouse_size}, deployment warehouse size: {deployment_warehouse_size}")

    if current_warehouse_size.lower() != deployment_warehouse_size.lower():
        cursor.execute(f"ALTER WAREHOUSE {warehouse} SET WAREHOUSE_SIZE = {deployment_warehouse_size.upper()}")
        print(f"Warehouse size updated to {deployment_warehouse_size}")
        cursor.close()
        conn.close()
        return current_warehouse_size, True
    else:
        print("Warehouse size already matches deployment size.")
        cursor.close()
        conn.close()
        return current_warehouse_size, False


def revert_warehouse_size(user, account, role, warehouse, database, authenticator, password, original_size, size_changed):
    if not size_changed:
        print("No size change detected. No revert needed.")
        return

    conn = snowflake.connector.connect(
        user=user,
        account=account,
        role=role,
        warehouse=warehouse,
        database=database,
        authenticator=authenticator,
        password=password
    )
    cursor = conn.cursor()

    cursor.execute(f"ALTER WAREHOUSE {warehouse} SET WAREHOUSE_SIZE = {original_size.upper()}")
    print(f"Warehouse size reverted to {original_size}")
    cursor.close()
    conn.close()
