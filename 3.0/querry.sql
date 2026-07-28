SELECT DISTINCT
    referencing_database,
    referencing_schema,
    referencing_object_name AS referencing_view,
    referenced_database,
    referenced_schema,
    referenced_object_name AS referenced_table
FROM snowflake.account_usage.object_dependencies
WHERE referenced_object_domain = 'TABLE'
  AND referencing_object_domain = 'VIEW'
  -- Target database where table modifications occurred
  AND UPPER(referenced_database) = UPPER('YOUR_TARGET_DATABASE')
  
  -- Pass the modified table names here
  AND UPPER(referenced_object_name) IN (
      UPPER('TABLE_NAME_1'),
      UPPER('TABLE_NAME_2')
  )
  
  -- Exclude databases containing specific keywords
  AND NOT REGEXP_LIKE(
      referencing_database,
      '(?i).*(DEV|TEST|SANDBOX|PREPROD|CLONE|UAT|COMMON_UTILITY|CONNECTORS_SECRET|CO_AI_AGENTS|DEMO|DROPPED|EVENTS_DB|RBAC_GEN|STREAMLIT_APPS|USER|UTIL).*'
  )
  
  -- Optional: Filter for a specific list of target databases to validate
  -- AND UPPER(referencing_database) IN (UPPER('DB1'), UPPER('DB2'))
ORDER BY referencing_database, referencing_schema, referencing_view;
