{{
    fivetran_utils.union_data(
        table_identifier='job_family_job_profile', 
        database_variable='workday_database', 
        schema_variable='workday_schema', 
        default_database=target.database,
        default_schema='workday',
        default_variable='job_family_job_profile',
        union_schema_variable='workday_union_schemas',
        union_database_variable='workday_union_databases'
    )
}}
