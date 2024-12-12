--depends_on: {{ var('organization') }}
{{
    fivetran_utils.union_data(
        table_identifier='organization', 
        database_variable='linkedin_pages_database', 
        schema_variable='linkedin_pages_schema', 
        default_database=target.database,
        default_schema='linkedin_company_pages',
        default_variable='organization',
        union_schema_variable='linkedin_pages_union_schemas',
        union_database_variable='linkedin_pages_union_databases'
    )
}}