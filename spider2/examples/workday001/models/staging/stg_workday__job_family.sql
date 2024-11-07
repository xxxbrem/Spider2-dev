
with base as (

    select * 
    from {{ ref('stg_workday__job_family_base') }}
),

fields as (

    select
        {{
            fivetran_utils.fill_staging_columns(
                source_columns=adapter.get_columns_in_relation(ref('stg_workday__job_family_base')),
                staging_columns=get_job_family_columns()
            )
        }}
        {{ fivetran_utils.source_relation(
            union_schema_variable='workday_union_schemas', 
            union_database_variable='workday_union_databases') 
        }}
    from base
),

final as (
    
    select 
        source_relation,
        _fivetran_synced,
        effective_date,
        id as job_family_id,
        inactive as is_inactive,
        job_family_code,
        summary as job_family_summary
    from fields
    where not coalesce(_fivetran_deleted, false)
)

select *
from final
