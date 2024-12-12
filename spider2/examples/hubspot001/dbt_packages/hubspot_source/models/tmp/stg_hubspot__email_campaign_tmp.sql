{{ config(enabled=fivetran_utils.enabled_vars(['hubspot_marketing_enabled','hubspot_email_event_enabled'])) }}

select {{ dbt_utils.star(source('hubspot','email_campaign')) }}
from {{ var('email_campaign') }}
