{{ config(enabled=var('hubspot_service_enabled', False)) }}

select {{ dbt_utils.star(source('hubspot','ticket_contact')) }}
from {{ var('ticket_contact') }}
