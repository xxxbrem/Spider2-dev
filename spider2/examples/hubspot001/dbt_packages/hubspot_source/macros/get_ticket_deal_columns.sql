{% macro get_ticket_deal_columns() %}

{% set columns = [
    {"name": "_fivetran_synced", "datatype": dbt.type_timestamp()},
    {"name": "ticket_id", "datatype": dbt.type_int()},
    {"name": "deal_id", "datatype": dbt.type_int()}
] %}

{{ return(columns) }}

{% endmacro %}
