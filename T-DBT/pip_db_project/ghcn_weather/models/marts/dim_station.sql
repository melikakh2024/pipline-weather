-- depends_on: {{ ref('stations') }}
{{config(
    pre_hook="create table if not exists `finalbatchproject.gchn_weather_dw_marts.hook_log` (message STRING)",
    post_hook="alter table {{this}} set options(description='Station dimension table')"

)}}
select
{{dbt_utils.generate_surrogate_key(['id'])}} as sk_s,
id,
latitude,
longitude,
elevation
from {{ref('stationsnapshot')}}
