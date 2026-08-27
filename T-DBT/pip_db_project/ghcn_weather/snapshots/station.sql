{% snapshot stationsnapshot %}

{{ config(
    unique_key='id',
    strategy='check',
    check_cols=['latitude', 'longitude','elevation'],
    materialized='snapshot'
) }}

select id, latitude, longitude, elevation
from {{ref('stations')}}

{% endsnapshot %}
