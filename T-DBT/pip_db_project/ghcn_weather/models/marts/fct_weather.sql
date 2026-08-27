-- depends_on: {{ ref('weather') }}
{{config(
          materialized="incremental",
          incremental_strategy="merge",
          unique_key="primarykey"
          )}}
select
w.primarykey,
w.id,
w.code_country,
w.date,
w.Element,
w.value,
dc.sk_c,
ds.sk_s


from {{ref('weather')}}  w
     left join {{ref('dim_country')}}  dc on dc.id=w.code_country
     left join {{ref('dim_station')}}  ds    on ds.id=w.id

{% if is_incremental() %}
where w.date >= coalesce((select max(date) from {{ this }}), '1900-01-01')
{% endif %}