-- depends_on: {{ ref('country') }}
select
{{dbt_utils.generate_surrogate_key(['id'])}} as sk_c,
cast(id as String) as id,
country
 from {{ ref('country')}}