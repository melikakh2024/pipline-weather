with
source as (
    select * from  {{ source ('gchn_source', 'weather') }}
),

renamed as (
    select
    {{ dbt_utils.generate_surrogate_key(['id' ,'date' ,'element']) }} as primarykey,
        id as id,
        substring (id,1,2) as code_country,
        date as date,
        Element as Element,
        value as value,
    from source
)
select * from renamed

