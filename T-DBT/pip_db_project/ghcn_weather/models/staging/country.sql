with
source as (
    select * from  {{ source ('gchn_source', 'country') }}
),

renamed as (
    select
        cast( id as string) as id,
        country  as country
    from source
)
select * from renamed

