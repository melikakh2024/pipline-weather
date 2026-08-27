with
source as (
    select * from  {{ source ('gchn_source', 'station') }}
),

renamed as (
    select
    cast(id as string) as id ,
    cast(latitude as FLOAT64)as latitude,
    cast(longitude as FLOAT64) as longitude,
    cast(elevation as FLOAT64) as elevation
    from source
)
select * from renamed

