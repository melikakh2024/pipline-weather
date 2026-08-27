with source as (
    select * from {{ source('gchn_source', 'country') }}
),

renamed as (
    select
        cast(id as string) as id,
        cast(country as string) as country
    from source
)

select
    id,
    country
from renamed
