-- depends_on: {{ ref('country') }}
select
    {{ dbt_utils.generate_surrogate_key(['id']) }} as sk_c,
    cast(c.id as string) as id,
    cast(c.country as string) as country
from {{ ref('country') }} as c