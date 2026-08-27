{{config(materialized="table")}}

with spine as (
  {{
    dbt_utils.date_spine(
    datepart="day",
    start_date="cast('2024-01-01' as date)",
    end_date="cast('2025-01-01' as date)"
    )
}})


SELECT
  cast(date_day as date) as date_,
  extract(year from date_day) as year,
  extract(month from date_day) as month,
FORMAT_DATE('%B', date_day) as month_name,
FORMAT_DATE('%A', date_day) as day_name
from spine