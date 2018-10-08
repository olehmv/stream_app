select
    month,
    year,
    number_of_calls,
    max(number_of_calls) over (partition by month) - number_of_calls as diff
from
    (select
        year,
        month,
        number_of_calls,
        rank() over (partition by year order by number_of_calls desc) as rank,
        dense_rank() over (partition by year order by number_of_calls desc) as dense_rank,
        row_number() over (partition by year order by number_of_calls desc) as row_number
    from
        (select
            distinct year(CallDateTs) as year,
            month(CallDateTs) as month ,
            count(*) over (partition by year(CallDateTs),
            month(CallDateTs)) as number_of_calls
            from fire_calls  order by year, month) number_of_calls_per_year_per_month
            order by year) month_rank
order by
    month,
    number_of_calls desc