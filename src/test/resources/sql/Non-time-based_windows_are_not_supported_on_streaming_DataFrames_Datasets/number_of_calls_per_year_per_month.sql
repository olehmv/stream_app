select
    distinct year(CallDateTs) as year,
    month(CallDateTs) as month ,
    count(*) over (partition by year(CallDateTs),
    month(CallDateTs)) as number_of_calls
from fire_calls
order by year, month