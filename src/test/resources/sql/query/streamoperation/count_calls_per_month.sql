select
    month(CallDateTs) as month,
    count(CallDateTs) as count
from fire_calls
group by month
order by month