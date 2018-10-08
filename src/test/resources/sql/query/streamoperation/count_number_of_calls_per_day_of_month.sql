select
    dayofmonth(CallDateTs) as day,
    count(*) as count
from fire_calls
group by day
order by count desc