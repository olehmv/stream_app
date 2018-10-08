select
    date_format(CallDateTs,"EEEE") as day,
    count(*) as count
from fire_calls
group by day
order by count