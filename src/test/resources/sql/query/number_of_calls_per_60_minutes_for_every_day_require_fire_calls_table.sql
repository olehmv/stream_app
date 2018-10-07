select
CallDateTs,
window(ReceivedDtTs,"60 minutes") as OneHour,
count(ReceivedDtTs) as Count
from fire_calls
group by CallDateTs,OneHour