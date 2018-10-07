select
month,
year,
number_of_calls,
max(number_of_calls) over (partition by month) - number_of_calls as diff
from month_rank
order by  month,number_of_calls desc