create table month_rank as select year,month,number_of_calls,
rank() over (partition by year order by number_of_calls desc) as rank,
dense_rank() over (partition by year order by number_of_calls desc) as dense_rank,
row_number() over (partition by year order by number_of_calls desc) as row_number
from number_of_calls_per_year_per_month  order by year