select member_id, sum(total_item_price)
from transactions
group by member_id
order by sum(total_item_price) desc
limit 10
