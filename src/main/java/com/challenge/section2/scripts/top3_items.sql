select item_id, count(transaction_id)
from transactions
join items using (item_id)
group by item_id
order by count(transaction_id) desc
limit 3

