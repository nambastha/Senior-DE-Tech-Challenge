

SELECT m.item_id
FROM (
    SELECT p.item_id, rank() OVER(ORDER BY p.quantity DESC) as rank
    FROM (
        SELECT item_id, SUM(quantity) as quantity
        FROM order_items
        GROUP BY item_id
    ) p
   ) m
WHERE rank > 4;