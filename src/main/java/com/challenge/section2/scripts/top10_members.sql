/* Which are the top 10 members by spending */

WITH member_order AS (
SELECT t.membership_id, rank() OVER(ORDER BY t.total_cost DESC)
FROM (
    SELECT membership_id, SUM(total_cost) AS total_cost
    FROM orders
    GROUP BY membership_id
    ) t
)

SELECT m.membership_id, m.first_name, m.last_name
FROM members m INNER JOIN member_order n
ON m.membership_id = n.membership_id
WHERE n.rank > 11;


