-- Найти имена и фамилии клиентов с топ-3 минимальной и топ-3 максимальной суммой транзакций за весь период (учесть клиентов, у которых нет заказов, приняв их сумму транзакций за 0).
with customer_orders as (
	select
		c.customer_id
		,c.first_name
		,c.last_name
		,sum(coalesce(oi.quantity,0) * coalesce(oi.item_list_price_at_sale,0))::numeric as sum_sales
	from customer c
	left join orders o on o.customer_id = c.customer_id
		and o.order_status = 'Approved'
	left join order_items oi using (order_id)
	group by c.customer_id, c.first_name, c.last_name
),
customers_ranked_min as (
	select *
 		,dense_rank() over (order by sum_sales) as dr
	from customer_orders 
),
customers_ranked_max as (
	select *
 		,dense_rank() over (order by sum_sales desc) as dr
	from customer_orders 
)
SELECT * FROM customers_ranked_min where dr <= 3
union all
SELECT * FROM customers_ranked_max where dr <= 3
;