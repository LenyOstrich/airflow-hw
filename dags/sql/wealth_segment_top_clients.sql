-- Найти топ-5 клиентов (по общему доходу) в каждом сегменте благосостояния (wealth_segment). 
-- Вывести имя, фамилию, сегмент и общий доход. Если в сегменте менее 5 клиентов, вывести всех.
with customer_with_total_revenue as (
	select 
			c.customer_id
			,c.wealth_segment 
			,sum(coalesce(oi.quantity,0) * coalesce(oi.item_list_price_at_sale,0)) as sum_sales
		from customer c
		join orders o using (customer_id)
		join order_items oi using (order_id)
		where o.order_status = 'Approved'
		group by c.customer_id, c.wealth_segment
),
customer_ranked as (
	select 
		cwr.customer_id
		,cwr.wealth_segment
		,cwr.sum_sales 
		,dense_rank() over (partition by cwr.wealth_segment order by cwr.sum_sales desc) as dr
	from customer_with_total_revenue cwr
),
customer_top_ids as (
	select
		customer_id
		,wealth_segment 
		,sum_sales 
	from customer_ranked
	where dr <= 5
)
select
	c.first_name
	,c.last_name
	,c.wealth_segment
	,cwr.sum_sales 
from customer c
join customer_top_ids cwr on c.customer_id = cwr.customer_id 
	and c.wealth_segment = cwr.wealth_segment
order by c.wealth_segment, cwr.sum_sales desc
;