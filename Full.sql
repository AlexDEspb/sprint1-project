--------------

CREATE TABLE if not exists analysis.tmp_rfm_recency (
 user_id INT NOT NULL PRIMARY KEY,
 recency INT NOT NULL CHECK(recency >= 1 AND recency <= 5)
);
CREATE TABLE if not exists analysis.tmp_rfm_frequency (
 user_id INT NOT NULL PRIMARY KEY,
 frequency INT NOT NULL CHECK(frequency >= 1 AND frequency <= 5)
);
CREATE TABLE if not exists analysis.tmp_rfm_monetary_value (
 user_id INT NOT NULL PRIMARY KEY,
 monetary_value INT NOT NULL CHECK(monetary_value >= 1 AND monetary_value <= 5)
);
CREATE TABLE if not exists analysis.dm_rfm_segments (
user_id INT NOT NULL PRIMARY KEY
, recency INT NOT NULL CHECK(recency >= 1 AND recency <= 5)
, frequency INT NOT NULL CHECK(frequency >= 1 AND frequency <= 5)
, monetary_value INT NOT NULL CHECK(monetary_value >= 1 AND monetary_value <= 5)
);

create OR REPLACE view analysis.orderitems_vw as (select * from production.orderitems);
create OR REPLACE view analysis.orders_vw as (select * from production.orders);
create OR REPLACE view analysis.orderstatuses_vw as (select * from production.orderstatuses);
create OR REPLACE view analysis.orderstatuslog_vw as (select * from production.orderstatuslog);
create OR REPLACE view analysis.products_vw as (select * from production.products);
create OR REPLACE view analysis.users_vw as (select * from production.users);


truncate analysis.tmp_rfm_frequency;

---------------------------

insert into analysis.tmp_rfm_frequency
with cte as (
select count(o.order_id) ordcnt
	,u.id user_id
	,row_number() over (order by count(order_id)) ordrank
	from analysis.users_vw u
		left join analysis.orders_vw o 
		on (u.id = o.user_id and o.status = 4 and extract ('YEAR' from o.order_ts)=2022)
	group by u.id
	--where extract ('YEAR' from o.order_ts)=2022 and status = 4
	--group by user_id
	order by count(order_id)
	)
select 	--ordcnt
		user_id
		, case 
			when ordrank between 0 and (select max(ordrank) from cte)::numeric*1/5 then 1 
			when ordrank between (select max(ordrank) from cte)::numeric*1/5 and (select max(ordrank) from cte)::numeric*2/5 then 2
			when ordrank between (select max(ordrank) from cte)::numeric*2/5 and (select max(ordrank) from cte)::numeric*3/5 then 3
			when ordrank between (select max(ordrank) from cte)::numeric*3/5 and (select max(ordrank) from cte)::numeric*4/5 then 4
			when ordrank between (select max(ordrank) from cte)::numeric*4/5 and (select max(ordrank) from cte)::numeric*5/5 then 5
			end frequency
		from cte;
		
select count(user_id), frequency from analysis.tmp_rfm_frequency
group by frequency;
------------------------------------
 
--with cte as (
--select 
--	o.order_id
--	,o.order_ts
--	,o.user_id
--	, o.status
--	--,row_number() over (order by count(order_id)) ordrank
--	, lag(o.order_ts) OVER (PARTITION BY user_id 
--                                 ORDER BY order_ts) prev_hitdatetime
--	from production.orders o
--	--inner join (select * from production.orders o1 where o.order_id = o2.order_id and o.user_id = o2.user_id offset 1) as o1
--	where extract ('YEAR' from o.order_ts)=2022 and o.status = 4
--	--group by user_id
--	order by o.user_id, o.order_ts
--	)
--select * from cte

truncate analysis.tmp_rfm_recency;

insert into analysis.tmp_rfm_recency
with cte as (
select 
	u.id user_id
	,max(o.order_ts) 
	,row_number() over (order by coalesce(max(o.order_ts), to_timestamp(0))) ordrank
	from analysis.users_vw u
		left join analysis.orders_vw o 
		on (u.id = o.user_id and o.status = 4 and extract ('YEAR' from o.order_ts)=2022)
	group by u.id
)
select 	--ordcnt
		user_id
		, case 
			when ordrank between 0 and (select max(ordrank) from cte)::numeric*1/5 then 1 
			when ordrank between (select max(ordrank) from cte)::numeric*1/5 and (select max(ordrank) from cte)::numeric*2/5 then 2
			when ordrank between (select max(ordrank) from cte)::numeric*2/5 and (select max(ordrank) from cte)::numeric*3/5 then 3
			when ordrank between (select max(ordrank) from cte)::numeric*3/5 and (select max(ordrank) from cte)::numeric*4/5 then 4
			when ordrank between (select max(ordrank) from cte)::numeric*4/5 and (select max(ordrank) from cte)::numeric*5/5 then 5
			end recency
		from cte;

select count(user_id), recency from analysis.tmp_rfm_recency
group by recency;

-----------------------------------------

truncate analysis.tmp_rfm_monetary_value;

insert into analysis.tmp_rfm_monetary_value
with cte as (
select 
	u.id user_id
	,sum(o."cost") 
	,row_number() over (order by sum(o."cost")) ordrank
	from analysis.users_vw u
		left join analysis.orders_vw o 
		on (u.id = o.user_id and o.status = 4 and extract ('YEAR' from o.order_ts)=2022)
	group by u.id
	order by ordrank 
)
select 	--ordcnt
		user_id
		, case 
			when ordrank between 0 and (select max(ordrank) from cte)::numeric*1/5 then 1 
			when ordrank between (select max(ordrank) from cte)::numeric*1/5 and (select max(ordrank) from cte)::numeric*2/5 then 2
			when ordrank between (select max(ordrank) from cte)::numeric*2/5 and (select max(ordrank) from cte)::numeric*3/5 then 3
			when ordrank between (select max(ordrank) from cte)::numeric*3/5 and (select max(ordrank) from cte)::numeric*4/5 then 4
			when ordrank between (select max(ordrank) from cte)::numeric*4/5 and (select max(ordrank) from cte)::numeric*5/5 then 5
			end monetary_value
		from cte;

select count(user_id), monetary_value from analysis.tmp_rfm_monetary_value
group by monetary_value;

----------------------------------------------

truncate analysis.dm_rfm_segments;

insert into analysis.dm_rfm_segments
select 
		r.user_id
		, r.recency 
		, f.frequency
		, v.monetary_value 
		from analysis.tmp_rfm_recency r 
			inner join analysis.tmp_rfm_frequency f on r.user_id = f.user_id 
			inner join analysis.tmp_rfm_monetary_value v on r.user_id = v.user_id 
		order by r.user_id;
	
select * from analysis.dm_rfm_segments;
			
-----------------------------------------------

create OR REPLACE view analysis.orders_vw2 as (
with cte as (
select 	o.order_id 
		,o.order_ts 
		,o.user_id 
		,o.bonus_payment 
		,o.payment 
		,o."cost" 
		,o.bonus_grant 
		,osl.status_id status
		,rank() OVER (PARTITION BY o.order_id ORDER BY osl.dttm desc) rnk
		from production.orders o 
			inner join production.orderstatuslog osl 
			on o.order_id = osl.order_id  
		order by o.order_id 
)
select order_id 
		,order_ts 
		,user_id 
		,bonus_payment 
		,payment 
		,"cost" 
		,bonus_grant 
		,status 
		from cte
		where rnk = 1
);

