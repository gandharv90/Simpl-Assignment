/*
SQL used - BigQuery Standard SQL
*/


/*
Given a transactions table: TRANSACTION_ID,USER_ID,MERCHANT_NAME,TRANSACTION_DATE,AMOUNT 
Assumptions:
TRANSACTION_ID is incremental
TRANSACTION_DATE is only date without time of day
*/

-- 1. a. Write a query to find the first merchant a user transacts on
Select USER_ID , MERCHANT_NAME 
from (
  select * , rank() over (partition by USER_ID order by TRANSACTION_ID) as rn
  from `simpl.sql_data` 
)
where rn = 1;

--1. b. Extract count of users who transacted for the first time on a Monday for each merchant
Select count(distinct USER_ID)  
from (
  select * , rank() over (partition by USER_ID order by TRANSACTION_ID) as rn
  from `simpl.sql_data` 
)
where rn = 1 and extract(DAYOFWEEK from TRANSACTION_DATE) = 1;


--2 Write a query to extract alternate rows ordered by date for each user.
Select *except(rn)
from (
  select * , rank() over (partition by USER_ID order by TRANSACTION_DATE ) as rn
  from `simpl.sql_data` 
)
where mod(rn, 2) = 1
order by USER_ID, TRANSACTION_DATE;

-- 3. Write a query to extract top 25% of users based on amount spent
Select USER_ID, total_spent, percentile
from(
  Select *, round((rank() over (order by total_spent))/(count(count_helper) over (partition by count_helper))*100,2)  percentile
  from(
    select USER_ID, sum(AMOUNT__) as total_spent, 1 as count_helper from `simpl.sql_data` group by 1,3 order by 2 
  )
)
where percentile >= 75;

--4. Write a query to calculate time difference (in days) between current and previous order of each customer for every row and the avg time difference between two orders for every customer.
Select *, round(avg(days_since_last_txn) over (partition by USER_ID ),1) as avg_order_delay
from(
  Select * , date_diff(TRANSACTION_DATE, 
                  lag( TRANSACTION_DATE) over (partition by USER_ID order by TRANSACTION_DATE )
                  ,DAY) as days_since_last_txn
  from `simpl.sql_data` 
)
order by USER_ID ,TRANSACTION_DATE ;

--5. Write a query to get count of users who transact in 3 continuous months.
Select count(distinct USER_ID) as three_month_users
from(
  SELECT USER_ID  ,month, year
    , DATE_DIFF( date,lag( date) over (partition by user_id order by date), Month ) as lag1
    , DATE_DIFF( date,lag( date,2) over (partition by user_id order by date), Month ) as lag2
  from (
    Select USER_ID 
      , extract(month from TRANSACTION_DATE ) as month
      , extract(year from TRANSACTION_DATE ) as year
      , min(TRANSACTION_DATE) as date
    from `simpl.sql_data`
    group by 1,2,3
  )
  order by 1,3,2
)
where lag1=1 and lag2=2