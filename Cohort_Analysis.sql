WITH first_values as (   -- Initial user settings
 
    SELECT DISTINCT user_id,
           FIRST_VALUE(c.city_name) OVER (PARTITION BY user_id ORDER BY datetime) AS city_name,
           FIRST_VALUE(c.city_id) OVER (PARTITION BY user_id ORDER BY datetime) AS city_id,
           FIRST_VALUE(a.device_type) OVER (PARTITION BY user_id ORDER BY datetime) AS device_type,
           FIRST_VALUE(a.age) OVER (PARTITION BY user_id ORDER BY datetime) AS age,
           FIRST_VALUE(a.source) OVER (PARTITION BY user_id ORDER BY datetime) AS source,
           FIRST_VALUE(a.first_date) OVER (PARTITION BY user_id ORDER BY datetime) AS first_date 
    FROM analytics_events AS a
    LEFT JOIN cities c ON a.city_id = c.city_id
    WHERE first_date BETWEEN '2021-05-01' AND '2021-06-25' 
          AND event = 'authorization'
          AND user_id IS NOT NULL
 
),
new AS (
 
    -- New users by day for CAC calculation
 
    SELECT first_date,
           source,
           COUNT(DISTINCT user_id) AS new_dau
    FROM first_values
    GROUP BY first_date,
             source
 
),
cac AS (
 
    -- CAC calculation
 
    SELECT b.source,
           b.date,
           SUM(b.budget) AS budget,
           SUM(n.new_dau) AS new_dau,
           SUM(b.budget) / SUM(n.new_dau) AS cac
    FROM advertisement_budgets AS b
    LEFT JOIN new AS n ON n.source = b.source
                       AND n.first_date = b.date
    WHERE date BETWEEN '2021-05-01' AND '2021-06-25' 
    GROUP BY b.source, date
 
),
profiles AS (
 
    -- Formation of profiles
 
    SELECT f.user_id,
           f.first_date,
           f.city_name,
           f.city_id,
           f.device_type,
           f.age,
           f.source, 
           c.cac
    FROM first_values AS f
    LEFT JOIN cac AS c ON c.source = f.source
                       AND c.date = f.first_date
 
),
cohorts AS (
 
    -- Formation of cohorts
 
    SELECT first_date,
           city_name,
           city_id,
           device_type,
           age,
           source, 
           COUNT(DISTINCT user_id) AS cohort_size,
           SUM(cac) AS ad_cost
    FROM profiles
    GROUP BY first_date,
             city_name,
             city_id,
             device_type,
             age,
             source 
),

orders AS (
 
    -- Daily revenue
 
    SELECT first_date,
           city_id,
           device_type,
           age,
           source, 
           log_date -  first_date AS lifetime,
           (revenue * commission - delivery) AS rev 
    FROM analytics_events
    WHERE event = 'order'
          AND log_date BETWEEN '2021-05-01' and TO_DATE('2021-06-25', 'YYYY-MM-DD') + INTERVAL '7 day' 
    GROUP BY first_date,
             city_id,
             device_type,
             age,
             source,
             (revenue * commission - delivery),
             log_date -  first_date
 
),
rev AS (
 
    -- Revenue by cohort
 
    SELECT c.first_date,
           c.city_name,
           c.city_id,
           c.device_type,
           c.age,
           c.source, 
           o.lifetime,
           o.rev,
           c.cohort_size,
           c.ad_cost
    FROM cohorts c
    LEFT JOIN orders o ON c.first_date = o.first_date
                       AND c.city_id = o.city_id
                       AND c.device_type = o.device_type
                       AND c.age = o.age
                       AND c.source = o.source 
 
)
     -- Final result 
SELECT first_date,
       city_name,
       device_type,
       age,
       source, 
       MAX(cohort_size) AS cohort_size,
       MAX(ad_cost) AS ad_cost,
       SUM(CASE WHEN lifetime <= 0 THEN rev ELSE 0 END) AS ltv_d1, 
       SUM(CASE WHEN lifetime <= 1 THEN rev ELSE 0 END) AS ltv_d2,
       SUM(CASE WHEN lifetime <= 2 THEN rev ELSE 0 END) AS ltv_d3,
       SUM(CASE WHEN lifetime <= 3 THEN rev ELSE 0 END) AS ltv_d4,
       SUM(CASE WHEN lifetime <= 4 THEN rev ELSE 0 END) AS ltv_d5,
       SUM(CASE WHEN lifetime <= 5 THEN rev ELSE 0 END) AS ltv_d6,
       SUM(CASE WHEN lifetime <= 6 THEN rev ELSE 0 END) AS ltv_d7 
FROM rev
GROUP BY first_date,
         city_name,
         device_type,
         age,
         source 