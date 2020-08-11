# 3회차 Assignment by nublu1234

## Assignment - 1

### SQL 연습: 사용자별로 처음 채널과 마지막 채널 알아내기

```sql
-- Sol 1
SELECT DISTINCT usc.userid, 
       FIRST_VALUE(usc.channel) OVER(PARTITION BY userid 
                                    ORDER BY st.ts 
                                     ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_channel,
       LAST_VALUE(usc.channel) OVER(PARTITION BY userid 
                                    ORDER BY st.ts 
                                    ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_channel
FROM raw_data.session_timestamp st 
LEFT JOIN raw_data.user_session_channel usc 
ON st.sessionid = usc.sessionid
ORDER BY 1;


-- Sol 2
WITH sub AS (
    SELECT usc.userid, usc.channel,
           ROW_NUMBER() OVER(PARTITION BY userid ORDER BY st.ts ASC) rn
    FROM raw_data.session_timestamp st 
    LEFT JOIN raw_data.user_session_channel usc 
    ON st.sessionid = usc.sessionid
    ORDER BY 1, 3
)
-- SELECT * FROM sub;

, main AS (
    SELECT userid, MAX(rn) AS max_, MIN(rn) AS min_ 
    FROM sub
    GROUP BY userid 
)
-- SELECT * FROM main;

SELECT m.userid, s1.channel AS first_channel, s2.channel AS last_channel
FROM main m
LEFT JOIN sub s1
ON m.userid = s1.userid 
AND m.min_ = s1.rn
LEFT JOIN sub s2
ON m.userid = s2.userid 
AND m.max_ = s2.rn
ORDER BY 1;
```



## Assignment - 2

```sql
SELECT usc.userid, SUM(amount) AS gross_revenue 
FROM raw_data.user_session_channel usc
LEFT JOIN raw_data.session_transaction st
ON usc.sessionid = st.sessionid
GROUP BY 1
HAVING SUM(amount) IS NOT NULL
ORDER BY 2 DESC
LIMIT 10;
```



## Assignment - 3

```sql
WITH full_table AS (
    SELECT c.channelname, 
           usc.channel, 
           usc.userid, 
           EXTRACT(MONTH FROM st.ts) month_, 
           sts.refunded, 
           sts.amount
    FROM raw_data.channel c
    FULL JOIN raw_data.user_session_channel usc
    ON c.channelname = usc.channel
    LEFT JOIN raw_data.session_timestamp st
    ON usc.sessionid = st.sessionid
    LEFT JOIN raw_data.session_transaction sts
    ON usc.sessionid = sts.sessionid
)   
-- SELECT * FROM full_table;

SELECT month_, 
       channelname AS channel, 
       COUNT(DISTINCT userid) AS uniqueUsers,
       COUNT(DISTINCT (CASE WHEN refunded IS NOT NULL THEN userid END)) AS paidUsers,
	   COUNT(DISTINCT (CASE WHEN refunded IS NOT NULL THEN userid END))::float/
       (CASE WHEN COUNT(DISTINCT userid) = 0 THEN 1 ELSE COUNT(DISTINCT userid) END)::float AS conversionRate,
       SUM(amount) AS grossRevenue,
       SUM(CASE WHEN refunded = False THEN amount ELSE NULL END) AS netRevenue
FROM full_table
GROUP BY 1, 2
ORDER BY 1, 2;
```



## Assignment - 4

```sql
WITH full_table AS (
    SELECT DISTINCT usc.userid, 
           FIRST_VALUE(LEFT(st.ts, 7)) OVER(PARTITION BY userid ORDER BY yearmonth ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_month, 
           LEFT(st.ts, 7) AS yearmonth
    FROM raw_data.user_session_channel usc
    LEFT JOIN raw_data.session_timestamp st
    ON usc.sessionid = st.sessionid
)
-- SELECT * FROM full_table ORDER BY 1, 2, 3;
-- SELECT first_month, TO_NUMBER(LEFT(first_month,4),9999) + 1 FROM full_table;


, grpby_table AS (
    SELECT first_month, 
           12*(TO_NUMBER(LEFT(yearmonth,4),9999) - TO_NUMBER(LEFT(first_month,4),9999))
           + (TO_NUMBER(RIGHT(yearmonth,2),99) - TO_NUMBER(RIGHT(first_month,2),99))
           + 1 AS month_, 
           COUNT(userid) AS user_count
    FROM full_table
    GROUP BY 1, 2
    ORDER BY 1, 2
)
-- SELECT * FROM grpby_table;

SELECT first_month AS Cohort_Month,
       MAX(CASE WHEN month_ = 1 THEN user_count ELSE NULL END) AS Month_1,
       MAX(CASE WHEN month_ = 2 THEN user_count ELSE NULL END) AS Month_2,
       MAX(CASE WHEN month_ = 3 THEN user_count ELSE NULL END) AS Month_3,
       MAX(CASE WHEN month_ = 4 THEN user_count ELSE NULL END) AS Month_4,
       MAX(CASE WHEN month_ = 5 THEN user_count ELSE NULL END) AS Month_5,
       MAX(CASE WHEN month_ = 6 THEN user_count ELSE NULL END) AS Month_6,
       MAX(CASE WHEN month_ = 7 THEN user_count ELSE NULL END) AS Month_7
FROM grpby_table 
GROUP BY 1
ORDER BY 1;
```



### Question

1. 빅쿼리 같은 환경에서 비용친화적인 쿼리 사용법이 궁금합니다. LIMIT 대신 WHERE 사용하기, 'SELECT *' 대신 column 명확히 기입하기 외에 다른 팁이 있다면 알고 싶습니다.
2. 해당 링크에서 '쿼리 결과를 단계별로 구체화하기'가 어떤 내용인지 알고 싶습니다. https://cloud.google.com/bigquery/docs/best-practices-costs?hl=ko
3. Assignment를 수행하다보니 window 함수에서의 over안의 frame(ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)을 작성하지 않는 경우에 에러가 발생하였습니다. BigQuery, Oracle, MySQL를 이용할때는 이러한 경험이 없었는데 RedShift를 이용하는 과정에서 이러한 오류가 발생하니 제가 쿼리를 어딘가 잘못 작성한게 아닌가 하는 생각이 듭니다. 제 쿼리에 어느 부분이 문제가 있는건지 알고 싶습니다.



> It written by [Tydora](https://typora.io/)