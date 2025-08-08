WITH table_1 AS (
SELECT
s.date,
sp.country,
COUNT(DISTINCT a.id) AS account_cnt,
a.send_interval,
a.is_verified,
a.is_unsubscribed
FROM `DA.account` a
JOIN `DA.account_session` ass
ON a.id = ass.account_id
JOIN `DA.session` s
ON ass.ga_session_id = s.ga_session_id
JOIN `DA.session_params` sp
ON s.ga_session_id = sp.ga_session_id
GROUP BY s.date, sp.country, a.send_interval, a.is_unsubscribed, a.is_verified
),


table_2 AS (
SELECT
DATE_ADD(se.date, INTERVAL es.sent_date DAY) AS email_date,
sp.country,
a.send_interval,
a.is_verified,
a.is_unsubscribed,
COUNT(DISTINCT es.id_message) AS sent_msg,
COUNT(DISTINCT eo.id_message) AS open_msg,
COUNT(DISTINCT ev.id_message) AS visit_msg
FROM `DA.email_sent` es
LEFT JOIN `DA.email_open` eo
ON es.id_message = eo.id_message
LEFT JOIN `DA.email_visit` ev
ON eo.id_message = ev.id_message
JOIN `DA.account_session` ass
ON es.id_account = ass.account_id
JOIN `DA.session_params` sp
ON ass.ga_session_id = sp.ga_session_id
JOIN `DA.session` se
ON se.ga_session_id = sp.ga_session_id
JOIN `DA.account` a
ON a.id = ass.account_id
GROUP BY email_date, sp.country, a.send_interval, a.is_verified, a.is_unsubscribed
),


table_3 AS (
SELECT
date,
country,
send_interval,
is_verified,
is_unsubscribed,
account_cnt,
0 AS sent_msg,
0 AS open_msg,
0 AS visit_msg
FROM table_1


UNION ALL


SELECT
email_date AS date,
country,
send_interval,
is_verified,
is_unsubscribed,
0 AS account_cnt,
sent_msg,
open_msg,
visit_msg
FROM table_2
),


table_4 AS (
SELECT
date,
country,
send_interval AS send_interval,
is_verified AS is_verified,
is_unsubscribed AS is_unsubscribed,
SUM(account_cnt) AS total_account_cnt,
SUM(sent_msg) AS total_sent_msg,
SUM(open_msg) AS total_open_msg,
SUM(visit_msg) AS total_visit_msg
FROM table_3
GROUP BY date, country, send_interval, is_verified, is_unsubscribed
),


table_5 AS (
SELECT
date,
country,
send_interval,
is_verified,
is_unsubscribed,
total_account_cnt,
total_sent_msg,
total_open_msg,
total_visit_msg,
total_country_account_cnt,
total_country_sent_cnt,
DENSE_RANK() OVER (ORDER BY total_country_account_cnt DESC ) AS rank_total_country_account_cnt,
DENSE_RANK() OVER (ORDER BY total_country_sent_cnt DESC) AS rank_total_country_sent_cnt
FROM (
SELECT
date,
country,
send_interval AS send_interval,
is_verified AS is_verified,
is_unsubscribed AS is_unsubscribed,
total_account_cnt,
total_sent_msg,
total_open_msg,
total_visit_msg,
SUM(total_account_cnt) OVER (PARTITION BY country) AS total_country_account_cnt,
SUM(total_sent_msg) OVER (PARTITION BY country) AS total_country_sent_cnt
FROM
table_4))


SELECT
t5.date,
t5.country,
t5.send_interval,
t5.is_verified,
t5.is_unsubscribed,
t5.total_account_cnt AS account_cnt,
t5.total_sent_msg AS sent_msg,
t5.total_open_msg AS open_msg,
t5.total_visit_msg AS visit_msg,
t5.total_country_account_cnt,
t5.total_country_sent_cnt,
t5.rank_total_country_account_cnt,
t5.rank_total_country_sent_cnt
FROM table_5 t5
WHERE t5.rank_total_country_account_cnt <= 10 OR t5.rank_total_country_sent_cnt <= 10;
