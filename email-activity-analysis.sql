SELECT
distinct sent_month,
id_account,
account_sent/total_sent*100 as sent_msg_percent_from_this_month,
first_sent_date,
last_sent_date
FROM (
SELECT
DATE_TRUNC(DATE_ADD(se.date, INTERVAL es.sent_date DAY), MONTH) as sent_month,
COUNT(id_message) OVER(PARTITION BY DATE_TRUNC(DATE_ADD(se.date, INTERVAL es.sent_date DAY), MONTH)) AS total_sent,
COUNT(id_message) OVER(PARTITION BY id_account, DATE_TRUNC(DATE_ADD(se.date, INTERVAL es.sent_date DAY), MONTH)) AS account_sent,
MIN (DATE_ADD(se.date, INTERVAL es.sent_date DAY)) OVER (partition by id_account,DATE_TRUNC(DATE_ADD(se.date, INTERVAL es.sent_date DAY), MONTH)) as first_sent_date,
MAX (DATE_ADD(se.date, INTERVAL es.sent_date DAY)) OVER (partition by id_account,DATE_TRUNC(DATE_ADD(se.date, INTERVAL es.sent_date DAY), MONTH)) as last_sent_date,
id_account, id_message
FROM `DA.email_sent` es
join `DA.account_session` ass
ON es.id_account=ass.account_id
join `DA.session` se
ON ass.ga_session_id=se.ga_session_id) q
order by id_account
