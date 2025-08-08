SELECT
sp.operating_system,
COUNT(DISTINCT es.id_message) AS sent_msg,
COUNT(DISTINCT eo.id_message) AS open_msg,
COUNT(DISTINCT ev.id_message) AS vist_msg,
COUNT(DISTINCT eo.id_message) / COUNT(DISTINCT es.id_message) * 100 AS open_rate,
COUNT(DISTINCT ev.id_message) / COUNT(DISTINCT es.id_message) * 100 AS click_rate,
COUNT(DISTINCT ev.id_message) / COUNT(DISTINCT eo.id_message) * 100 AS ctor,
FROM `DA.account` a
JOIN `DA.email_sent` es
ON a.id = es.id_account
LEFT JOIN `DA.email_open` eo
ON es.id_message = eo.id_message
LEFT JOIN `DA.email_visit` ev
ON es.id_message = ev.id_message
JOIN `DA.account_session` acs
ON ev.id_account = acs.account_id
JOIN `DA.session_params` sp
ON acs.ga_session_id = sp.ga_session_id
WHERE a.is_unsubscribed = 0
GROUP BY sp.operating_system
