SELECT
sp.continent,
COUNT(CASE
WHEN params.value.string_value LIKE '%YouTube%' AND params.key = 'page_title'
THEN ep.ga_session_id
END) / COUNT(*) * 100 AS percent_page_title
FROM `DA.event_params` ep, unnest (event_params) as params
JOIN `DA.session_params` sp
ON ep.ga_session_id = sp.ga_session_id
WHERE params.key = 'page_title'
AND params.value.string_value IS NOT NULL
GROUP BY sp.continent
