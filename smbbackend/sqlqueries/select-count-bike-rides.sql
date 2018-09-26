SELECT
  date_trunc('day', start_date),
  count(date_part('day', start_date))
FROM tracks_segment
WHERE start_date >= %(start)s
  AND start_date < %(end)s
  AND vehicle_type = 'bike'
GROUP BY date_trunc('day', start_date)