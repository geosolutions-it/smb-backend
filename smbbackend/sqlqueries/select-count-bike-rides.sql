SELECT
  date_trunc('day', s.start_date),
  count(date_part('day', s.start_date))
FROM tracks_segment AS s
  JOIN tracks_track AS t ON (s.track_id = t.id)
WHERE s.start_date >= %(start)s
  AND s.start_date < %(end)s
  AND s.vehicle_type = 'bike'
  AND t.is_valid = TRUE
GROUP BY date_trunc('day', s.start_date)