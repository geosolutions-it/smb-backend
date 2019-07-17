SELECT date_trunc('day', created_at), COUNT(1)
FROM tracks_track
WHERE is_valid = TRUE
  AND owner_id = %(owner_id)s
  AND created_at >= %(start_date)s
  AND created_at <= %(end_date)s
GROUP BY date_trunc('day', created_at)