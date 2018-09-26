SELECT date_trunc('day', created_at), COUNT(1)
FROM tracks_track
WHERE
  created_at >= %(start_date)s and
  created_at <= %(end_date)s
GROUP BY date_trunc('day', created_at)