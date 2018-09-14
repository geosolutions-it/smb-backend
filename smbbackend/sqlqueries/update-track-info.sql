UPDATE tracks_track SET
  geom = sq.geom,
  length = st_length(sq.geom::geography),
  start_date = sq.start,
  end_date = sq.end,
  duration = sq.duration
FROM (
  SELECT
    track_id,
    st_makeline(the_geom ORDER BY timestamp ) AS geom,
    MIN(timestamp) AS "start",
    MAX(timestamp) AS "end",
    extract(day from MAX(timestamp) - MIN(timestamp)) * 24 * 60 +
      extract(hour from MAX(timestamp) - MIN(timestamp)) * 60 +
      extract(minute from MAX(timestamp) - MIN(timestamp)) +
      extract(second from MAX(timestamp) - MIN(timestamp)) / 60  AS "duration"
  FROM tracks_collectedpoint
  WHERE track_id = %(track_id)s
  GROUP BY track_id
) AS sq
WHERE id = %(track_id)s
