SELECT
  id,
  vehicle_type,
  ST_Length(geom::geography) AS length,
  end_date - start_date AS duration
FROM tracks_segment
WHERE track_id = %(track_id)s