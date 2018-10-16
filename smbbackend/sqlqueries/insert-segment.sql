INSERT INTO tracks_segment (
  track_id,
  user_uuid,
  vehicle_type,
  geom,
  start_date,
  end_date
) VALUES (
  %(track_id)s,
  %(user_uuid)s,
  %(vehicle_type)s,
  ST_Force2D(ST_GeomFromWKB(%(geometry)s, 4326)),
  %(start_date)s,
  %(end_date)s
)
RETURNING id
