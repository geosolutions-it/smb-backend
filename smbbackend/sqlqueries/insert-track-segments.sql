INSERT INTO tracks_segment (
  track_id,
  user_uuid,
  vehicle_type,
  geom,
  start_date,
  end_date
)
SELECT
  track_id AS track_id,
  %(user_uuid)s AS user_uuid,
  vehicle_type AS vehicle_type,
  ST_MakeLine(the_geom) AS geom,
  MIN(timestamp) AS start,
  MAX(timestamp) AS end
FROM (
  SELECT
    track_id,
    the_geom,
    timestamp,
    vehicle_type,
    SUM(CASE WHEN changed THEN 0 ELSE 1 END) OVER (ORDER BY timestamp ASC) AS clustr
    FROM (
      SELECT
        track_id,
        timestamp,
        the_geom,
        vehicle_type,
        vehicle_type = lag(vehicle_type, 1) OVER (ORDER BY timestamp ASC) AS changed
      FROM tracks_collectedpoint
      WHERE track_id = %(track_id)s
    ) AS sq
  ) as sq2
GROUP BY
  clustr,
  vehicle_type,
  track_id
RETURNING id, vehicle_type