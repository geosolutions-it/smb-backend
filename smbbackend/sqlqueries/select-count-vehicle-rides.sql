SELECT COUNT(1) AS num_rides
FROM tracks_segment AS s
  JOIN tracks_track AS t ON (t.id = s.track_id)
WHERE vehicle_type = ANY(%(vehicle_types)s)
  AND t.owner_id = %(user_id)s
