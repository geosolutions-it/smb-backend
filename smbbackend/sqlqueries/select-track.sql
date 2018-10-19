SELECT
  t.id,
  t.created_at,
  t.owner_id,
  t.aggregated_costs,
  t.aggregated_emissions,
  t.aggregated_health,
  t.duration,
  t.start_date,
  t.end_date,
  t.length,
  t.is_valid,
  t.validation_error,
  json_agg(
    json_build_object(
      'id', s.id,
      'vehicle_type', s.vehicle_type,
      'length', st_length(s.geom::geography)
    )
  ) AS segments
FROM tracks_track AS t
  LEFT JOIN tracks_segment as s ON (t.id = s.track_id)
WHERE t.id = %(track_id)s
GROUP BY
  t.id,
  t.created_at,
  t.owner_id,
  t.aggregated_costs,
  t.aggregated_emissions,
  t.aggregated_health,
  t.duration,
  t.start_date,
  t.end_date,
  t.length