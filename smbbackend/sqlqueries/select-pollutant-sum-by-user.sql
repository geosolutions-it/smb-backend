SELECT
  SUM((aggregated_emissions->>%(pollutant)s)::float)
FROM tracks_track
WHERE is_valid = TRUE
  AND owner_id = %(user_id)s
