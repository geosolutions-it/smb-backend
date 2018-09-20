SELECT
  SUM((aggregated_emissions->>%(pollutant)s)::float)
FROM tracks_track
WHERE owner_id = %(user_id)s
