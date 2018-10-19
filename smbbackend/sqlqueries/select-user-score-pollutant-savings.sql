SELECT
  SUM(e.{pollutant_name})
FROM tracks_segment AS s
  JOIN tracks_emission AS e ON (s.id = e.segment_id)
  JOIN tracks_track AS t ON (t.id = s.track_id)
  JOIN profiles_smbuser AS u ON (u.id = t.owner_id)
WHERE s.start_date >= %(start_date)s
  AND s.end_date <= %(end_date)s
  AND u.id = %(user_id)s
  AND t.is_valid = true
