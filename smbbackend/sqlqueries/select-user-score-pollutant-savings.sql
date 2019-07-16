SELECT
  SUM(e.{pollutant_name})
FROM tracks_segment AS s
  JOIN tracks_emission AS e ON (s.id = e.segment_id)
  JOIN tracks_track AS t ON (t.id = s.track_id)
  JOIN prizes_competitionparticipant cp on t.owner_id = cp.user_id
WHERE s.start_date >= %(start_date)s
  AND s.end_date <= %(end_date)s
  AND cp.user_id = %(user_id)s
  AND cp.registration_status = 'approved'
  AND t.is_valid = true
