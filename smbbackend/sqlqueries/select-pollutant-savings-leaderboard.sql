SELECT
  ROW_NUMBER() OVER (ORDER BY SUM(e.{pollutant_name})) AS points,
  cp.user_id AS user_id,
  SUM(e.{pollutant_name})
FROM tracks_segment AS s
  JOIN tracks_emission AS e ON (s.id = e.segment_id)
  JOIN tracks_track AS t ON (t.id = s.track_id)
  JOIN prizes_competitionparticipant AS cp ON (t.owner_id = cp.user_id)
  JOIN prizes_competition AS c ON (c.id = cp.competition_id)
WHERE s.start_date >= %(start_date)s
  AND s.end_date <= %(end_date)s
  AND t.is_valid = true
  AND cp.registration_status = 'approved'
  AND c.id = %(competition_id)s
GROUP BY cp.user_id
ORDER BY points DESC
LIMIT %(threshold)s
