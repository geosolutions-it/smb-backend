SELECT
  ROW_NUMBER() OVER (ORDER BY SUM(e.{pollutant_name})) AS points,
  u.id,
  p.age,
  SUM(e.{pollutant_name})
FROM tracks_segment AS s
  JOIN tracks_emission AS e ON (s.id = e.segment_id)
  JOIN tracks_track AS t ON (t.id = s.track_id)
  JOIN profiles_smbuser AS u ON (u.id = t.owner_id)
  JOIN profiles_enduserprofile AS p ON (p.user_id = u.id)
WHERE s.start_date >= %(start_date)s
  AND s.end_date <= %(end_date)s
  AND p.age = ANY(%(age_groups)s)
GROUP BY
  u.id,
  p.age
ORDER BY points DESC
LIMIT %(threshold)s