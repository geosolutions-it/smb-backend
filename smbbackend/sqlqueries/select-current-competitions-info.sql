-- select all competitions which have already started but have no winner(s)
SELECT
  c.id,
  c.name,
  c.criteria,
  c.winner_threshold,
  c.start_date,
  c.end_date,
  c.age_groups
FROM prizes_competition AS c
  LEFT OUTER JOIN prizes_winner AS w ON (c.id = w.competition_id)
WHERE c.start_date <= %(relevant_date)s
  AND w.user_id IS null
