-- select all competitions which have already started but have no winner(s)
SELECT
  c.id,
  cd.name,
  cd.criteria,
  cd.repeat_when,
  cd.winner_threshold,
  c.start_date,
  c.end_date,
  c.age_group
FROM prizes_competition AS c
  JOIN prizes_competitiondefinition AS cd ON (c.competition_definition_id = cd.id)
  LEFT OUTER JOIN prizes_winner AS w ON (c.id = w.competition_id)
WHERE c.start_date <= %(relevant_date)s
  AND w.user_id IS null
