SELECT
  p.name
FROM prizes_competitionprize AS cp
  JOIN prizes_prize AS p ON (p.id = cp.prize_id)
where competition_id = %(competition_id)s
  and user_rank = %(user_rank)s