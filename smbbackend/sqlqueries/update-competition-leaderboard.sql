UPDATE prizes_competition SET
  closing_leaderboard = %(leaderboard)s
WHERE id = %(competition_id)s