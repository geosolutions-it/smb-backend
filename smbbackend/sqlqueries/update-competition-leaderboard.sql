UPDATE prizes_competition SET
  closing_leaderboard = %(leaderboard)
WHERE id = %(competition_id)s