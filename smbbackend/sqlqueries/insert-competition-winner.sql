-- insert a new winner for the specified competition
INSERT INTO prizes_winner (
  competition_id,
  user_id,
  rank
) VALUES (
  %(competition_id)s,
  %(user_id)s,
  %(rank)s
)