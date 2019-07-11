-- insert a new winner for the specified competition
INSERT INTO prizes_winner (
  participant_id,
  rank
) VALUES (
  %(participant_id)s,
  %(rank)s
)