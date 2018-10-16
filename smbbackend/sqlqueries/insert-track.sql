INSERT INTO tracks_track (owner_id, session_id, created_at)
VALUES (
  %(owner_id)s,
  %(session_id)s,
  %(created_at)s
)
RETURNING id
