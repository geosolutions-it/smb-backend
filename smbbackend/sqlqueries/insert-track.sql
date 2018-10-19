INSERT INTO tracks_track (
  owner_id,
  session_id,
  created_at,
  is_valid,
  validation_error
)
VALUES (
  %(owner_id)s,
  %(session_id)s,
  %(created_at)s,
  %(is_valid)s,
  %(validation_error)s
)
RETURNING id
