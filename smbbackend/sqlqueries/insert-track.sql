INSERT INTO tracks_track (owner_id, session_id, created_at)
VALUES (%s, %s, %s)
RETURNING id
