SELECT SUM((aggregated_health->>'calories_consumed')::float)
FROM tracks_track
WHERE owner_id = %(user_id)s
