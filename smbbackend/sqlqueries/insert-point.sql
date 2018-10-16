INSERT INTO tracks_collectedpoint (
    vehicle_type,
    track_id,
    the_geom,
    sessionid,
    timestamp
) VALUES (
    %(vehicle_type)s,
    %(track_id)s,
    ST_SetSRID(ST_MakePoint(%(longitude)s, %(latitude)s), 4326),
    %(sessionid)s,
    %(timestamp)s
)
