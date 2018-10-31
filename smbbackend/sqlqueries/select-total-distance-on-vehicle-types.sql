SELECT SUM(st_length(s.geom::geography))
FROM tracks_segment AS s
  JOIN bossoidc_keycloak AS k ON (s.user_uuid = k."UID")
  JOIN tracks_track AS t ON (s.track_id = t.id)
WHERE vehicle_type LIKE ANY(%(vehicle_types)s)
  AND k.user_id = %(user_id)s
  AND t.is_valid = TRUE

