UPDATE tracks_track SET aggregated_health = (
  SELECT row_to_json(agg) FROM (
    SELECT
      SUM(h.calories_consumed) AS calories_consumed
    FROM tracks_health AS h
      JOIN tracks_segment AS s ON (h.segment_id = s.id)
    WHERE s.track_id = %(track_id)s
    GROUP BY s.track_id
  ) AS agg
)
