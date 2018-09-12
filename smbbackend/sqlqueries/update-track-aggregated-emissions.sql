UPDATE tracks_track SET aggregated_emissions = (
  SELECT row_to_json (agg) FROM (
    SELECT
      SUM(e.so2) AS so2,
      SUM(e.so2_saved) AS so2_saved,
      SUM(e.nox) AS nox,
      SUM(e.nox_saved) AS nox_saved,
      SUM(e.co) AS co,
      SUM(e.co_saved) AS co_saved,
      SUM(e.co2) AS co2,
      SUM(e.co2_saved) AS co2_saved,
      SUM(e.pm10) AS pm10,
      SUM(e.pm10_saved) AS pm10_saved
    FROM tracks_emission AS e
      JOIN tracks_segment AS s ON (e.segment_id = s.id)
    WHERE s.track_id = %(track_id)s
    GROUP BY s.track_id
  ) AS agg
)
