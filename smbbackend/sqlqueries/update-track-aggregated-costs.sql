UPDATE tracks_track SET aggregated_costs = (
  SELECT row_to_json(agg) FROM (
    SELECT
      SUM(c.fuel_cost) AS fuel_cost,
      SUM(c.time_cost) AS time_cost,
      SUM(c.depreciation_cost) AS depreciation_cost,
      SUM(c.operation_cost) AS operation_cost,
      SUM(c.total_cost) AS total_cost
    FROM tracks_cost AS c
      JOIN tracks_segment AS s ON (c.segment_id = s.id)
    WHERE s.track_id = %(track_id)s
    GROUP BY s.track_id
  ) AS agg
)
