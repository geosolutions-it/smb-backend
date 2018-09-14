INSERT INTO tracks_cost (
  fuel_cost,
  time_cost,
  depreciation_cost,
  operation_cost,
  total_cost,
  segment_id
) VALUES (
  %(fuel_cost)s,
  %(time_cost)s,
  %(depreciation_cost)s,
  %(operation_cost)s,
  %(total_cost)s,
  %(segment_id)s
)
