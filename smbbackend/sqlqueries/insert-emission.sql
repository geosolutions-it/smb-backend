INSERT INTO tracks_emission (
  so2,
  so2_saved,
  nox,
  nox_saved,
  co2,
  co2_saved,
  co,
  co_saved,
  pm10,
  pm10_saved,
  segment_id
) VALUES (
  %(so2)s,
  %(so2_saved)s,
  %(nox)s,
  %(nox_saved)s,
  %(co2)s,
  %(co2_saved)s,
  %(co)s,
  %(co_saved)s,
  %(pm10)s,
  %(pm10_saved)s,
  %(segment_id)s
)