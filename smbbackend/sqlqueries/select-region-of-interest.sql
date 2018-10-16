WITH dumped AS (
  SELECT (ST_Dump(geom)).geom AS geom
  FROM tracks_regionofinterest
)
SELECT ST_AsBinary(ST_Collect(geom)) AS geom
FROM dumped
