-- Delete points that are disjoint with the region of interest
--
-- Since the `tracks.regionofinterest` table has geometries of MultiPolygon
-- AND may contain multiple rows, we employ a trick:
-- -  dump all features
-- -  then collect them again
--
-- The result is a single MultiPolygon with the combined geometries of all
-- rows.
--
-- We finally do the deletion as the result of all points which are disjoint
-- (i.e. do not intersect) this combined ROI geometry
--

with dumped as (
  select (st_dump(geom)).geom as geom
  from tracks_regionofinterest
)
,
flattened_roi AS (
  select st_collect(geom) as geom
  from dumped
)
,
disposable AS (
  SELECT distinct(cp.id) as id
  FROM
    tracks_collectedpoint AS cp,
    flattened_roi
  WHERE cp.track_id = %(track_id)s
    AND NOT st_intersects(the_geom, flattened_roi.geom)
)

DELETE
FROM tracks_collectedpoint as cp
WHERE cp.id = ANY(SELECT d.id FROM disposable AS d)
RETURNING cp.id
