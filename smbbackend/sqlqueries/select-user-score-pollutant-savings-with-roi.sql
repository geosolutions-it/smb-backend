WITH competition_roi AS (
    SELECT
        cr.competition_id,
        ST_Collect(roi.geom) AS geom
    FROM prizes_competition_regions AS cr
             JOIN prizes_regionofinterest AS roi ON cr.regionofinterest_id = roi.id
    WHERE cr.competition_id = %(competition_id)s
    GROUP BY cr.competition_id
)
SELECT
    SUM(ST_Length(ST_Intersection(t.geom, croi.geom)::GEOGRAPHY)) /
    SUM(ST_Length(t.geom::GEOGRAPHY)) *
    SUM(CAST(t.aggregated_emissions ->> '{pollutant_name}' AS FLOAT)) AS emissions
FROM tracks_track AS t
         JOIN prizes_competitionparticipant AS cp ON t.owner_id = cp.user_id
         JOIN competition_roi AS croi ON croi.competition_id = cp.competition_id
         JOIN prizes_competition AS c ON cp.competition_id = c.id
WHERE ST_Intersects(croi.geom, t.geom)
  AND t.owner_id = %(user_id)s
  AND t.start_date >= c.start_date
  AND t.end_date <= c.end_date
  AND t.is_valid = true
  AND cp.registration_status = 'approved'
