WITH pollutant_emissions AS (
    SELECT
        t.owner_id,
        SUM(CAST(t.aggregated_emissions ->> '{pollutant_name}' AS FLOAT)) AS emissions
    FROM tracks_track AS t
        JOIN prizes_competitionparticipant AS cp ON t.owner_id = cp.user_id
        JOIN prizes_competition AS c ON cp.competition_id = c.id
    WHERE c.id = %(competition_id)s
        AND t.start_date >= c.start_date
        AND t.end_date <= c.end_date
        AND cp.registration_status = 'approved'
        AND t.is_valid = true
    GROUP BY t.owner_id
)
SELECT
    row_number() OVER (ORDER BY pe.emissions) AS points,
    pe.owner_id AS user_id,
    pe.emissions
FROM pollutant_emissions pe
ORDER BY points DESC
LIMIT %(threshold)s
