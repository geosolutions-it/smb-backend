SELECT SUM(CAST(t.aggregated_emissions ->> '{pollutant_name}' AS FLOAT)) AS emissions
FROM tracks_track AS t
    JOIN prizes_competitionparticipant AS cp ON t.owner_id = cp.user_id
    JOIN prizes_competition AS c ON cp.competition_id = c.id
WHERE c.id = %(competition_id)s
    AND t.owner_id = %(user_id)s
    AND t.start_date >= c.start_date
    AND t.end_date <= c.end_date
    AND cp.registration_status = 'approved'
    AND t.is_valid = true
