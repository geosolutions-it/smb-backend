SELECT id
FROM prizes_competitionparticipant
WHERE competition_id = %(competition_id)s
    AND user_id = %(user_id)s
