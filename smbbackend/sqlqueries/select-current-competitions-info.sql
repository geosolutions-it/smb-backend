-- select all competitions which have already started but have no winner(s)
WITH competition_without_winner AS (
    SELECT DISTINCT c.id
    FROM prizes_competition AS c
             JOIN prizes_competitionparticipant AS cp on c.id = cp.competition_id
             LEFT OUTER JOIN prizes_winner AS w on w.participant_id = cp.id
    WHERE w.participant_id IS NULL
      AND c.start_date <= %(relevant_date)s
)
SELECT
    c.id,
    c.name,
    c.criteria,
    c.winner_threshold,
    c.start_date,
    c.end_date,
    c.age_groups,
    st_astext(st_collect(roi.geom)) AS region_of_interest
FROM prizes_competition c
         JOIN competition_without_winner cww ON c.id = cww.id
         LEFT OUTER JOIN prizes_competition_regions cr ON c.id = cr.competition_id
         LEFT OUTER JOIN prizes_regionofinterest roi ON roi.id = cr.regionofinterest_id
GROUP BY
    c.id,
    c.name,
    c.criteria,
    c.winner_threshold,
    c.start_date,
    c.end_date,
    c.age_groups
