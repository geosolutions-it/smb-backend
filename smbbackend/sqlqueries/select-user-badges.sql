SELECT
  b.id,
  b.name,
  b.acquired,
  p.target,
  p.progress
FROM profiles_smbuser AS u
  JOIN django_gamification_gamificationinterface AS i ON (u.gamification_interface_id = i.id)
  JOIN django_gamification_badge AS b ON (b.interface_id = i.id)
  LEFT JOIN django_gamification_progression AS p ON (b.progression_id = p.id)
WHERE u.id = %(user_id)s
  AND b.acquired = false
