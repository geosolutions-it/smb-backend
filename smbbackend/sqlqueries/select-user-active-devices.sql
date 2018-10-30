SELECT d.registration_id
FROM fcm_django_fcmdevice AS d
  JOIN profiles_smbuser AS u ON (u.id = d.user_id)
  JOIN bossoidc_keycloak AS k ON (u.id = k.user_id)
WHERE d.active = true AND k."UID" = %(owner_uuid)s
