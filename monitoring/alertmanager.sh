#!/bin/sh
# Alertmanager cannot read environment variables in its config, so the config is rendered here from
# the SMTP settings in .env (see .env.example). Without them, mail goes to the local Mailpit inbox.
set -eu

printf '%s' "${SMTP_PASSWORD:-}" > /tmp/smtp_password

cat > /tmp/alertmanager.yml <<EOF
global:
  smtp_smarthost: '${SMTP_SMARTHOST}'
  smtp_from: '${ALERT_EMAIL_FROM}'
  smtp_auth_username: '${SMTP_USERNAME:-}'
  smtp_auth_password_file: /tmp/smtp_password
  smtp_require_tls: ${SMTP_REQUIRE_TLS}

route:
  receiver: email
  group_by: [alertname, service]
  group_wait: 30s
  group_interval: 5m
  repeat_interval: 4h

receivers:
  - name: email
    email_configs:
      - to: '${ALERT_EMAIL_TO}'
        send_resolved: true

# A down component also trips its warnings (unhealthy, lag, ...); only mail the critical one.
inhibit_rules:
  - source_matchers: [severity="critical"]
    target_matchers: [severity="warning"]
    equal: [service]
EOF

exec /bin/alertmanager --config.file=/tmp/alertmanager.yml --storage.path=/alertmanager "$@"
