#!/usr/bin/env bash
# Takes a component down, waits until Alertmanager fires the alerts for it (and, with the local
# Mailpit inbox, until the email arrives), then brings it back and waits until the alerts resolve.
#
# usage: monitoring/trigger-alert.sh <service> [--kill] [--keep-down]
#   <service>    compose service, e.g. kafka3, taskmanager, timescaledb, redis, fastapi
#   --kill       SIGKILL the container (a crash) instead of stopping it gracefully
#   --keep-down  leave the service down; start it later with: docker compose start <service>
set -euo pipefail
cd "$(dirname "$0")/.."

ALERTMANAGER=${ALERTMANAGER:-http://localhost:19093}
MAILPIT=${MAILPIT:-http://localhost:8025}
TIMEOUT=300

usage() {
  sed -n '5,8p' "$0" | sed 's/^# \{0,1\}//'
  exit 1
}

service=${1:-}
[ -n "$service" ] || usage
shift
mode=stop
keep_down=false
for arg in "$@"; do
  case $arg in
    --kill) mode=kill ;;
    --keep-down) keep_down=true ;;
    *) usage ;;
  esac
done

# "<alertname> <service label>" of the alerts expected to fire.
case $service in
  kafka[1-5]) expected=("ContainerDown $service" "KafkaBrokerDown kafka") ;;
  taskmanager) expected=("ContainerDown taskmanager" "FlinkNoTaskManager taskmanager") ;;
  timescaledb) expected=("ContainerDown timescaledb" "TimescaleDBDown timescaledb") ;;
  jobmanager)
    echo "Flink runs without high availability: stopping the jobmanager cancels the job, and it is not"
    echo "resumed when the jobmanager comes back. Try taskmanager instead (the job recovers from its checkpoint)."
    exit 1 ;;
  kafka-init-topics|python-db-helper|job-submitter|logging-setup-*|prometheus|alertmanager|mailpit)
    echo "$service is a setup or alerting container; stopping it does not trigger a ContainerDown alert."
    exit 1 ;;
  *) expected=("ContainerDown $service") ;;
esac

if ! docker compose ps --services --status running | grep -qx "$service"; then
  echo "$service is not running; start the stack first (docker compose up -d)."
  exit 1
fi
curl -fsS -o /dev/null "$ALERTMANAGER/-/ready" || { echo "Alertmanager is not reachable at $ALERTMANAGER."; exit 1; }

# With SMTP settings in .env the mail goes to a real mailbox, which this script cannot check.
check_mail=true
if grep -qs '^SMTP_SMARTHOST=' .env || ! curl -fsS -o /dev/null "$MAILPIT/api/v1/messages?limit=1"; then
  check_mail=false
fi

alert_active() {  # <alertname> <service label>
  curl -fsS -G "$ALERTMANAGER/api/v2/alerts" \
    --data-urlencode "filter=alertname=\"$1\"" --data-urlencode "filter=service=\"$2\"" \
    | grep -q '"state":"active"'
}

all_firing() {
  for alert in "${expected[@]}"; do
    alert_active $alert || return 1
  done
}

none_firing() {
  for alert in "${expected[@]}"; do
    ! alert_active $alert || return 1
  done
}

firing_mails() {  # <alertname> <service label>
  curl -fsS "$MAILPIT/api/v1/messages?limit=500" | { grep -o "\[FIRING:[0-9]*\] $1 $2 " || true; } | wc -l | tr -d ' '
}

all_mailed() {
  for i in "${!expected[@]}"; do
    [ "$(firing_mails ${expected[$i]})" -gt "${mails_before[$i]}" ] || return 1
  done
}

started=$(date +%s)
wait_for() {  # <description> <check function>
  printf '%s' "$1"
  local deadline=$(( $(date +%s) + TIMEOUT ))
  until $2; do
    if [ "$(date +%s)" -ge "$deadline" ]; then
      echo " timed out after ${TIMEOUT}s"
      return 1
    fi
    printf '.'
    sleep 5
  done
  echo " after $(( $(date +%s) - started ))s"
}

restore() {
  echo "Starting $service again"
  docker compose start "$service" >/dev/null
}

mails_before=()
if $check_mail; then
  for alert in "${expected[@]}"; do
    mails_before+=("$(firing_mails $alert)")
  done
fi

echo "Expected alerts: $(printf '%s, ' "${expected[@]}" | sed 's/, $//')"
echo "Taking $service down ($mode)"
docker compose "$mode" "$service" >/dev/null
started=$(date +%s)
$keep_down || trap 'echo; restore; exit 130' INT TERM

wait_for "Waiting for the alerts to fire (rules wait 1 minute before firing)" all_firing || { $keep_down || restore; exit 1; }
if $check_mail; then
  wait_for "Waiting for the alert email in Mailpit ($MAILPIT)" all_mailed || true
else
  echo "Alert email sent to the mailbox configured in .env"
fi

if $keep_down; then
  echo "$service stays down; start it with: docker compose start $service"
  exit 0
fi

trap - INT TERM
restore
started=$(date +%s)
wait_for "Waiting for the alerts to resolve" none_firing
echo "The RESOLVED email follows within 5 minutes (Alertmanager group_interval)."
case $service in
  taskmanager|timescaledb) echo "The Flink job restarts from its last checkpoint; a FlinkJobRestarted warning may follow." ;;
esac
