# Usage: ./poll.sh [minutes]
INTERVAL_MINUTES="1"
REPO_DIR="C:\Users\Dell\Documents\VSC\VDS\zoho-api"

cd "$REPO_DIR"

while true; do
  printf '\n[%s] mail_bookings_trigger\n' "$(date)"
  python -m app.tasks.mail_bookings_trigger

  printf '[%s] mail_unsub_poll\n' "$(date)"
  python -m app.tasks.mail_unsub_poll

  printf '[%s] sleeping %s minute(s)\n' "$(date)" "$INTERVAL_MINUTES"
  sleep "${INTERVAL_MINUTES}m"
done
