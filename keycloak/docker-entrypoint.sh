#!/bin/bash
set -e

# Start Keycloak in the background
/opt/keycloak/bin/kc.sh start-dev --import-realm &
KC_PID=$!

# Wait for Keycloak to become ready
echo "Waiting for Keycloak to start..."
until /opt/keycloak/bin/kcadm.sh config credentials \
    --server http://localhost:8080 \
    --realm master \
    --user "$KEYCLOAK_ADMIN" \
    --password "$KEYCLOAK_ADMIN_PASSWORD" 2>/dev/null; do
    sleep 2
done

echo "Keycloak is ready. Disabling SSL requirement for reports-realm..."

/opt/keycloak/bin/kcadm.sh update realms/reports-realm -s sslRequired=NONE 2>/dev/null && \
    echo "SSL requirement disabled for reports-realm." || \
    echo "reports-realm not found yet, skipping SSL update."

# Wait for the Keycloak process
wait $KC_PID
