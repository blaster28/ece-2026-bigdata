#!/usr/bin/env bash
# silver.users was dropped from Polaris during the cleanup of lab 09 part 2.
# Register the lab 07 table again (v1.metadata.json), through the Iceberg REST API, as the principal dev.
set -euo pipefail
POLARIS_URI="${POLARIS_URI:-http://polaris:8181}"
CATALOG="${POLARIS_CATALOG:-$KUBERNETES_NAMESPACE}"
BUCKET="$KUBERNETES_NAMESPACE"
: "${POLARIS_DEV_CREDENTIAL:?load ~/work/.polaris-credentials.env first}"

TOKEN=$(curl -fsS "$POLARIS_URI/api/catalog/v1/oauth/tokens" \
  -d grant_type=client_credentials \
  -d client_id="${POLARIS_DEV_CREDENTIAL%%:*}" \
  -d client_secret="${POLARIS_DEV_CREDENTIAL#*:}" \
  -d scope=PRINCIPAL_ROLE:ALL | jq -r .access_token)

curl -sS -X POST "$POLARIS_URI/api/catalog/v1/$CATALOG/namespaces/silver/register" \
  -H "Authorization: Bearer $TOKEN" -H "Content-Type: application/json" \
  -d "{\"name\":\"users\",\"metadata-location\":\"s3a://$BUCKET/warehouse/silver/users/metadata/v1.metadata.json\"}" \
  | jq -c '{"metadata-location", location: .metadata.location, snapshot: .metadata."current-snapshot-id", error: .error.message}'

curl -sS "$POLARIS_URI/api/catalog/v1/$CATALOG/namespaces/silver/tables" \
  -H "Authorization: Bearer $TOKEN" | jq -c '[.identifiers[].name]'
