#!/usr/bin/env bash
# Exercise 2: catalog role silver_reader with read grants on the silver namespace only,
# granted to a new principal role silver_analyst, assigned to a new principal silver_user.
# Runs with the root principal; the new credentials go to ~/work/.polaris-credentials.env (outside Git).
set -euo pipefail
POLARIS_URI="${POLARIS_URI:-http://polaris:8181}"
CATALOG="${POLARIS_CATALOG:-$KUBERNETES_NAMESPACE}"
API="$POLARIS_URI/api/management/v1"
CREDENTIALS_FILE="$HOME/work/.polaris-credentials.env"

TOKEN=$(curl -fsS "$POLARIS_URI/api/catalog/v1/oauth/tokens" \
  -d grant_type=client_credentials -d client_id=root -d client_secret="${POLARIS_ROOT_SECRET:-s3cr3t}" \
  -d scope=PRINCIPAL_ROLE:ALL | jq -r .access_token)

# call METHOD URL [JSON]: prints the HTTP status (201/204 = done, 409 = already exists)
call() {
  local data=()
  if [ -n "${3:-}" ]; then data=(-d "$3"); fi
  curl -sS -o /dev/null -w "%{http_code} $1 ${2#"$API"}\n" -X "$1" "$2" \
    -H "Authorization: Bearer $TOKEN" -H "Content-Type: application/json" "${data[@]}"
}

# 1. Catalog role with grants on the silver namespace (inherited by the tables it contains)
call POST "$API/catalogs/$CATALOG/catalog-roles" '{"catalogRole":{"name":"silver_reader"}}'
for privilege in NAMESPACE_READ_PROPERTIES TABLE_LIST TABLE_READ_PROPERTIES TABLE_READ_DATA; do
  call PUT "$API/catalogs/$CATALOG/catalog-roles/silver_reader/grants" \
    "{\"grant\":{\"type\":\"namespace\",\"namespace\":[\"silver\"],\"privilege\":\"$privilege\"}}"
done

# 2. Principal role, granted the catalog role
call POST "$API/principal-roles" '{"principalRole":{"name":"silver_analyst"}}'
call PUT "$API/principal-roles/silver_analyst/catalog-roles/$CATALOG" '{"catalogRole":{"name":"silver_reader"}}'

# 3. Principal: its credentials are returned only once, at creation
RESPONSE=$(curl -sS -X POST "$API/principals" \
  -H "Authorization: Bearer $TOKEN" -H "Content-Type: application/json" \
  -d '{"principal":{"name":"silver_user"}}')
if echo "$RESPONSE" | jq -e '.credentials.clientId' >/dev/null 2>&1; then
  echo "$RESPONSE" | jq -r '"POLARIS_SILVER_USER_CREDENTIAL=\"\(.credentials.clientId):\(.credentials.clientSecret)\""' \
    >> "$CREDENTIALS_FILE"
  echo "201 POST /principals (credentials saved to $CREDENTIALS_FILE)"
else
  echo "principal silver_user not created: $(echo "$RESPONSE" | jq -c '.error.message? // .')"
fi
call PUT "$API/principals/silver_user/principal-roles" '{"principalRole":{"name":"silver_analyst"}}'

# 4. Resulting grants
curl -sS "$API/catalogs/$CATALOG/catalog-roles/silver_reader/grants" \
  -H "Authorization: Bearer $TOKEN" | jq -c '.grants[]'
