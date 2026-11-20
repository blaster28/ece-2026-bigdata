# Lab 09: Deploy Apache Polaris (Iceberg REST catalog)

Johan LIN, ING4 DATA & AI, GRP03

## Goal

Deploy Apache Polaris 1.8.0 in my Kubernetes namespace with Helm, backed by PostgreSQL,
and bootstrap an RBAC model (principals, principal roles, catalog roles, privileges)
for an Iceberg catalog stored on S3 (SeaweedFS). Polaris stays deployed for the next module.

## Repository content






`polaris/deployment/charts/` (downloaded dependency archives) is gitignored.

## Architecture

| Component | Kind | Limits | Role |
|---|---|---|---|
| `polaris-postgresql` | StatefulSet (1Gi PVC) | 250m / 256Mi | metastore (relational-jdbc) |
| `polaris` | Deployment | 500m / 512Mi | Iceberg REST catalog (`:8181`) + management/health (`:8182`) |
| `polaris-bootstrap` | Job (Helm hook) | | creates principals, roles, catalog, grants |
| `vscode-pyspark` | StatefulSet (patched) | 2000m / 2500Mi | my workspace, reduced to free quota |

Configuration objects created before the install:

- ConfigMap `polaris-config`: catalog name, URIs, realm `POLARIS`, base location `s3://<namespace>/warehouse/`, S3 endpoint.
- Secret `polaris-credential`: root principal secret (demo value).
- Secret `polaris-s3`: S3 STS credentials used by Polaris to read and write the warehouse.

## Steps

1. Patch the VS Code StatefulSet limits (2000m CPU, 2500Mi memory) so the namespace quota
   (4 CPU / 4Gi) leaves room for Polaris and PostgreSQL. Quota after restart: `limits.cpu 2/4`, `limits.memory 2500Mi/4Gi`.
2. Create the chart files, the ConfigMap and the two secrets.
3. `helm dependency update polaris/deployment` then `helm upgrade --install polaris polaris/deployment --wait`.
4. Check health: `curl -s http://polaris-mgmt:8182/q/health` returns `"status": "UP"`.
5. Extract the `dev` and `analyst` credentials from the bootstrap Job logs into an env file
   stored outside the repository.

## RBAC result

Bootstrap log summary (`kubectl logs job/polaris-bootstrap`):


| Catalog role | Privileges |
|---|---|
| `catalog_contributor` | CATALOG_MANAGE_ACCESS, CATALOG_MANAGE_METADATA, CATALOG_MANAGE_CONTENT |
| `catalog_reader` | TABLE_READ_DATA, TABLE_READ_PROPERTIES, CATALOG_READ_PROPERTIES |

Checks with a root token on the management API (`/api/management/v1`):

- catalogs: `user-j-lin-ece` (INTERNAL, `s3://user-j-lin-ece/warehouse/`)
- principals: `root`, `dev`, `analyst`
- principal roles: `service_admin`, `data_engineer`, `data_analyst`
- catalog roles: `catalog_admin`, `catalog_contributor`, `catalog_reader`

## Issues met and fixes

- **Cluster API outage**: `kubectl` returned `TLS handshake timeout` on `10.96.0.1:443` for hours,
  so the patched workspace pod could not be restarted. It came back the next day with the new limits.
- **Stale AWS variables**: the pod environment still exported expired `AWS_*` variables, which take
  precedence over `AWS_PROFILE` in the AWS CLI (`InvalidAccessKeyId`). Fixed by unsetting them and loading
  the profile with `aws configure export-credentials --profile default --format env` before creating `polaris-s3`.
- **Interrupted Helm install**: the terminal running `helm --wait` was closed, the release ended in `failed`
  and the bootstrap hook never ran, although Polaris itself was `1/1`. Since the Job hook is
  `post-install,post-upgrade`, a second `helm upgrade --install` (revision 2, run with `nohup`) executed
  the bootstrap without touching the database password (not defined in the secret template).
- **`kubectl get pods` refused** (`You must be logged in`) while other resources worked, so progress was
  monitored with `kubectl get statefulset,deployment,job` and `kubectl get events`.

## Security notes

- Principal credentials are shown only once in the Job logs: they are stored in `~/work/.polaris-credentials.env`
  (mode 600, outside the Git repository) and downloaded locally.
- `polaris-s3` holds temporary STS credentials (about 12h). When they expire, recreate the secret from fresh
  credentials and restart Polaris: `kubectl rollout restart deployment/polaris`.
- The root secret `s3cr3t` is a lab value only.
