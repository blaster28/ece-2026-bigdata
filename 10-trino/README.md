# Lab 10: Deploy Trino with Helm

Johan LIN, ING4 DATA & AI, GRP03

Lab: [10.trino/lab-1-deployment.md](https://github.com/adaltas/ece-bigdata-2026-fall/blob/main/10.trino/lab-1-deployment.md)
Prerequisite: Polaris deployed in module 09 (`../09-polaris/`), table `silver.orders` registered in the catalog `user-j-lin-ece`.

## Goal

Deploy Trino 480 (coordinator only) in my Kubernetes namespace with Helm, and connect it to the Polaris
Iceberg REST catalog as the principal `dev`, so that the lakehouse tables can be queried in SQL.

## Repository content

```
10-trino/
├── README.md
└── trino/deployment/
    ├── Chart.yaml     # umbrella chart: trino 1.45.0 from https://trinodb.github.io/charts
    ├── Chart.lock     # pinned dependency version
    └── values.yaml    # image 480, 0 worker, memory settings, catalog "lakehouse"
```

`trino/deployment/charts/` (downloaded chart archive) is gitignored.

## Architecture

| Component | Kind | Limits | Role |
|---|---|---|---|
| `trino-coordinator` | Deployment | 1000m / 1Gi (heap 700M) | SQL engine, also runs the tasks (`workers: 0`) |
| `polaris` + `polaris-postgresql` | module 09 | 500m / 512Mi + 250m / 256Mi | Iceberg REST catalog and its metastore |
| `vscode-pyspark` | StatefulSet (patched) | 2000m / 2000Mi | my workspace |

The catalog `lakehouse` uses the Iceberg connector:

- metadata through Polaris: `iceberg.catalog.type=rest`, URI `http://polaris:8181/api/catalog`, warehouse
  `user-j-lin-ece`, OAuth2 with the `dev` credential (secret `trino-polaris-oauth`), scope `PRINCIPAL_ROLE:ALL`;
- data files read directly from S3 with the native S3 file system (`fs.native-s3.enabled=true`), path-style access
  to the SeaweedFS endpoint, keys taken from the secret `polaris-s3` of module 09.

Kubernetes objects created for this lab: secret `trino-polaris-oauth` and ConfigMap `trino-polaris-config`. The
ConfigMap `polaris-config` and the secret `polaris-s3` are reused from module 09.

## Steps

1. Lower the VS Code limits to 2000m / 2000Mi (requests 200m / 400Mi): with Trino, the limits of the namespace
   would reach 4292Mi, above the quota of 4Gi. Quota after the restart of the workspace pod:
   `limits.memory 2768Mi/4Gi`, `requests.memory 784Mi/1Gi`.
2. Create the secret `trino-polaris-oauth` from `~/work/.polaris-credentials.env` (outside the repository) and the
   ConfigMap `trino-polaris-config`.
3. `helm dependency update ./trino/deployment`, then `helm upgrade --install trino ./trino/deployment --wait`.

## Results

```
$ helm status trino
STATUS: deployed
REVISION: 1

$ curl -s http://trino:8080/v1/info
{"nodeId":"trino-coordinator-65c645d64f-xgv9r","state":"ACTIVE","nodeVersion":{"version":"480"},
 "environment":"production","coordinator":true,"starting":false,"uptime":"2.30m"}

$ kubectl exec deploy/trino-coordinator -- trino --execute "SHOW SCHEMAS FROM lakehouse"
"information_schema"
"silver"
"system"

$ kubectl exec deploy/trino-coordinator -- trino --execute "SELECT count(*) AS orders FROM lakehouse.silver.orders"
"2829"
```

The schema list comes from Polaris (namespace `silver`), and the count of 2829 orders matches Spark: Trino got the
current metadata file of `silver.orders` from Polaris and read its Parquet file from the bucket.

## Issues met and fixes

- **The patch did not restart the workspace pod**: the quota still showed the old limits (3268Mi). Deleting the pod
  (`kubectl delete pod vscode-pyspark-38344-0`) let the StatefulSet recreate it with the new resources. The VS Code
  session is cut during the restart and `~/.aws` is wiped, so the S3 profile was saved in `~/work/.aws-backup` first
  and restored afterwards.
- **Startup probe failures** (`Failed to connect to localhost port 8080`, then `Server is starting`) during the first
  minute: the JVM was starting, the coordinator became ready without restart.

## Operations notes

- The S3 keys of `polaris-s3` are temporary STS credentials. When they expire, both Polaris and Trino must reload
  them: update the secret, then restart the deployments (`kubectl scale deployment/<name> --replicas=0` then
  `--replicas=1`, as in the Polaris help page, so that two pods never exceed the quota at the same time).
- Trino has no authentication here: the ingress is disabled and the service is only reachable inside the namespace.
- Teardown (not done, Trino is kept for the next labs): `helm uninstall trino`, `helm repo remove trino`, then delete
  the secret `trino-polaris-oauth` and the ConfigMap `trino-polaris-config`.
