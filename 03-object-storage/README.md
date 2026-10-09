# Lab: Object storage with S3

Johan LIN, ING4 DATA & AI, GRP03

Lab: [03.object-storage/lab-2-s3.md](https://github.com/adaltas/ece-bigdata-2026-fall/blob/main/03.object-storage/lab-2-s3.md)
Platform: Onyxia `vscode-pyspark` service, S3 backend SeaweedFS (`https://s3.seaweedfs.adm.adaltas.cloud`), bucket `user-j-lin-ece`.

## Files

- `lifecycle.json`: lifecycle rules (expire `large/` after 30 days, delete noncurrent versions after 7 days, abort incomplete multipart uploads after 7 days)
- `policy.json`: bucket policy denying `s3:DeleteObject` on `bronze/*`
- `job-upload-bronze.yaml`: Kubernetes Job uploading the datasets to the bronze layer (with the `limits.cpu` fix, see below)

Generated datasets (`*.csv`, `*.bin`) and credentials (`s3.env`) are excluded by `.gitignore`.

## Results

| Section | Result |
|---|---|
| Bucket verification | `user-j-lin-ece` exists |
| s5cmd install | v2.3.0, checksum OK |
| PUT / LIST / GET | Upload OK, `aws s3 ls` and `s5cmd ls` agree (7351 bytes), downloaded file identical (`diff`) |
| MV / DELETE | `mv` = copy + delete (not atomic). Re-running `mv` on a missing key fails with 404 (HeadObject). `rm` on a missing key succeeds: DELETE is idempotent |
| System metadata | Single PUT: ETag = MD5 of the file (`7289c046e686ab80e449aa9704638d35`) |
| User metadata | `source`, `version`, `rows` attached at upload; values are always strings |
| Presigned GET | `curl` returns 200 before expiration, 403 after (tested with `--expires-in 10`) |
| Presigned PUT | boto3 default URL (SigV2) refused with 403. With `Config(signature_version="s3v4")`: 200 and object created |
| Multipart | 200 MiB file: `aws s3 cp` ETag `...-25` (8 MiB parts), `s5cmd` ETag `...-4` (50 MiB parts). ETag != file MD5. No incomplete upload listed |
| Versioning | 2 versions kept (7351 and 5855 bytes), v1 restored by `VersionId`. `rm` creates a delete marker; deleting the delete marker restores the object |
| Lifecycle | The 3 rules are accepted and returned by `get-bucket-lifecycle-configuration` (enforcement not observable during the lab) |
| Bucket policy | Enforced: `DeleteObject` on `bronze/protected.csv` denied with `AccessDenied`, even for an authenticated user. Policy removed afterwards (`NoSuchBucketPolicy`) |
| Object ACL | Single grant: `FULL_CONTROL` for the owner, object not public |
| Consistency | Object read back immediately after PUT (7351 bytes): strong read-after-write consistency |
| Cleanup | All versions and delete markers deleted, versioning `Suspended` (cannot be disabled), lifecycle removed |
| Kubernetes Job | `Complete 1/1` in 23 s, `bronze/users.csv` (7351 B) and `bronze/orders.csv` (331568 B) uploaded |

## Platform-specific observations (SeaweedFS / Onyxia)

- **Global bucket listing**: `aws s3 ls` shows the buckets of all users. Commands must always target `$LAB_BUCKET_NAME`.
- **Endpoint location**: the Onyxia script stores the endpoint under `[services default-s3]`, so `aws configure get endpoint_url` returns nothing; `S3_ENDPOINT_URL` was set explicitly.
- **Folder markers**: SeaweedFS creates a 0-byte object with key `bronze/` on upload (filer-based storage). AWS S3 does not.
- **`ContentEncoding: aws-chunked`** is kept in the object metadata (recent AWS CLI streaming uploads). AWS S3 strips it.
- **Delete marker owner** is `anonymous` while versions are owned by `admin`.
- **SigV2 presigned URLs are refused**; SigV4 must be used.
- **`aws s3 cp ... /dev/null`** downloads the object but returns a non-zero exit code (cannot set the file mtime). Using `-` (stdout) avoids it.

## Kubernetes Job: ResourceQuota issue

The Job from the lab was created, but no pod ever started. The Job events showed:

The namespace `ResourceQuota` (`onyxia-quota`) constrains `limits.cpu`, so every container must declare a CPU limit. The error is not reported by `kubectl apply`, only in the Job events, because pods are created asynchronously by the Job controller. Fix: add `cpu: 500m` under `resources.limits`, delete the Job (the pod template is immutable) and apply it again.

## Questions

**Why are the credentials stored in a Secret and not in the ConfigMap?**
A ConfigMap holds non-sensitive configuration and is readable in plain text by anyone allowed to read the namespace configuration. A Secret is a separate resource type: access can be restricted independently with RBAC, it can be encrypted at rest in etcd, it is not displayed by default, and it can be managed by external secret managers. Note that a Secret is only base64-encoded by default, so RBAC and encryption at rest are what actually protect it.

**The Onyxia credentials are temporary. What happens if the Job runs again tomorrow? How would a production platform provide credentials to a Job?**
The Secret contains a copy of the STS session credentials (`AWS_SESSION_TOKEN`), valid about 12 hours. Tomorrow the upload would fail with `ExpiredToken` / `AccessDenied`, and the Secret would have to be recreated by hand. In production, credentials are not copied into a static Secret: the pod gets a workload identity (Kubernetes ServiceAccount token exchanged with STS `AssumeRoleWithWebIdentity`, such as IRSA on EKS or Workload Identity on GKE), short-lived credentials renewed automatically, or a secret manager (Vault, External Secrets Operator) that injects and rotates them. Access is scoped to the minimum (write on `bronze/` only).

**How would you turn this Job into a daily ingestion?**
Use a `CronJob` with the same pod template, for example `schedule: "0 2 * * *"`, with `concurrencyPolicy: Forbid` and history limits. The data should be read from the source system instead of a ConfigMap (1 MiB limit), and the keys partitioned by date (`bronze/users/date=YYYY-MM-DD/users.csv`) to keep each run and allow reprocessing. For dependencies between steps, retries and monitoring, an orchestrator (Airflow, Argo Workflows) is more suitable.
