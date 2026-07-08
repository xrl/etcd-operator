# ETCD Operator API References

## Packages
- [operator.etcd.io/v1alpha1](#operatoretcdiov1alpha1)


## operator.etcd.io/v1alpha1

Package v1alpha1 contains API Schema definitions for the operator v1alpha1 API group.

### Resource Types
- [EtcdBackup](#etcdbackup)
- [EtcdBackupList](#etcdbackuplist)
- [EtcdCluster](#etcdcluster)
- [EtcdClusterList](#etcdclusterlist)
- [EtcdRestore](#etcdrestore)
- [EtcdRestoreList](#etcdrestorelist)



#### AltNames







_Appears in:_
- [CommonConfig](#commonconfig)
- [ProviderAutoConfig](#providerautoconfig)
- [ProviderCertManagerConfig](#providercertmanagerconfig)

| Field | Description | Default | Validation |
| --- | --- | --- | --- |
| `dnsNames` _string array_ | DNSNames is the expected array of DNS subject alternative names.<br />if empty defaults to $(POD_NAME).$(ETCD_CLUSTER_NAME).$(POD_NAMESPACE).svc.cluster.local |  | Optional: \{\} <br /> |
| `ipAddresses` _IP array_ | IPs is the expected array of IP address subject alternative names. |  | Optional: \{\} <br /> |


#### BackupDestination



BackupDestination describes where a snapshot is uploaded. Exactly one
provider-specific block must be populated and it must match Provider.



_Appears in:_
- [EtcdBackupSpec](#etcdbackupspec)
- [SnapshotLocation](#snapshotlocation)

| Field | Description | Default | Validation |
| --- | --- | --- | --- |
| `provider` _[BackupProvider](#backupprovider)_ | Provider selects the object-storage backend. |  | Enum: [s3 gcs] <br /> |
| `prefix` _string_ | Prefix is an optional key prefix (a.k.a. "folder") within the bucket<br />under which the snapshot object is written. A trailing slash is optional. |  | Optional: \{\} <br /> |
| `secretRef` _[LocalObjectReference](https://kubernetes.io/docs/reference/generated/kubernetes-api/v/#localobjectreference-v1-core)_ | SecretRef references a Secret in the EtcdBackup's namespace holding the<br />credentials for the provider. The expected keys depend on the provider:<br />  - s3:  accessKeyID / secretAccessKey (and optionally sessionToken)<br />  - gcs: serviceAccountJSON (a GCP service-account key)<br />If unset, the controller falls back to ambient credentials available to<br />the operator pod (e.g. IRSA / Workload Identity), which is the<br />recommended production posture. |  | Optional: \{\} <br /> |
| `s3` _[S3DestinationSpec](#s3destinationspec)_ | S3 holds S3-specific destination configuration. Required when<br />Provider is "s3". |  | Optional: \{\} <br /> |
| `gcs` _[GCSDestinationSpec](#gcsdestinationspec)_ | GCS holds GCS-specific destination configuration. Required when<br />Provider is "gcs". |  | Optional: \{\} <br /> |


#### BackupPhase

_Underlying type:_ _string_

BackupPhase is a high-level summary of where an EtcdBackup is in its
lifecycle. It is surfaced on .status.phase for at-a-glance reporting.



_Appears in:_
- [EtcdBackupStatus](#etcdbackupstatus)

| Field | Description |
| --- | --- |
| `Pending` | BackupPhasePending means the backup has been accepted but work has not<br />started yet.<br /> |
| `Snapshotting` | BackupPhaseSnapshotting means a snapshot is being taken from a member.<br /> |
| `Uploading` | BackupPhaseUploading means the snapshot is being uploaded to object storage.<br /> |
| `Completed` | BackupPhaseCompleted means the snapshot was uploaded successfully.<br /> |
| `Failed` | BackupPhaseFailed means the backup failed; see conditions for details.<br /> |


#### BackupProvider

_Underlying type:_ _string_

BackupProvider enumerates the supported object-storage backends a snapshot
can be uploaded to. The set is intentionally open: the controller dispatches
on this value to a pluggable provider implementation in pkg/objectstore, so
adding a new backend is a matter of registering a new provider and a new
value here.

_Validation:_
- Enum: [s3 gcs]

_Appears in:_
- [BackupDestination](#backupdestination)

| Field | Description |
| --- | --- |
| `s3` | BackupProviderS3 uploads the snapshot to an AWS S3 (or S3-compatible)<br />bucket via the aws-sdk-go-v2 provider.<br /> |
| `gcs` | BackupProviderGCS uploads the snapshot to a Google Cloud Storage bucket<br />via the cloud.google.com/go/storage provider.<br /> |


#### BackupReference



BackupReference points at an EtcdBackup in the same namespace.



_Appears in:_
- [SnapshotSource](#snapshotsource)

| Field | Description | Default | Validation |
| --- | --- | --- | --- |
| `name` _string_ | Name is the metadata.name of the EtcdBackup to restore from. |  | MinLength: 1 <br /> |


#### CommonConfig







_Appears in:_
- [ProviderAutoConfig](#providerautoconfig)
- [ProviderCertManagerConfig](#providercertmanagerconfig)

| Field | Description | Default | Validation |
| --- | --- | --- | --- |
| `commonName` _string_ | CommonName is the expected common name X509 certificate subject attribute.<br />Should have a length of 64 characters or fewer to avoid generating invalid CSRs. |  | Optional: \{\} <br /> |
| `organizations` _string array_ | Organization is the expected array of Organization names to be used on the Certificate. |  | Optional: \{\} <br /> |
| `altNames` _[AltNames](#altnames)_ | AltNames contains the domain names and IP addresses that will be added<br />to the x509 certificate SubAltNames fields. The values will be passed<br />directly to the x509.Certificate object. |  |  |
| `validityDuration` _string_ | ValidityDuration is the expected duration until which the certificate will be valid,<br />expects in human-readable duration: 100d12h, if empty defaults to 90d for cert-manager<br />and 365d for auto as per: https://github.com/etcd-io/etcd/blob/b87bc1c3a275d7d4904f4d201b963a2de2264f0d/client/pkg/transport/listener.go#L275 |  | Optional: \{\} <br /> |


#### DataLossInfo



DataLossInfo captures what the operator knows about the data retained by — and
therefore the data potentially lost during — a force-new-cluster rebuild.

The operator cannot enumerate exactly which keys were lost (the members that
held the un-replicated writes are gone), so this records the provable lower
bound on retained state: the survivor's identity and its last committed
revision. Everything the destroyed majority committed beyond SurvivorRevision
is unrecoverable.



_Appears in:_
- [RecoveryStatus](#recoverystatus)

| Field | Description | Default | Validation |
| --- | --- | --- | --- |
| `survivorMemberID` _string_ | SurvivorMemberID is the hex-encoded etcd member ID of the survivor whose<br />data directory was used to rebuild the cluster. |  | Optional: \{\} <br /> |
| `survivorRevision` _integer_ | SurvivorRevision is the key-value store revision present on the survivor at<br />the moment the single-member cluster came back healthy. It is the highest<br />revision guaranteed to be retained; any revision the lost majority committed<br />above this value did not survive the rebuild. |  | Optional: \{\} <br /> |
| `raftIndex` _integer_ | RaftIndex is the survivor's raft committed index at rebuild time, recorded<br />for forensic correlation with member logs. |  | Optional: \{\} <br /> |
| `recoveredTime` _[Time](https://kubernetes.io/docs/reference/generated/kubernetes-api/v/#time-v1-meta)_ | RecoveredTime is when the rebuilt single-member cluster was confirmed<br />healthy and this accounting was captured. |  | Optional: \{\} <br /> |
| `message` _string_ | Message is a human-readable, operator-facing summary of the data-loss<br />situation, e.g. "recovered with possible data loss; rebuilt from member<br /><id> at revision <r>". |  | Optional: \{\} <br /> |


#### EtcdBackup



EtcdBackup is the Schema for the etcdbackups API. It represents a single
point-in-time snapshot of an EtcdCluster uploaded to object storage.



_Appears in:_
- [EtcdBackupList](#etcdbackuplist)

| Field | Description | Default | Validation |
| --- | --- | --- | --- |
| `apiVersion` _string_ | `operator.etcd.io/v1alpha1` | | |
| `kind` _string_ | `EtcdBackup` | | |
| `metadata` _[ObjectMeta](https://kubernetes.io/docs/reference/generated/kubernetes-api/v/#objectmeta-v1-meta)_ | Refer to Kubernetes API documentation for fields of `metadata`. |  |  |
| `spec` _[EtcdBackupSpec](#etcdbackupspec)_ |  |  |  |


#### EtcdBackupList



EtcdBackupList contains a list of EtcdBackup.





| Field | Description | Default | Validation |
| --- | --- | --- | --- |
| `apiVersion` _string_ | `operator.etcd.io/v1alpha1` | | |
| `kind` _string_ | `EtcdBackupList` | | |
| `metadata` _[ListMeta](https://kubernetes.io/docs/reference/generated/kubernetes-api/v/#listmeta-v1-meta)_ | Refer to Kubernetes API documentation for fields of `metadata`. |  |  |
| `items` _[EtcdBackup](#etcdbackup) array_ |  |  |  |


#### EtcdBackupSpec



EtcdBackupSpec defines the desired state of an EtcdBackup.



_Appears in:_
- [EtcdBackup](#etcdbackup)

| Field | Description | Default | Validation |
| --- | --- | --- | --- |
| `clusterRef` _string_ | ClusterRef references the EtcdCluster to snapshot. The cluster must live<br />in the same namespace as this EtcdBackup. |  | MinLength: 1 <br /> |
| `destination` _[BackupDestination](#backupdestination)_ | Destination describes the object-storage target for the snapshot. |  |  |
| `snapshotTimeout` _[Duration](https://kubernetes.io/docs/reference/generated/kubernetes-api/v/#duration-v1-meta)_ | SnapshotTimeout bounds how long the snapshot-save step may run before it<br />is considered failed. Defaults to 10m if unset. |  | Optional: \{\} <br /> |
| `retention` _[RetentionPolicy](#retentionpolicy)_ | Retention, when set, asks the controller to delete older snapshots under<br />the same bucket/prefix once more than RetainCount snapshots exist. A<br />value of 0 (the default) disables retention pruning. |  | Optional: \{\} <br /> |




#### EtcdCluster



EtcdCluster is the Schema for the etcdclusters API.



_Appears in:_
- [EtcdClusterList](#etcdclusterlist)

| Field | Description | Default | Validation |
| --- | --- | --- | --- |
| `apiVersion` _string_ | `operator.etcd.io/v1alpha1` | | |
| `kind` _string_ | `EtcdCluster` | | |
| `metadata` _[ObjectMeta](https://kubernetes.io/docs/reference/generated/kubernetes-api/v/#objectmeta-v1-meta)_ | Refer to Kubernetes API documentation for fields of `metadata`. |  |  |
| `spec` _[EtcdClusterSpec](#etcdclusterspec)_ |  |  |  |






#### EtcdClusterList



EtcdClusterList contains a list of EtcdCluster.





| Field | Description | Default | Validation |
| --- | --- | --- | --- |
| `apiVersion` _string_ | `operator.etcd.io/v1alpha1` | | |
| `kind` _string_ | `EtcdClusterList` | | |
| `metadata` _[ListMeta](https://kubernetes.io/docs/reference/generated/kubernetes-api/v/#listmeta-v1-meta)_ | Refer to Kubernetes API documentation for fields of `metadata`. |  |  |
| `items` _[EtcdCluster](#etcdcluster) array_ |  |  |  |


#### EtcdClusterSpec



EtcdClusterSpec defines the desired state of EtcdCluster.



_Appears in:_
- [EtcdCluster](#etcdcluster)

| Field | Description | Default | Validation |
| --- | --- | --- | --- |
| `size` _integer_ | Size is the expected size of the etcd cluster. |  | Minimum: 1 <br /> |
| `imageRegistry` _string_ | ImageRegistry specifies the container registry that hosts the etcd images.<br />If unset, it defaults to the value provided via the controller's<br />--image-registry flag, which itself defaults to "gcr.io/etcd-development/etcd". |  |  |
| `version` _string_ | Version is the expected version of the etcd container image. |  |  |
| `storageSpec` _[StorageSpec](#storagespec)_ | StorageSpec is the name of the StorageSpec to use for the etcd cluster. If not provided, then each POD just uses the temporary storage inside the container. |  |  |
| `tls` _[EtcdClusterTLS](#etcdclustertls)_ | TLS configures etcd's two independent TLS surfaces (peer and client/server).<br />Each surface is optional and configured fully independently; a nil surface<br />means that surface is served/dialed in cleartext. When TLS itself is nil, the<br />entire cluster (peer + client + operator client) is cleartext, byte-identical<br />to a TLS-free deployment.<br />TLS is effectively create-time: it flows into the pod template and cert mounts.<br />Toggling it on (or off) on a running cluster rolls the StatefulSet into a mixed<br />http/https membership whose peers cannot connect, dropping quorum. The supported<br />path is a NEW TLS cluster plus data migration, not an in-place flip. |  |  |
| `etcdOptions` _string array_ | etcd configuration options are passed as command line arguments to the etcd container, refer to etcd documentation for configuration options applicable for the version of etcd being used. |  |  |
| `quotaBackendBytes` _[Quantity](https://kubernetes.io/docs/reference/generated/kubernetes-api/v/#quantity-resource-api)_ | QuotaBackendBytes is the etcd backend storage quota (--quota-backend-bytes).<br />When exceeded etcd raises a NOSPACE alarm and becomes read-only.<br />Unset means the etcd default (2GiB). etcd recommends at most 8GiB.<br />Must be at least 100Mi: etcd disables the quota for non-positive<br />values, and a byte-scale typo ("8" instead of "8Gi") would alarm the<br />whole cluster read-only on the first pod restart. Lowering the quota<br />below the current DB size has the same read-only effect. |  | Optional: \{\} <br /> |
| `autoCompactionMode` _string_ | AutoCompactionMode selects the MVCC auto-compaction policy (--auto-compaction-mode). |  | Enum: [periodic revision] <br />Optional: \{\} <br /> |
| `autoCompactionRetention` _string_ | AutoCompactionRetention is the retention window for auto compaction<br />(--auto-compaction-retention): a duration such as "5m"/"1h" in periodic<br />mode, or a revision count in revision mode. "0" disables auto compaction. |  | Pattern: `^([0-9]+[smh])+$\|^[0-9]+$` <br />Optional: \{\} <br /> |
| `podTemplate` _[PodTemplate](#podtemplate)_ | PodTemplate is the pod template to use for the etcd cluster. |  |  |
| `metrics` _[MetricsSpec](#metricsspec)_ | Metrics configures Prometheus observability for this cluster. When unset,<br />the operator still exports its own per-cluster domain metrics on its<br />/metrics endpoint, but does not create a PodMonitor for the etcd member<br />pods. See MetricsSpec for the available knobs. |  | Optional: \{\} <br /> |




#### EtcdClusterTLS



EtcdClusterTLS configures etcd's two independent TLS surfaces. Each surface is
optional; a nil surface means that surface is served/dialed in cleartext (http).
The two surfaces are configured fully independently -- different providers,
issuers, and client-cert-auth policy are allowed and expected. Both surfaces nil
is legal and means fully-cleartext (today's default); it is intentional, not an
error, so there is no "at least one surface" validation.



_Appears in:_
- [EtcdClusterSpec](#etcdclusterspec)

| Field | Description | Default | Validation |
| --- | --- | --- | --- |
| `peer` _[TLSSurface](#tlssurface)_ | Peer configures etcd<->etcd (peer) TLS. When nil, peer traffic is cleartext.<br />A configured peer surface REQUIRES a CA-capable issuer shared by all members<br />so members can mutually verify; a self-signed *leaf* issuer cannot form a<br />multi-member cluster. (CA-capability lives on the cert-manager Issuer object,<br />not on this spec, so that check is enforced at reconcile time, not via CEL.) |  | Optional: \{\} <br /> |
| `client` _[TLSSurface](#tlssurface)_ | Client configures client->etcd (server) TLS AND, transitively, the operator's<br />own etcd client identity (the operator authenticates to etcd as a client).<br />When nil, client traffic is cleartext and the operator dials cleartext. |  | Optional: \{\} <br /> |


#### EtcdRestore



EtcdRestore is the Schema for the etcdrestores API. It represents restoring a
snapshot from object storage into a new EtcdCluster.



_Appears in:_
- [EtcdRestoreList](#etcdrestorelist)

| Field | Description | Default | Validation |
| --- | --- | --- | --- |
| `apiVersion` _string_ | `operator.etcd.io/v1alpha1` | | |
| `kind` _string_ | `EtcdRestore` | | |
| `metadata` _[ObjectMeta](https://kubernetes.io/docs/reference/generated/kubernetes-api/v/#objectmeta-v1-meta)_ | Refer to Kubernetes API documentation for fields of `metadata`. |  |  |
| `spec` _[EtcdRestoreSpec](#etcdrestorespec)_ |  |  |  |


#### EtcdRestoreList



EtcdRestoreList contains a list of EtcdRestore.





| Field | Description | Default | Validation |
| --- | --- | --- | --- |
| `apiVersion` _string_ | `operator.etcd.io/v1alpha1` | | |
| `kind` _string_ | `EtcdRestoreList` | | |
| `metadata` _[ListMeta](https://kubernetes.io/docs/reference/generated/kubernetes-api/v/#listmeta-v1-meta)_ | Refer to Kubernetes API documentation for fields of `metadata`. |  |  |
| `items` _[EtcdRestore](#etcdrestore) array_ |  |  |  |


#### EtcdRestoreSpec



EtcdRestoreSpec defines the desired state of an EtcdRestore: take a snapshot
from object storage and bootstrap a new EtcdCluster from it.



_Appears in:_
- [EtcdRestore](#etcdrestore)

| Field | Description | Default | Validation |
| --- | --- | --- | --- |
| `source` _[SnapshotSource](#snapshotsource)_ | Source selects the snapshot to restore. |  |  |
| `target` _[RestoreTarget](#restoretarget)_ | Target describes the EtcdCluster to create and restore into. |  |  |
| `restoreTimeout` _[Duration](https://kubernetes.io/docs/reference/generated/kubernetes-api/v/#duration-v1-meta)_ | RestoreTimeout bounds how long the download+restore step may run before it<br />is considered failed. Defaults to 10m if unset. |  | Optional: \{\} <br /> |




#### GCSDestinationSpec



GCSDestinationSpec describes a Google Cloud Storage upload target.



_Appears in:_
- [BackupDestination](#backupdestination)

| Field | Description | Default | Validation |
| --- | --- | --- | --- |
| `bucket` _string_ | Bucket is the destination GCS bucket name. |  | MinLength: 1 <br /> |
| `endpoint` _string_ | Endpoint overrides the GCS endpoint, enabling GCS-compatible emulators<br />such as fake-gcs-server or the gcloud storage testbench for hermetic,<br />credential-free testing. It must point at the JSON API root the emulator<br />serves (e.g. "http://fake-gcs:9000/storage/v1/"). When set, the client is<br />pointed at this endpoint and runs unauthenticated, mirroring the S3<br />endpoint override that targets MinIO. If empty, the real Google endpoint<br />and the normal credential chain are used. |  | Optional: \{\} <br /> |


#### MemberStatus



MemberStatus defines the observed state of a single etcd member.



_Appears in:_
- [EtcdClusterStatus](#etcdclusterstatus)

| Field | Description | Default | Validation |
| --- | --- | --- | --- |
| `name` _string_ | Name of the etcd member, typically the pod name (e.g., "etcd-cluster-example-0").<br />This can also be the name reported by etcd itself if set. |  | Optional: \{\} <br /> |
| `id` _string_ | ID is the hex-encoded member ID as reported by etcd.<br />This is the canonical identifier for an etcd member. |  |  |
| `version` _string_ | Version of etcd running on this member. |  | Optional: \{\} <br /> |
| `isHealthy` _boolean_ | IsHealthy indicates if the member is considered healthy.<br />A member is healthy if its etcd /health endpoint is reachable and reports OK,<br />and its Status endpoint does not report any 'Errors'. |  |  |
| `isLearner` _boolean_ | IsLearner indicates if the member is currently a learner in the etcd cluster. |  | Optional: \{\} <br /> |
| `isLeader` _boolean_ | IsLeader indicates if this member is currently the cluster leader. |  | Optional: \{\} <br /> |


#### MetricsSpec



MetricsSpec configures Prometheus observability for an EtcdCluster.



_Appears in:_
- [EtcdClusterSpec](#etcdclusterspec)

| Field | Description | Default | Validation |
| --- | --- | --- | --- |
| `enabled` _boolean_ | Enabled toggles emission of the operator's per-cluster domain metrics<br />(member counts, quorum, leader changes, reconcile timings, etc.) for this<br />cluster. When nil it defaults to true: metrics are exported unless<br />explicitly disabled. Disabling drops the cluster's series from the<br />operator's /metrics endpoint. |  | Optional: \{\} <br /> |
| `podMonitor` _[PodMonitorSpec](#podmonitorspec)_ | PodMonitor, when set and enabled, instructs the operator to create and<br />maintain a prometheus-operator PodMonitor selecting this cluster's etcd<br />member pods so that Prometheus scrapes etcd's own /metrics endpoint.<br />This requires the prometheus-operator PodMonitor CRD to be installed in<br />the cluster; if it is absent the operator logs and skips PodMonitor<br />reconciliation without failing the rest of the reconcile. |  | Optional: \{\} <br /> |


#### PodMetadata







_Appears in:_
- [PodTemplate](#podtemplate)

| Field | Description | Default | Validation |
| --- | --- | --- | --- |
| `annotations` _object (keys:string, values:string)_ |  |  |  |
| `labels` _object (keys:string, values:string)_ |  |  |  |


#### PodMonitorSpec



PodMonitorSpec controls creation of a PodMonitor for the etcd member pods.



_Appears in:_
- [MetricsSpec](#metricsspec)

| Field | Description | Default | Validation |
| --- | --- | --- | --- |
| `enabled` _boolean_ | Enabled toggles creation of the PodMonitor. Defaults to false. |  | Optional: \{\} <br /> |
| `interval` _string_ | Interval at which Prometheus scrapes the etcd member pods, e.g. "30s".<br />When empty the prometheus-operator default is used. |  | Optional: \{\} <br /> |
| `port` _string_ | Port is the name of the pod port exposing etcd's /metrics endpoint.<br />Defaults to "client" when empty. |  | Optional: \{\} <br /> |
| `labels` _object (keys:string, values:string)_ | Labels are additional metadata labels to set on the generated<br />PodMonitor. They are merged on top of the operator-managed labels<br />(app.kubernetes.io/name, /managed-by, /instance): user-provided keys<br />take precedence on conflict. This is typically used to satisfy a<br />namespaced Prometheus's podMonitorSelector, e.g. setting<br />"release: kvs-prometheus" so a release-scoped Prometheus discovers and<br />scrapes this PodMonitor. When empty, only the operator-managed labels<br />are applied (today's behavior). |  | Optional: \{\} <br /> |


#### PodSpec







_Appears in:_
- [PodTemplate](#podtemplate)

| Field | Description | Default | Validation |
| --- | --- | --- | --- |
| `affinity` _[Affinity](https://kubernetes.io/docs/reference/generated/kubernetes-api/v/#affinity-v1-core)_ |  |  |  |
| `nodeSelector` _object (keys:string, values:string)_ |  |  |  |
| `tolerations` _[Toleration](https://kubernetes.io/docs/reference/generated/kubernetes-api/v/#toleration-v1-core) array_ |  |  |  |
| `topologySpreadConstraints` _[TopologySpreadConstraint](https://kubernetes.io/docs/reference/generated/kubernetes-api/v/#topologyspreadconstraint-v1-core) array_ | TopologySpreadConstraints describes how member pods should spread across topology<br />domains, typically zones or hosts. The labelSelector must match the member pod<br />labels, e.g. "app: <cluster-name>". |  | Optional: \{\} <br /> |
| `resources` _[ResourceRequirements](https://kubernetes.io/docs/reference/generated/kubernetes-api/v/#resourcerequirements-v1-core)_ | Resources describes the compute resource requirements of the etcd container. |  | Optional: \{\} <br /> |
| `priorityClassName` _string_ | PriorityClassName is the priority class of the member pods,<br />e.g. "system-cluster-critical". |  | Optional: \{\} <br /> |
| `schedulerName` _string_ | SchedulerName dispatches the member pods to a specific scheduler instead of the<br />default one. |  | Optional: \{\} <br /> |


#### PodTemplate







_Appears in:_
- [EtcdClusterSpec](#etcdclusterspec)

| Field | Description | Default | Validation |
| --- | --- | --- | --- |
| `metadata` _[PodMetadata](#podmetadata)_ | Refer to Kubernetes API documentation for fields of `metadata`. |  |  |
| `spec` _[PodSpec](#podspec)_ |  |  |  |


#### ProviderAutoConfig







_Appears in:_
- [ProviderConfig](#providerconfig)

| Field | Description | Default | Validation |
| --- | --- | --- | --- |
| `commonName` _string_ | CommonName is the expected common name X509 certificate subject attribute.<br />Should have a length of 64 characters or fewer to avoid generating invalid CSRs. |  | Optional: \{\} <br /> |
| `organizations` _string array_ | Organization is the expected array of Organization names to be used on the Certificate. |  | Optional: \{\} <br /> |
| `altNames` _[AltNames](#altnames)_ | AltNames contains the domain names and IP addresses that will be added<br />to the x509 certificate SubAltNames fields. The values will be passed<br />directly to the x509.Certificate object. |  |  |
| `validityDuration` _string_ | ValidityDuration is the expected duration until which the certificate will be valid,<br />expects in human-readable duration: 100d12h, if empty defaults to 90d for cert-manager<br />and 365d for auto as per: https://github.com/etcd-io/etcd/blob/b87bc1c3a275d7d4904f4d201b963a2de2264f0d/client/pkg/transport/listener.go#L275 |  | Optional: \{\} <br /> |


#### ProviderCertManagerConfig







_Appears in:_
- [ProviderConfig](#providerconfig)

| Field | Description | Default | Validation |
| --- | --- | --- | --- |
| `commonName` _string_ | CommonName is the expected common name X509 certificate subject attribute.<br />Should have a length of 64 characters or fewer to avoid generating invalid CSRs. |  | Optional: \{\} <br /> |
| `organizations` _string array_ | Organization is the expected array of Organization names to be used on the Certificate. |  | Optional: \{\} <br /> |
| `altNames` _[AltNames](#altnames)_ | AltNames contains the domain names and IP addresses that will be added<br />to the x509 certificate SubAltNames fields. The values will be passed<br />directly to the x509.Certificate object. |  |  |
| `validityDuration` _string_ | ValidityDuration is the expected duration until which the certificate will be valid,<br />expects in human-readable duration: 100d12h, if empty defaults to 90d for cert-manager<br />and 365d for auto as per: https://github.com/etcd-io/etcd/blob/b87bc1c3a275d7d4904f4d201b963a2de2264f0d/client/pkg/transport/listener.go#L275 |  | Optional: \{\} <br /> |
| `issuerKind` _string_ | IssuerKind is the expected kind of Issuer, either "ClusterIssuer" or "Issuer". |  | Enum: [Issuer ClusterIssuer] <br /> |
| `issuerName` _string_ | IssuerName is the expected name of Issuer required to issue a certificate |  |  |
| `issuerGroup` _string_ | IssuerGroup is the API group of the issuer referenced by IssuerKind/IssuerName.<br />Empty defaults to "cert-manager.io". Set this to target issuers served by an<br />external/intermediate issuer group (e.g. an out-of-tree CA controller). |  | Optional: \{\} <br /> |


#### ProviderConfig







_Appears in:_
- [TLSSurface](#tlssurface)

| Field | Description | Default | Validation |
| --- | --- | --- | --- |
| `autoCfg` _[ProviderAutoConfig](#providerautoconfig)_ |  |  |  |
| `certManagerCfg` _[ProviderCertManagerConfig](#providercertmanagerconfig)_ |  |  |  |


#### RecoveryPhase

_Underlying type:_ _string_

RecoveryPhase enumerates the stages of the automatic quorum-loss recovery
state machine. The phases form a strict, idempotent progression so that the
controller can resume recovery safely after a restart or a transient error.



_Appears in:_
- [RecoveryStatus](#recoverystatus)

| Field | Description |
| --- | --- |
| `Detecting` | RecoveryPhaseDetecting means the controller has observed a candidate<br />quorum-loss event but has not yet confirmed it is sustained (it may still<br />be a transient blip that self-heals before the grace window elapses).<br /> |
| `Rebuilding` | RecoveryPhaseRebuilding means sustained quorum loss was confirmed and the<br />controller is rebuilding a single-member cluster from a surviving member<br />using --force-new-cluster.<br /> |
| `ScalingOut` | RecoveryPhaseScalingOut means the single-member cluster is healthy again<br />and the controller is re-adding the remaining members one at a time via<br />the normal learner-add path.<br /> |
| `Completed` | RecoveryPhaseCompleted means the cluster was restored to its desired size<br />and quorum.<br /> |


#### RecoveryStatus



RecoveryStatus records the progress of the quorum-loss recovery state machine.



_Appears in:_
- [EtcdClusterStatus](#etcdclusterstatus)

| Field | Description | Default | Validation |
| --- | --- | --- | --- |
| `phase` _[RecoveryPhase](#recoveryphase)_ | Phase is the current stage of the recovery state machine. |  | Optional: \{\} <br /> |
| `survivorOrdinal` _integer_ | SurvivorOrdinal is the StatefulSet pod ordinal whose data directory was<br />chosen as the surviving source of truth for the rebuild. It is always 0<br />today (the operator keeps ordinal-0's PVC) but is recorded explicitly so<br />the choice is auditable and future survivor-selection policies remain<br />backward compatible. |  | Optional: \{\} <br /> |
| `detectedTime` _[Time](https://kubernetes.io/docs/reference/generated/kubernetes-api/v/#time-v1-meta)_ | DetectedTime is the first time a sustained-quorum-loss candidate was<br />observed. It anchors the grace window used to distinguish true quorum loss<br />from transient single-member failures. |  | Optional: \{\} <br /> |
| `lastTransitionTime` _[Time](https://kubernetes.io/docs/reference/generated/kubernetes-api/v/#time-v1-meta)_ | LastTransitionTime is the time the recovery phase last changed. |  | Optional: \{\} <br /> |
| `message` _string_ | Message is a human-readable description of the current recovery step. |  | Optional: \{\} <br /> |
| `attempts` _integer_ | Attempts is the number of times the operator has committed to a destructive<br />rebuild for this cluster (i.e. entered the Rebuilding phase from Detecting).<br />It is a monotonically increasing counter that survives across recoveries and<br />is never reset, giving operators a durable signal of how often this cluster<br />has needed disaster recovery — repeated recoveries usually point at an<br />underlying infrastructure problem rather than a one-off event. |  | Optional: \{\} <br /> |
| `dataLoss` _[DataLossInfo](#datalossinfo)_ | DataLoss records the data-loss accounting for the most recent rebuild.<br />Quorum-loss recovery via --force-new-cluster is NOT a lossless operation:<br />the rebuilt cluster retains only the writes that were committed to the<br />surviving member's local data directory. Any write that a now-destroyed<br />majority had committed but had not yet replicated to the survivor is GONE.<br />This field surfaces that fact explicitly (alongside a Warning Event, the<br />DataLossPossible condition, and structured logs) so the loss is auditable<br />and never silent. It is nil until a rebuild from a survivor completes. |  | Optional: \{\} <br /> |


#### RestorePhase

_Underlying type:_ _string_

RestorePhase is a high-level summary of where an EtcdRestore is in its
lifecycle. It is surfaced on .status.phase for at-a-glance reporting.



_Appears in:_
- [EtcdRestoreStatus](#etcdrestorestatus)

| Field | Description |
| --- | --- |
| `Pending` | RestorePhasePending means the restore has been accepted but work has not<br />started yet.<br /> |
| `Downloading` | RestorePhaseDownloading means the snapshot is being fetched from object<br />storage.<br /> |
| `Restoring` | RestorePhaseRestoring means the snapshot is being written into a fresh<br />data directory on the target member and the new cluster is bootstrapping.<br /> |
| `Completed` | RestorePhaseCompleted means the snapshot was restored into the target<br />cluster successfully.<br /> |
| `Failed` | RestorePhaseFailed means the restore failed; see conditions for details.<br /> |


#### RestoreTarget



RestoreTarget describes the new EtcdCluster the snapshot is restored into. A
restore always targets a fresh, empty cluster: the restored data directory
must be the cluster's genesis, never overlaid onto an existing member, so the
controller refuses to proceed if a non-empty EtcdCluster of this name already
exists.



_Appears in:_
- [EtcdRestoreSpec](#etcdrestorespec)

| Field | Description | Default | Validation |
| --- | --- | --- | --- |
| `name` _string_ | Name is the metadata.name of the EtcdCluster to create (in the<br />EtcdRestore's namespace) and restore into. It must not already exist<br />unless it exists and reports zero ready members (an empty shell). |  | MinLength: 1 <br /> |
| `size` _integer_ | Size is the desired size of the restored cluster. Defaults to 1 if unset;<br />a single-member restore is the safest default because etcd's snapshot<br />restore bootstraps a single-node cluster which is then grown. |  | Minimum: 1 <br />Optional: \{\} <br /> |
| `version` _string_ | Version is the etcd version for the restored cluster. If empty, the<br />controller inherits the version recorded on the source EtcdBackup (when<br />restoring via backupRef) or requires it to be set explicitly. |  | Optional: \{\} <br /> |
| `storageSpec` _[StorageSpec](#storagespec)_ | StorageSpec optionally requests persistent storage for the restored<br />cluster, mirroring EtcdClusterSpec.StorageSpec. When omitted the restored<br />cluster uses ephemeral container storage. |  | Optional: \{\} <br /> |


#### RetentionPolicy



RetentionPolicy controls automatic pruning of old snapshots.



_Appears in:_
- [EtcdBackupSpec](#etcdbackupspec)

| Field | Description | Default | Validation |
| --- | --- | --- | --- |
| `retainCount` _integer_ | RetainCount is the number of most-recent snapshots to keep under the<br />destination bucket/prefix. Older snapshots are deleted after a<br />successful upload. Zero disables pruning. |  | Minimum: 0 <br /> |


#### S3DestinationSpec



S3DestinationSpec describes an AWS S3 (or S3-compatible) upload target.



_Appears in:_
- [BackupDestination](#backupdestination)

| Field | Description | Default | Validation |
| --- | --- | --- | --- |
| `bucket` _string_ | Bucket is the destination S3 bucket name. |  | MinLength: 1 <br /> |
| `region` _string_ | Region is the AWS region the bucket resides in (e.g. "us-east-1"). |  | Optional: \{\} <br /> |
| `endpoint` _string_ | Endpoint overrides the S3 endpoint, enabling S3-compatible stores such<br />as MinIO or Ceph RGW. If empty, the default AWS endpoint for the region<br />is used. |  | Optional: \{\} <br /> |
| `forcePathStyle` _boolean_ | ForcePathStyle forces path-style addressing (bucket in the path rather<br />than the host). Required by most S3-compatible stores. |  | Optional: \{\} <br /> |


#### SnapshotLocation



SnapshotLocation describes an explicit snapshot object in object storage. It
reuses BackupDestination (provider, bucket, prefix, secretRef) and adds the
object key relative to the destination prefix.



_Appears in:_
- [SnapshotSource](#snapshotsource)

| Field | Description | Default | Validation |
| --- | --- | --- | --- |
| `destination` _[BackupDestination](#backupdestination)_ | Destination is the object-storage location (provider, bucket, prefix and<br />optional secretRef) the snapshot lives in. The same secretRef semantics<br />as EtcdBackup apply: omit it to use ambient credentials. |  |  |
| `key` _string_ | Key is the object key of the snapshot, relative to the destination<br />Prefix (e.g. "my-cluster/backup-1-20260617T010203Z.db"). It is joined<br />with the destination prefix exactly as the backup path joins them, so a<br />value copied verbatim from an EtcdBackup's status round-trips. |  | MinLength: 1 <br /> |


#### SnapshotSource



SnapshotSource describes where the snapshot to restore comes from. Exactly
one of BackupRef or Location must be set; the controller rejects a source
that sets both or neither.



_Appears in:_
- [EtcdRestoreSpec](#etcdrestorespec)

| Field | Description | Default | Validation |
| --- | --- | --- | --- |
| `backupRef` _[BackupReference](#backupreference)_ | BackupRef names a completed EtcdBackup in the same namespace whose<br />uploaded snapshot should be restored. The controller reads that backup's<br />destination (bucket/prefix/provider/secretRef) and recorded<br />snapshotLocation, so this is the convenient path when restoring a backup<br />the operator itself produced. |  | Optional: \{\} <br /> |
| `location` _[SnapshotLocation](#snapshotlocation)_ | Location fully describes a snapshot object independently of any<br />EtcdBackup resource. Use this to restore a snapshot taken out-of-band or<br />after the originating EtcdBackup has been deleted. |  | Optional: \{\} <br /> |


#### StorageSpec







_Appears in:_
- [EtcdClusterSpec](#etcdclusterspec)
- [RestoreTarget](#restoretarget)

| Field | Description | Default | Validation |
| --- | --- | --- | --- |
| `accessModes` _[PersistentVolumeAccessMode](https://kubernetes.io/docs/reference/generated/kubernetes-api/v/#persistentvolumeaccessmode-v1-core)_ |  |  |  |
| `storageClassName` _string_ |  |  |  |
| `pvcName` _string_ |  |  |  |
| `volumeSizeRequest` _[Quantity](https://kubernetes.io/docs/reference/generated/kubernetes-api/v/#quantity-resource-api)_ |  |  |  |
| `volumeSizeLimit` _[Quantity](https://kubernetes.io/docs/reference/generated/kubernetes-api/v/#quantity-resource-api)_ |  |  |  |


#### TLSSurface



TLSSurface is the full, independent TLS configuration for ONE surface (peer or
client). It carries its own provider, provider config (issuer), and mutual
client-cert-auth policy.

The two XValidation rules below are the apply-time anti-misconfiguration
guardrails (plan Decision 2.1/2.2): they reject incoherent provider/config
combinations and mTLS-without-a-resolvable-CA at the API server, so a user
"cannot misconfigure" these from the spec alone. Rules that require reading
cluster objects (issuer existence, peer CA-capability, client/server CA match)
cannot be expressed in CEL and are enforced at reconcile time instead (plan
Decision 2.5-2.7, Decision 3) -- see validateTLSSurface and the cert-manager
provider's validateCertificateConfig.



_Appears in:_
- [EtcdClusterTLS](#etcdclustertls)

| Field | Description | Default | Validation |
| --- | --- | --- | --- |
| `provider` _string_ | Provider selects the certificate provider for THIS surface.<br />Defaults to "auto" when empty. |  | Enum: [auto cert-manager] <br />Optional: \{\} <br /> |
| `providerCfg` _[ProviderConfig](#providerconfig)_ | ProviderCfg is the provider-specific config for THIS surface. |  | Optional: \{\} <br /> |
| `clientCertAuth` _boolean_ | ClientCertAuth toggles mutual cert auth for THIS surface (etcd's<br />--client-cert-auth for the client surface, --peer-client-cert-auth for the<br />peer surface). Defaults to true (mTLS). Set false to serve server-only TLS<br />where clients authenticate by other means (password/token). When true with<br />the cert-manager provider a trusted CA (issuerName) is REQUIRED, enforced by<br />the XValidation rule above. | true | Optional: \{\} <br /> |


