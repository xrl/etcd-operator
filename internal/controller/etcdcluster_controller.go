/*
Copyright 2024.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package controller

import (
	"context"
	"fmt"
	"strings"
	"time"

	certv1 "github.com/cert-manager/cert-manager/pkg/apis/certmanager/v1"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	policyv1 "k8s.io/api/policy/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/tools/events"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	controllerruntime "sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/log"

	ecv1alpha1 "go.etcd.io/etcd-operator/api/v1alpha1"
	"go.etcd.io/etcd-operator/internal/etcdutils"
	"go.etcd.io/etcd-operator/internal/metrics"
	etcdversions "go.etcd.io/etcd/api/v3/version"
	clientv3 "go.etcd.io/etcd/client/v3"
)

const (
	requeueDuration = 10 * time.Second
)

// EtcdClusterReconciler reconciles a EtcdCluster object
type EtcdClusterReconciler struct {
	client.Client
	Scheme *runtime.Scheme
	// PodReader reads pods straight from the API server. Reading pods through
	// the cached client would lazily start a cluster-wide pod informer.
	PodReader     client.Reader
	Recorder      events.EventRecorder
	ImageRegistry string

	// MaxConcurrentReconciles is the number of reconcile workers the controller
	// runs in parallel. controller-runtime's default is 1.
	//
	// Each EtcdCluster is reconciled on its own workqueue key (the queue dedups
	// by namespaced name), so a single cluster is NEVER reconciled by two workers
	// at once; concurrency only ever parallelizes DISTINCT clusters. Because a
	// reconcile here is relatively heavy and long-running (StatefulSet patches,
	// member-list/health RPCs against managed etcd, certificate work), a small
	// pool meaningfully improves throughput when many clusters need attention at
	// the same time. The cost of a larger pool is more simultaneous apiserver and
	// managed-etcd load, so wise operators should tune this for their fleet.
	//
	// A value <= 0 falls back to controller-runtime's default of 1, which stays
	// behaviorally safe.
	MaxConcurrentReconciles int

	// clusterHealthFn probes the health of the given endpoints. It defaults to
	// etcdutils.ClusterHealth and exists as a seam so the recovery state machine's
	// survivor-health gate can be unit-tested without a live etcd. Resolved lazily
	// via clusterHealth; nil means "use the real implementation".
	//
	// Integration note: the seam intentionally takes only endpoints. The post-T6
	// TLS reshape made etcdutils.ClusterHealth take a *tls.Config, so the PRODUCTION
	// fallback in clusterHealth builds the operator's client TLS config from the
	// cluster; the test seam stays cleartext because stubs never reach etcd.
	clusterHealthFn func(eps []string) ([]etcdutils.EpHealth, error)
}

// clusterHealth probes endpoint health via the injected seam, falling back to the
// real implementation when unset (the production path).
//
// On the production path it builds the operator's etcd-client TLS config from the
// cluster (nil when the client surface is cleartext) so the health RPC presents
// the right identity post-T6; the test seam bypasses this entirely.
func (r *EtcdClusterReconciler) clusterHealth(ctx context.Context, ec *ecv1alpha1.EtcdCluster, eps []string) ([]etcdutils.EpHealth, error) {
	if r.clusterHealthFn != nil {
		return r.clusterHealthFn(eps)
	}
	tlsConfig, err := buildClientTLSConfig(ctx, ec, r.Client)
	if err != nil {
		return nil, err
	}
	return etcdutils.ClusterHealth(eps, tlsConfig)
}

// reconcileState holds all transient data for a single reconciliation loop.
// Every phase of Reconcile stores intermediate information here so that
// subsequent phases can operate without additional lookups.
type reconcileState struct {
	cluster        *ecv1alpha1.EtcdCluster      // cluster custom resource currently being reconciled
	sts            *appsv1.StatefulSet          // associated StatefulSet for the cluster
	memberListResp *clientv3.MemberListResponse // member list fetched from the etcd cluster
	memberHealth   []etcdutils.EpHealth         // health information for each etcd member
	tls            tlsReadiness                 // verdict on the cluster's TLS surfaces (drives the TLSReady condition)
	alarms         []etcdutils.MemberAlarm      // active etcd alarms (e.g. NOSPACE)
}

// +kubebuilder:rbac:groups=operator.etcd.io,resources=etcdclusters,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=operator.etcd.io,resources=etcdclusters/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=operator.etcd.io,resources=etcdclusters/finalizers,verbs=update
// +kubebuilder:rbac:groups=apps,resources=statefulsets,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=policy,resources=poddisruptionbudgets,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=core,resources=services,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=core,resources=configmaps,verbs=get;list;watch;create;update;patch;delete
// Quorum-loss recovery reads the survivor pod (cached client => list+watch) to
// confirm it exists before arming the irreversible --force-new-cluster rebuild;
// quorum-gated upgrades delete one outdated pod at a time (OnDelete strategy).
// +kubebuilder:rbac:groups="",resources=pods,verbs=get;list;watch;delete
// +kubebuilder:rbac:groups="",resources=events,verbs=create;patch;get;list;update
// +kubebuilder:rbac:groups=events.k8s.io,resources=events,verbs=create;patch
// +kubebuilder:rbac:groups="",resources=secrets,verbs=get;list;watch;create;patch;update;delete
// +kubebuilder:rbac:groups="cert-manager.io",resources=certificates,verbs=get;list;watch;create;patch;update;delete
// +kubebuilder:rbac:groups="cert-manager.io",resources=clusterissuers,verbs=get;list;watch
// +kubebuilder:rbac:groups="cert-manager.io",resources=issuers,verbs=get;list;watch
// +kubebuilder:rbac:groups="monitoring.coreos.com",resources=podmonitors,verbs=get;list;watch;create;update;patch;delete

// Reconcile orchestrates a single reconciliation cycle for an EtcdCluster. It
// sequentially fetches resources, ensures primitive objects exist, checks the
// health of the etcd cluster and then adjusts its state to match the desired
// specification. Each phase is handled by a dedicated helper method.
//
// For more details on the controller-runtime Reconcile contract see:
// https://pkg.go.dev/sigs.k8s.io/controller-runtime/pkg/reconcile
func (r *EtcdClusterReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	var state *reconcileState
	var res ctrl.Result
	var err error

	start := time.Now()

	// Defer status update to ensure it's called regardless of return path.
	// Reconcile timing/error metrics and per-cluster gauges are only recorded when
	// the cluster still exists: on the NotFound path fetchAndValidateState already
	// dropped the series via DeleteClusterMetrics, so observing here would
	// re-create them for a deleted cluster and defeat that cardinality bound.
	defer func() {
		if state != nil {
			metrics.ObserveReconcile(req.Namespace, req.Name, time.Since(start).Seconds(), err)
			if statusErr := r.updateStatus(ctx, state, err); statusErr != nil {
				// Log but don't override the main reconciliation error
				log.FromContext(ctx).Error(statusErr, "Failed to update status")
			}
		}
	}()

	state, res, err = r.fetchAndValidateState(ctx, req)
	if state == nil || err != nil {
		return res, err
	}

	if res, err = r.bootstrapStatefulSet(ctx, state); err != nil || !res.IsZero() {
		return res, err
	}

	healthErr := r.performHealthChecks(ctx, state)

	// Reconcile the PDB even when the health check errors: healthCheck still
	// returns the member list for unhealthy clusters, which is exactly when
	// disruption protection matters most. A PDB write failure is logged but
	// never blocks health handling, quorum recovery, or cluster reconciliation.
	var stsReplicas int32
	if state.sts != nil && state.sts.Spec.Replicas != nil {
		stsReplicas = *state.sts.Spec.Replicas
	}
	pdbErr := reconcilePodDisruptionBudget(
		ctx, log.FromContext(ctx), r.Client, state.cluster, state.memberListResp, stsReplicas, r.Scheme,
	)
	if pdbErr != nil {
		log.FromContext(ctx).Error(pdbErr, "Failed to reconcile PodDisruptionBudget")
	}

	// A pod replaced during an upgrade may never come back healthy; without
	// this the failed health check would keep template and pod convergence
	// unreachable forever.
	if healthErr != nil {
		if res, handled := r.recoverDegradedUpgrade(ctx, state); handled {
			return res, nil
		}
	}

	// While the StatefulSet is still converging (e.g. image pull after bootstrap)
	// an unreachable member is expected; requeue instead of counting an error.
	// This also keeps quorum-loss recovery out of ordinary rollouts.
	if healthErr != nil && !isStatefulSetSettled(state.sts) {
		log.FromContext(ctx).Info("Health check failed while StatefulSet is converging, requeuing",
			"reason", healthErr.Error())
		return ctrl.Result{RequeueAfter: requeueDuration}, nil
	}

	// Quorum-loss recovery gate. A failed health check on a multi-member cluster
	// can mean the cluster has permanently lost quorum (a majority of members are
	// gone) and cannot self-heal. maybeRecoverQuorum inspects the observed member
	// health, and — only on sustained, true quorum loss — drives an idempotent
	// disaster-recovery state machine (rebuild-from-survivor + re-add members).
	// While it owns the reconcile (handled=true) we requeue and skip normal
	// scaling so the two paths never fight. See quorum_recovery.go.
	if handled, requeueAfter, recErr := r.maybeRecoverQuorum(ctx, state, state.memberHealth, healthErr); handled || recErr != nil {
		return ctrl.Result{RequeueAfter: requeueAfter}, recErr
	}

	// During recovery scale-out the recovery state machine delegates membership
	// re-adds to reconcileClusterState below; a transient per-member health error
	// (e.g. a freshly added learner not yet caught up) must NOT short-circuit that
	// path, or recovery would stall. Outside recovery, a health error is fatal.
	if healthErr != nil && !recoveryActive(state.cluster) {
		return ctrl.Result{}, healthErr
	}

	res, err = r.reconcileClusterState(ctx, state)
	if err == nil && res.IsZero() && pdbErr != nil {
		// Nothing else requeues; surface the PDB failure for backoff. Assign
		// to err so the deferred status/metrics closure observes it too.
		err = pdbErr
	}
	return res, err
}

// fetchAndValidateState retrieves the EtcdCluster and its StatefulSet and ensures
// the StatefulSet, if present, is owned by the cluster. It returns a populated
// reconcileState for use in later phases. A non-empty ctrl.Result requests a
// requeue when transient issues occur.
func (r *EtcdClusterReconciler) fetchAndValidateState(ctx context.Context, req ctrl.Request) (*reconcileState, ctrl.Result, error) {
	logger := log.FromContext(ctx)

	ec := &ecv1alpha1.EtcdCluster{}
	if err := r.Get(ctx, req.NamespacedName, ec); err != nil {
		if errors.IsNotFound(err) {
			logger.Info("EtcdCluster resource not found. Ignoring since object may have been deleted")
			// Drop any metric series for the deleted cluster to bound cardinality.
			metrics.DeleteClusterMetrics(req.Namespace, req.Name)
			return nil, ctrl.Result{}, nil
		}
		return nil, ctrl.Result{}, err
	}

	// Determine desired etcd image registry
	if ec.Spec.ImageRegistry == "" {
		ec.Spec.ImageRegistry = r.ImageRegistry
	}

	// Reconcile-time backstop for the apply-time CEL rules (see validateTLS). Reject
	// an incoherent TLS spec by requeuing rather than silently proceeding into cert
	// provisioning, which would otherwise fail deep with a less actionable error.
	if errs := validateTLS(ec); len(errs) > 0 {
		agg := errs.ToAggregate()
		logger.Error(agg, "invalid TLS configuration; not reconciling until fixed",
			"etcdCluster", ec.Name)
		// Reuses the ClientCertificateError reason: this path predates evaluateTLSReadiness
		// (the spec is too malformed to evaluate surfaces), so there is no per-surface verdict
		// reason to emit; the aggregated CEL message in the note carries the specifics.
		r.Recorder.Eventf(ec, nil, corev1.EventTypeWarning, reasonClientCertificateError,
			"ValidateTLS", "invalid TLS configuration: %v", agg)
		return nil, ctrl.Result{RequeueAfter: requeueDuration}, nil
	}

	// The operator authenticates to etcd as a client, so it needs its own client
	// identity iff the CLIENT surface is configured (independent of the peer surface).
	if clientTLSEnabled(ec) {
		if err := createClientCertificate(ctx, ec, r.Client); err != nil {
			// The data path relies on this client certificate existing; surface
			// the failure (log + Event) and requeue with backoff rather than
			// proceeding/swallowing, so it is retried once provisioning recovers.
			logger.Error(err, "Failed to create operator client certificate", "etcdCluster", ec.Name)
			r.Recorder.Eventf(ec, nil, corev1.EventTypeWarning, reasonClientCertificateError,
				"CreateClientCertificate", "failed to create operator client certificate: %v", err)
			// This nil-error requeue returns a nil reconcileState, so the deferred
			// updateStatus never runs; without a direct write here a cluster stuck
			// on cert provisioning would loop forever with a completely empty
			// .status (Events being the only signal).
			r.markClientCertificateFailure(ctx, ec, err)
			return nil, ctrl.Result{RequeueAfter: requeueDuration}, nil
		}
	} else {
		// Cleartext (no client surface) is a supported mode; log at Info, not Error,
		// so legitimately-cleartext clusters don't spam error logs every reconcile.
		logger.Info("client TLS surface not configured; operator dials etcd in cleartext (not recommended for production)",
			"etcdCluster", ec.Name)
	}

	// Evaluate the configured TLS surfaces (Recorder-free verdict), emit any failure
	// Event at this controller boundary, and stash the verdict for the TLSReady
	// condition. This is the single place the runtime peer-CA / issuer / client-cert
	// checks are surfaced as Events; the verdict itself is computed without the
	// Recorder so the util/status helpers stay upstream-clean.
	tlsState := evaluateTLSReadiness(ctx, ec, r.Client)
	r.recordTLSEvent(ec, tlsState)

	logger.Info("Reconciling EtcdCluster", "spec", ec.Spec)

	sts, err := getStatefulSet(ctx, r.Client, ec.Name, ec.Namespace)
	if err != nil {
		if errors.IsNotFound(err) {
			sts = nil
		} else {
			logger.Error(err, "Failed to get StatefulSet. Requesting requeue")
			return nil, ctrl.Result{RequeueAfter: requeueDuration}, nil
		}
	}

	if sts != nil {
		if err := checkStatefulSetControlledByEtcdOperator(ec, sts); err != nil {
			logger.Error(err, "StatefulSet is not controlled by this EtcdCluster resource")
			// Omit the foreign StatefulSet so the status update cannot derive
			// replica counts or Progressing from a resource we do not manage.
			return &reconcileState{cluster: ec}, ctrl.Result{}, err
		}

		// If the version to be reconciled is unsupported, throw an error.
		if len(sts.Spec.Template.Spec.Containers) == 0 {
			logger.Error(err, "StatefulSet has no containers yet")
			return &reconcileState{cluster: ec, sts: sts, tls: tlsState}, ctrl.Result{}, nil
		}
		stsImage := sts.Spec.Template.Spec.Containers[0].Image
		// Note: createOrPatchStatefulSet() only supports the "registry-path:image" format.
		// TODO: switch to using the version from ec.Status eventually.
		//   https://github.com/etcd-io/etcd-operator/pull/278/changes#r2764796805
		idx := strings.Index(stsImage, ":")
		if idx == -1 {
			logger.Error(err, "could not extract image version from StatefulSet image",
				"image", stsImage)
			return &reconcileState{cluster: ec, sts: sts, tls: tlsState}, ctrl.Result{}, nil
		}
		currentVersion := stsImage[idx+1:]
		// Prefer the observed cluster version: the template tag may name an
		// image that never ran (e.g. a nonexistent patch release), which would
		// make every rollback look like a downgrade and wedge the cluster.
		if ec.Status.CurrentVersion != "" {
			currentVersion = ec.Status.CurrentVersion
		}
		targetVersion := ec.Spec.Version

		// Only handle cases when there is a version change. Compare on the
		// normalized (v-stripped) form so the v-prefixed live image tag and a bare
		// spec.version describing the same release are not seen as a change.
		if normalizeEtcdVersion(currentVersion) != normalizeEtcdVersion(targetVersion) {
			// TODO: consider adding an option in the CRD called allowCustomImageUpgrade to make
			// the behavior here optional:
			//   https://github.com/etcd-io/etcd-operator/pull/278/changes#r2764717418
			canParse, err := validateEtcdUpgradePath(etcdversions.AllVersions, currentVersion, targetVersion)
			if !canParse {
				logger.Info("error when parsing reconcile versions; it is your responsibility "+
					"to validate if the upgrade path is supported",
					"current", currentVersion,
					"target", targetVersion,
					"error", err,
				)
				return &reconcileState{cluster: ec, sts: sts, tls: tlsState}, ctrl.Result{}, nil
			} else {
				if err != nil {
					logger.Error(err, "unsupported upgrade path between current and target versions",
						"current", currentVersion,
						"target", targetVersion,
					)
					return &reconcileState{cluster: ec, sts: sts}, ctrl.Result{}, err
				}
				logger.Info("upgrade path between current and target versions is supported",
					"current", currentVersion,
					"target", targetVersion)
			}
		}
	}

	return &reconcileState{cluster: ec, sts: sts, tls: tlsState}, ctrl.Result{}, nil
}

// bootstrapStatefulSet ensures that the foundational Kubernetes objects for
// a cluster exist and are correctly initialized. It creates the StatefulSet (initially
// with 0 replicas) and the headless Service if necessary. When either resource
// is created or the StatefulSet is scaled from zero to one replica, the returned
// ctrl.Result requests a requeue so the next reconciliation loop can observe the
// new state. The reconcileState is updated with the current StatefulSet.
func (r *EtcdClusterReconciler) bootstrapStatefulSet(ctx context.Context, s *reconcileState) (ctrl.Result, error) {
	logger := log.FromContext(ctx)
	requeue := false
	var err error

	switch {
	case s.sts == nil:
		logger.Info("Creating StatefulSet with 0 replica", "expectedSize", s.cluster.Spec.Size)
		s.sts, err = reconcileStatefulSet(ctx, logger, s.cluster, r.Client, 0, r.Scheme)
		if err != nil {
			return ctrl.Result{}, err
		}
		requeue = true

	case s.sts.Spec.Replicas != nil && *s.sts.Spec.Replicas == 0:
		logger.Info("StatefulSet has 0 replicas. Trying to create a new cluster with 1 member")
		s.sts, err = reconcileStatefulSet(ctx, logger, s.cluster, r.Client, 1, r.Scheme)
		if err != nil {
			return ctrl.Result{}, err
		}
		requeue = true
	}

	if err = createHeadlessServiceIfNotExist(ctx, logger, r.Client, s.cluster, r.Scheme); err != nil {
		return ctrl.Result{}, err
	}

	if requeue {
		return ctrl.Result{RequeueAfter: requeueDuration}, nil
	}
	return ctrl.Result{}, nil
}

// performHealthChecks obtains the member list and health status from the etcd
// cluster specified in the StatefulSet. Results are stored on the reconcileState
// for later reconciliation steps.
func (r *EtcdClusterReconciler) performHealthChecks(ctx context.Context, s *reconcileState) error {
	logger := log.FromContext(ctx)
	logger.Info("Now checking health of the cluster members")
	var err error
	s.memberListResp, s.memberHealth, err = healthCheck(ctx, s.cluster, r.Client, s.sts, logger)

	// Poll alarms even when the health check errored: a NOSPACE alarm makes
	// members report unhealthy, and the deferred status update still runs.
	if s.memberListResp != nil {
		if tlsConfig, terr := buildClientTLSConfig(ctx, s.cluster, r.Client); terr != nil {
			logger.Error(terr, "failed to build client TLS config for alarm listing")
		} else if alarms, aerr := etcdutils.AlarmList(
			clientEndpointsFromStatefulsets(s.sts, clientScheme(s.cluster)), tlsConfig); aerr != nil {
			logger.Error(aerr, "failed to list etcd alarms")
		} else {
			s.alarms = alarms
		}
		r.reportNospaceAlarms(s)
	}

	if err != nil {
		return fmt.Errorf("health check failed: %w", err)
	}
	return nil
}

// memberNameForID resolves an etcd member ID to its name, falling back to the hex ID.
func memberNameForID(resp *clientv3.MemberListResponse, id uint64) string {
	if resp != nil {
		for _, m := range resp.Members {
			if m.ID == id && m.Name != "" {
				return m.Name
			}
		}
	}
	return fmt.Sprintf("%x", id)
}

// reportNospaceAlarms emits a Warning event for each member with an active NOSPACE alarm.
func (r *EtcdClusterReconciler) reportNospaceAlarms(s *reconcileState) {
	if r.Recorder == nil {
		return
	}
	for _, a := range s.alarms {
		if a.Type != "NOSPACE" {
			continue
		}
		name := memberNameForID(s.memberListResp, a.MemberID)
		r.Recorder.Eventf(s.cluster, nil, corev1.EventTypeWarning, "DatabaseQuotaExceeded", "AlarmDetected",
			"etcd member %s has an active NOSPACE alarm; cluster is read-only. Compact and defragment, then `etcdctl alarm disarm`. A raised spec.quotaBackendBytes rolls out automatically once members report healthy again.", name)
	}
}

// reconcileClusterState compares the desired cluster size with the observed
// etcd member list and StatefulSet replica count. It performs scaling actions
// and handles learner promotion when needed. A ctrl.Result with a requeue
// instructs the controller to retry after adjustments.
func (r *EtcdClusterReconciler) reconcileClusterState(ctx context.Context, s *reconcileState) (ctrl.Result, error) {
	logger := log.FromContext(ctx)
	memberCnt := 0
	if s.memberListResp != nil {
		memberCnt = len(s.memberListResp.Members)
	}

	// Don't act on a spec the StatefulSet controller hasn't observed yet (stale cache after our own write).
	if !statefulSetSpecObserved(s.sts) {
		logger.Info("StatefulSet spec not observed by its controller yet, requeuing",
			"generation", s.sts.Generation, "observedGeneration", s.sts.Status.ObservedGeneration)
		return ctrl.Result{RequeueAfter: requeueDuration}, nil
	}

	targetReplica := *s.sts.Spec.Replicas
	var err error

	// Build the operator's etcd-client TLS config once (nil when the client surface
	// is cleartext) and derive client endpoints from the client surface's scheme.
	clientTLSConfig, err := buildClientTLSConfig(ctx, s.cluster, r.Client)
	if err != nil {
		r.Recorder.Eventf(s.cluster, nil, corev1.EventTypeWarning, reasonClientCertificateError,
			"BuildClientTLSConfig", "failed to build operator client TLS config: %v", err)
		return ctrl.Result{}, err
	}
	cScheme := clientScheme(s.cluster)

	// The number of replicas in the StatefulSet doesn't match the number of etcd members in the cluster.
	if int(targetReplica) != memberCnt {
		logger.Info("The expected number of replicas doesn't match the number of etcd members in the cluster", "targetReplica", targetReplica, "memberCnt", memberCnt)
		if int(targetReplica) < memberCnt {
			logger.Info("An etcd member was added into the cluster, but the StatefulSet hasn't scaled out yet")
			newReplicaCount := targetReplica + 1
			logger.Info("Increasing StatefulSet replicas to match the etcd cluster member count", "oldReplicaCount", targetReplica, "newReplicaCount", newReplicaCount)
			if _, err := reconcileStatefulSet(ctx, logger, s.cluster, r.Client, newReplicaCount, r.Scheme); err != nil {
				return ctrl.Result{}, err
			}
		} else {
			logger.Info("An etcd member was removed from the cluster, but the StatefulSet hasn't scaled in yet")
			newReplicaCount := targetReplica - 1
			logger.Info("Decreasing StatefulSet replicas to remove the unneeded Pod.", "oldReplicaCount", targetReplica, "newReplicaCount", newReplicaCount)
			if _, err := reconcileStatefulSet(ctx, logger, s.cluster, r.Client, newReplicaCount, r.Scheme); err != nil {
				return ctrl.Result{}, err
			}
		}
		return ctrl.Result{RequeueAfter: requeueDuration}, nil
	}

	// Membership mutations below must only run against a settled StatefulSet. This gate sits
	// after mismatch recovery on purpose: an interrupted scale-in leaves an orphaned pod whose
	// etcd has exited, so the StatefulSet can never settle until recovery shrinks it.
	// Spec-driven scale-in is exempt: reaching here means every member passed health checks,
	// so removal is quorum-safe, and the member removed first is the last ordinal — often the
	// very pod whose unreadiness prevents settling. Gating it would wedge a shrink away from
	// a broken pod.
	scaleIn := targetReplica > int32(s.cluster.Spec.Size)
	if !scaleIn && !isStatefulSetSettled(s.sts) {
		logger.Info("StatefulSet replicas not ready yet, requeuing",
			"readyReplicas", s.sts.Status.ReadyReplicas, "replicas", targetReplica)
		// Health checks passed but pods lag: normal briefly after a scale-out, anomalous
		// if persistent (e.g. NotReady kubelet with etcd still serving).
		r.warnEvent(s.cluster, "StatefulSetNotSettled", "Requeue",
			"waiting for %d/%d ready replicas before mutating etcd membership",
			s.sts.Status.ReadyReplicas, targetReplica)
		return ctrl.Result{RequeueAfter: requeueDuration}, nil
	}

	var (
		learnerStatus *clientv3.StatusResponse
		learner       uint64
		leaderStatus  *clientv3.StatusResponse
	)

	if memberCnt > 0 {
		// Find the leader status
		_, leaderStatus = etcdutils.FindLeaderStatus(s.memberHealth, logger)
		if leaderStatus == nil {
			// If the leader is not available, wait for the leader to be elected
			return ctrl.Result{}, fmt.Errorf("couldn't find leader, memberCnt: %d", memberCnt)
		}

		learner, learnerStatus = etcdutils.FindLearnerStatus(s.memberHealth, logger)
		if learner > 0 {
			// There is at least one learner. Try to promote it if it's ready; otherwise requeue and wait.
			logger.Info("Learner found", "learnedID", learner, "learnerStatus", learnerStatus)
			if etcdutils.IsLearnerReady(leaderStatus, learnerStatus) {
				logger.Info("Learner is ready to be promoted to voting member", "learnerID", learner)
				logger.Info("Promoting the learner member", "learnerID", learner)
				eps := clientEndpointsFromStatefulsets(s.sts, cScheme)
				eps = eps[:(len(eps) - 1)]
				if err := etcdutils.PromoteLearner(eps, learner, clientTLSConfig); err != nil {
					// The member is not promoted yet, so we error out and requeue via the caller.
					return ctrl.Result{}, err
				}
			} else {
				// Learner is not yet ready. We can't add another learner or proceed further until this one is promoted.
				logger.Info("The learner member isn't ready to be promoted yet", "learnerID", learner)
				return ctrl.Result{RequeueAfter: requeueDuration}, nil
			}
		}
	}

	if targetReplica == int32(s.cluster.Spec.Size) {
		return r.reconcileVersionUpgrade(ctx, s)
	}

	eps := clientEndpointsFromStatefulsets(s.sts, cScheme)

	// If there are no learners left, we can proceed to scale the cluster towards the desired size.
	// When there are no members to add, the controller will requeue above and this block won't execute.
	if targetReplica < int32(s.cluster.Spec.Size) {
		// scale out
		_, peerURL := peerEndpointForOrdinalIndex(s.cluster, int(targetReplica))
		targetReplica++
		logger.Info("[Scale out] adding a new learner member to etcd cluster", "peerURLs", peerURL)
		if _, err := etcdutils.AddMember(eps, []string{peerURL}, true, clientTLSConfig); err != nil {
			return ctrl.Result{}, err
		}

		logger.Info("Learner member added successfully", "peerURLs", peerURL)

		// gofail: var exceptionAfterMemberAdd struct{}

		if s.sts, err = reconcileStatefulSet(ctx, logger, s.cluster, r.Client, targetReplica, r.Scheme); err != nil {
			return ctrl.Result{}, err
		}

		return ctrl.Result{RequeueAfter: requeueDuration}, nil
	}

	if targetReplica > int32(s.cluster.Spec.Size) {
		// scale in
		targetReplica--
		logger = logger.WithValues("targetReplica", targetReplica, "expectedSize", s.cluster.Spec.Size)

		eps = eps[:targetReplica]
		if err := removeScaleInMember(logger, s, leaderStatus.Header.MemberId, eps,
			cScheme, clientTLSConfig, etcdutils.MoveLeader, etcdutils.RemoveMember); err != nil {
			return ctrl.Result{}, err
		}

		// gofail: var exceptionAfterMemberDelete struct{}

		if s.sts, err = reconcileStatefulSet(ctx, logger, s.cluster, r.Client, targetReplica, r.Scheme); err != nil {
			return ctrl.Result{}, err
		}

		return ctrl.Result{RequeueAfter: requeueDuration}, nil
	}

	// Ensure every etcd member reports itself healthy before declaring success.
	allMembersHealthy, err := areAllMembersHealthy(ctx, s.cluster, r.Client, s.sts, logger)
	if err != nil {
		return ctrl.Result{}, err
	}

	if !allMembersHealthy {
		// Requeue until the StatefulSet settles and all members are healthy.
		return ctrl.Result{RequeueAfter: requeueDuration}, nil
	}

	logger.Info("EtcdCluster reconciled successfully")
	return ctrl.Result{}, nil
}

// warnEvent emits a Warning event on the cluster; no-op when no Recorder is wired (unit tests).
func (r *EtcdClusterReconciler) warnEvent(obj runtime.Object, reason, action, note string, args ...interface{}) {
	if r.Recorder != nil {
		r.Recorder.Eventf(obj, nil, corev1.EventTypeWarning, reason, action, note, args...)
	}
}

// updateStatus updates the EtcdCluster status based on observed state.
// It is called at the end of each reconciliation cycle.
func (r *EtcdClusterReconciler) updateStatus(ctx context.Context, s *reconcileState, reconcileErr error) error {
	logger := log.FromContext(ctx)

	// Update ObservedGeneration
	s.cluster.Status.ObservedGeneration = s.cluster.Generation

	// Update replica counts from StatefulSet
	if s.sts != nil {
		if s.sts.Spec.Replicas != nil {
			s.cluster.Status.CurrentReplicas = *s.sts.Spec.Replicas
		}
		s.cluster.Status.ReadyReplicas = s.sts.Status.ReadyReplicas
	}

	// Update member count from etcd cluster
	if s.memberListResp != nil {
		s.cluster.Status.MemberCount = int32(len(s.memberListResp.Members))

		// The member list is ordered by member ID while the health report is
		// ordered by endpoint, so health info must be correlated by member ID
		// rather than by index.
		leader, leaderStatus := etcdutils.FindLeaderStatus(s.memberHealth, logger)
		healthByID := make(map[uint64]etcdutils.EpHealth, len(s.memberHealth))
		for _, health := range s.memberHealth {
			if health.Status != nil {
				healthByID[health.Status.Header.MemberId] = health
			}
		}

		// Update individual member statuses
		s.cluster.Status.Members = make([]ecv1alpha1.MemberStatus, 0, len(s.memberListResp.Members))
		for _, member := range s.memberListResp.Members {
			memberStatus := ecv1alpha1.MemberStatus{
				ID:   fmt.Sprintf("%x", member.ID),
				Name: member.Name,
			}

			// Find health info for this member
			if health, ok := healthByID[member.ID]; ok {
				memberStatus.IsHealthy = health.Health
				memberStatus.Version = health.Status.Version
				memberStatus.IsLeader = leader != 0 && member.ID == leader
			}

			memberStatus.IsLearner = member.IsLearner
			s.cluster.Status.Members = append(s.cluster.Status.Members, memberStatus)
		}

		// Update leader ID. Clear it when no leader is currently known so that
		// has_leader can drop to 0 during a leaderless window instead of
		// reporting a permanently stale leader from an earlier reconcile.
		if leaderStatus != nil {
			s.cluster.Status.LeaderID = fmt.Sprintf("%x", leader)
		} else {
			s.cluster.Status.LeaderID = ""
		}

		// Update current version from leader or first healthy member
		if leaderStatus != nil {
			s.cluster.Status.CurrentVersion = leaderStatus.Version
		} else if len(s.memberHealth) > 0 && s.memberHealth[0].Status != nil {
			s.cluster.Status.CurrentVersion = s.memberHealth[0].Status.Version
		}
	}

	// Update conditions
	r.updateConditions(s, reconcileErr)

	// Refresh per-cluster Prometheus domain metrics from the freshly computed
	// status. Metrics are on by default; an operator can opt a cluster out via
	// spec.metrics.enabled=false, in which case any stale series are dropped.
	if s.cluster.Spec.MetricsEnabled() {
		metrics.RecordClusterMetrics(s.cluster)
	} else {
		metrics.DeleteClusterMetrics(s.cluster.Namespace, s.cluster.Name)
	}

	// Reconcile an optional PodMonitor for the etcd member pods.
	r.reconcilePodMonitor(ctx, s.cluster)

	// Persist status update
	if err := r.Status().Update(ctx, s.cluster); err != nil {
		logger.Error(err, "Failed to update EtcdCluster status")
		return err
	}

	return nil
}

// updateConditions sets the standard Kubernetes conditions based on observed state
// and on the error, if any, returned by the current reconciliation cycle.
func (r *EtcdClusterReconciler) updateConditions(s *reconcileState, reconcileErr error) {
	now := metav1.Now()

	// Determine if cluster is available (has quorum and healthy members)
	availableCondition := metav1.Condition{
		Type:               ConditionAvailable,
		Status:             metav1.ConditionFalse,
		ObservedGeneration: s.cluster.Generation,
		LastTransitionTime: now,
		Reason:             "ClusterNotReady",
		Message:            "Etcd cluster is not yet available",
	}

	if s.memberListResp != nil && len(s.memberListResp.Members) > 0 {
		healthyCount := 0
		for _, health := range s.memberHealth {
			if health.Health {
				healthyCount++
			}
		}

		quorum := (len(s.memberListResp.Members) / 2) + 1
		if healthyCount >= quorum {
			availableCondition.Status = metav1.ConditionTrue
			availableCondition.Reason = "ClusterAvailable"
			availableCondition.Message = fmt.Sprintf("Etcd cluster has %d/%d healthy members with quorum", healthyCount, len(s.memberListResp.Members))
		} else {
			availableCondition.Message = fmt.Sprintf("Etcd cluster has %d/%d healthy members, quorum requires %d", healthyCount, len(s.memberListResp.Members), quorum)
		}
	}

	// Determine if cluster is progressing (scaling or upgrading)
	progressingCondition := metav1.Condition{
		Type:               "Progressing",
		Status:             metav1.ConditionFalse,
		ObservedGeneration: s.cluster.Generation,
		LastTransitionTime: now,
		Reason:             "ClusterStable",
		Message:            "Etcd cluster is stable",
	}

	if s.sts != nil && s.sts.Spec.Replicas != nil {
		currentReplicas := *s.sts.Spec.Replicas
		desiredSize := int32(s.cluster.Spec.Size)

		if currentReplicas != desiredSize {
			progressingCondition.Status = metav1.ConditionTrue
			progressingCondition.Reason = "ScalingInProgress"
			progressingCondition.Message = fmt.Sprintf("Scaling from %d to %d replicas", currentReplicas, desiredSize)
		} else if s.memberListResp != nil && int32(len(s.memberListResp.Members)) != desiredSize {
			progressingCondition.Status = metav1.ConditionTrue
			progressingCondition.Reason = "MembershipChanging"
			progressingCondition.Message = fmt.Sprintf("Etcd membership changing: %d members, target %d", len(s.memberListResp.Members), desiredSize)
		}

		// Check for learners (indicates scaling in progress)
		if s.memberListResp != nil {
			for _, member := range s.memberListResp.Members {
				if member.IsLearner {
					progressingCondition.Status = metav1.ConditionTrue
					progressingCondition.Reason = "LearnerPromotion"
					progressingCondition.Message = "Waiting for learner member to be promoted"
					break
				}
			}
		}
	}

	// Determine if cluster is degraded
	degradedCondition := metav1.Condition{
		Type:               "Degraded",
		Status:             metav1.ConditionFalse,
		ObservedGeneration: s.cluster.Generation,
		LastTransitionTime: now,
		Reason:             "ClusterHealthy",
		Message:            "All etcd members are healthy",
	}

	if s.memberListResp != nil && len(s.memberHealth) > 0 {
		unhealthyMembers := []string{}
		for _, health := range s.memberHealth {
			if !health.Health && health.Status != nil {
				unhealthyMembers = append(unhealthyMembers, fmt.Sprintf("%x", health.Status.Header.MemberId))
			}
		}

		if len(unhealthyMembers) > 0 {
			degradedCondition.Status = metav1.ConditionTrue
			degradedCondition.Reason = "UnhealthyMembers"
			degradedCondition.Message = fmt.Sprintf("Unhealthy members: %s", strings.Join(unhealthyMembers, ", "))
		}
	}

	// Surface the reconcile error so a failing cluster is visible via kubectl.
	if reconcileErr != nil {
		degradedCondition.Status = metav1.ConditionTrue
		degradedCondition.Reason = "ReconcileFailed"
		degradedCondition.Message = reconcileErr.Error()
	}

	// NOSPACE takes precedence over the generic UnhealthyMembers/ReconcileFailed
	// reasons: alarmed members are also reported unhealthy by the health check.
	var nospaceMembers []string
	for _, a := range s.alarms {
		if a.Type == "NOSPACE" {
			nospaceMembers = append(nospaceMembers, memberNameForID(s.memberListResp, a.MemberID))
		}
	}
	if len(nospaceMembers) > 0 {
		degradedCondition.Status = metav1.ConditionTrue
		degradedCondition.Reason = "DatabaseQuotaExceeded"
		degradedCondition.Message = fmt.Sprintf(
			"NOSPACE alarm active on member(s) %s: etcd is read-only until compaction/defragmentation and `etcdctl alarm disarm`",
			strings.Join(nospaceMembers, ", "))
	}

	// Update or append conditions
	meta.SetStatusCondition(&s.cluster.Status.Conditions, availableCondition)
	meta.SetStatusCondition(&s.cluster.Status.Conditions, progressingCondition)
	meta.SetStatusCondition(&s.cluster.Status.Conditions, degradedCondition)

	// One-way latch: the first time the cluster reaches quorum, record durably
	// that bootstrap completed. Never cleared — quorum-loss detection gates on
	// it (see quorum_recovery.go) because Available flips False during outages.
	if availableCondition.Status == metav1.ConditionTrue &&
		!meta.IsStatusConditionTrue(s.cluster.Status.Conditions, ConditionBootstrapComplete) {
		meta.SetStatusCondition(&s.cluster.Status.Conditions, metav1.Condition{
			Type:               ConditionBootstrapComplete,
			Status:             metav1.ConditionTrue,
			ObservedGeneration: s.cluster.Generation,
			Reason:             ReasonInitialQuorumReached,
			Message:            "Etcd cluster reached quorum for the first time",
		})
	}

	// TLSReady reflects the health of the configured TLS surfaces. When no surface
	// is configured (TLSNotConfigured) the condition is omitted entirely -- no TLS,
	// no TLS condition -- and any stale prior condition is removed so flipping a
	// cluster back to cleartext doesn't leave a dangling TLSReady.
	if s.tls.configured {
		meta.SetStatusCondition(&s.cluster.Status.Conditions, s.tls.condition(s.cluster.Generation))
	} else {
		meta.RemoveStatusCondition(&s.cluster.Status.Conditions, tlsReadyConditionType)
	}
}

// recordTLSEvent emits a CR Event reflecting the TLS verdict computed by
// evaluateTLSReadiness. It is the controller boundary that turns the Recorder-free
// verdict into a Kubernetes Event: a Warning (reason == the verdict reason) for any
// failure, and a single Normal "TLSReady" only when a configured cluster transitions
// INTO the ready state (so a steady-state healthy cluster doesn't emit a TLSReady
// event every reconcile). The reason strings match the TLSReady condition reasons.
func (r *EtcdClusterReconciler) recordTLSEvent(ec *ecv1alpha1.EtcdCluster, t tlsReadiness) {
	if !t.configured {
		return
	}
	if !t.ready {
		r.Recorder.Eventf(ec, nil, corev1.EventTypeWarning, t.reason, "TLSReady", "%s", t.message)
		return
	}
	// Ready: only emit on transition (the prior TLSReady condition was not already True).
	prior := meta.FindStatusCondition(ec.Status.Conditions, tlsReadyConditionType)
	if prior == nil || prior.Status != metav1.ConditionTrue {
		r.Recorder.Eventf(ec, nil, corev1.EventTypeNormal, reasonTLSReady, "TLSReady", "%s", t.message)
	}
}

// markClientCertificateFailure persists Degraded and TLSReady conditions when
// operator client-certificate provisioning fails in fetchAndValidateState. That
// path requeues with a nil error and a nil reconcileState, bypassing both the
// deferred updateStatus and the ReconcileFailed handling in updateConditions.
// Best-effort: a status-write failure is only logged so the requeue contract of
// the caller is unchanged.
func (r *EtcdClusterReconciler) markClientCertificateFailure(ctx context.Context, ec *ecv1alpha1.EtcdCluster, certErr error) {
	ec.Status.ObservedGeneration = ec.Generation
	meta.SetStatusCondition(&ec.Status.Conditions, metav1.Condition{
		Type:               "Degraded",
		Status:             metav1.ConditionTrue,
		ObservedGeneration: ec.Generation,
		Reason:             reasonClientCertificateError,
		Message:            certErr.Error(),
	})
	// Same failure through the TLSReady vocabulary: the client surface is
	// configured but the operator's own client certificate cannot be provisioned.
	verdict := tlsReadiness{
		configured: true,
		ready:      false,
		reason:     reasonClientCertificateError,
		message:    fmt.Sprintf("client surface: failed to create operator client certificate: %v", certErr),
	}
	meta.SetStatusCondition(&ec.Status.Conditions, verdict.condition(ec.Generation))
	if err := r.Status().Update(ctx, ec); err != nil {
		log.FromContext(ctx).Error(err, "Failed to update status after client certificate failure")
	}
}

// isCertManagerCRDPresent checks if cert-manager CRDs are installed in the cluster
func isCertManagerCRDPresent(mgr ctrl.Manager) bool {
	gvk := certv1.SchemeGroupVersion.WithKind("Certificate")
	_, err := mgr.GetRESTMapper().RESTMapping(gvk.GroupKind(), gvk.Version)
	return err == nil
}

// SetupWithManager sets up the controller with the Manager.
func (r *EtcdClusterReconciler) SetupWithManager(mgr ctrl.Manager) error {
	r.Recorder = mgr.GetEventRecorder("etcdcluster-controller")
	if r.PodReader == nil {
		r.PodReader = mgr.GetAPIReader()
	}
	setupLog := ctrl.Log.WithName("setup")

	builder := ctrl.NewControllerManagedBy(mgr).
		For(&ecv1alpha1.EtcdCluster{}).
		Owns(&appsv1.StatefulSet{}).
		Owns(&corev1.Service{}).
		Owns(&corev1.ConfigMap{}).
		Owns(&policyv1.PodDisruptionBudget{})

	// Conditionally watch cert-manager Certificate resources if CRDs are installed
	// This allows the controller to react to Certificate status changes when using cert-manager provider
	if isCertManagerCRDPresent(mgr) {
		// cert-manager CRDs are installed, add Certificate watch
		builder = builder.Owns(&certv1.Certificate{})
		setupLog.Info("cert-manager CRDs detected, enabling Certificate watches")
	} else {
		// cert-manager CRDs not installed, skip Certificate watch
		setupLog.Info("cert-manager CRDs not detected, only auto provider will be available. Restart the controller after cert-manager CRDs are installed")
	}

	// Widen the reconcile worker pool when configured. A value <= 0 leaves
	// controller-runtime at its default of a single worker. See the doc comment
	// on EtcdClusterReconciler.MaxConcurrentReconciles for why a small pool is a
	// safe default for this operator.
	if r.MaxConcurrentReconciles > 0 {
		builder = builder.WithOptions(controllerruntime.Options{
			MaxConcurrentReconciles: r.MaxConcurrentReconciles,
		})
	}

	return builder.Complete(r)
}
