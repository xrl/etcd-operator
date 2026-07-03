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
	"errors"
	"fmt"
	"testing"

	certv1 "github.com/cert-manager/cert-manager/pkg/apis/certmanager/v1"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/api/v3/etcdserverpb"
	clientv3 "go.etcd.io/etcd/client/v3"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/tools/events"
	"k8s.io/utils/ptr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/config"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"

	ecv1alpha1 "go.etcd.io/etcd-operator/api/v1alpha1"
	"go.etcd.io/etcd-operator/internal/etcdutils"
)

// TestFetchAndValidateState describes the scenarios for the fetchAndValidateState
// helper. Each sub-test will set up a fake client with different existing
// resources and assert on the returned state, result and error.
func TestFetchAndValidateState(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = corev1.AddToScheme(scheme)
	_ = appsv1.AddToScheme(scheme)
	_ = ecv1alpha1.AddToScheme(scheme)

	cases := []struct {
		name   string
		req    ctrl.Request
		ec     *ecv1alpha1.EtcdCluster
		sts    *appsv1.StatefulSet
		assert func(t *testing.T, state *reconcileState, res ctrl.Result, err error, ec *ecv1alpha1.EtcdCluster, sts *appsv1.StatefulSet)
	}{
		{
			name: "EtcdCluster Not Found",
			req:  ctrl.Request{NamespacedName: types.NamespacedName{Name: "etcd", Namespace: "default"}},
			assert: func(t *testing.T, state *reconcileState, res ctrl.Result, err error, _ *ecv1alpha1.EtcdCluster, _ *appsv1.StatefulSet) {
				assert.Nil(t, state)
				assert.NoError(t, err)
				assert.Equal(t, ctrl.Result{}, res)
			},
		},
		{
			name: "StatefulSet Not Found",
			ec: &ecv1alpha1.EtcdCluster{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "etcd",
					Namespace: "default",
					UID:       "1",
				},
				Spec: ecv1alpha1.EtcdClusterSpec{Size: 1, Version: "3.5.17"},
			},
			req: ctrl.Request{NamespacedName: types.NamespacedName{Name: "etcd", Namespace: "default"}},
			assert: func(t *testing.T, state *reconcileState, res ctrl.Result, err error, ec *ecv1alpha1.EtcdCluster, _ *appsv1.StatefulSet) {
				require.NotNil(t, state)
				assert.Equal(t, ec.Name, state.cluster.Name)
				assert.Nil(t, state.sts)
				assert.NoError(t, err)
				assert.Equal(t, ctrl.Result{}, res)
			},
		},
		{
			name: "Resources Exist and Owned",
			ec: &ecv1alpha1.EtcdCluster{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "etcd",
					Namespace: "default",
					UID:       "2",
				},
				Spec: ecv1alpha1.EtcdClusterSpec{Size: 1, Version: "3.5.17"},
			},
			sts: &appsv1.StatefulSet{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "etcd",
					Namespace: "default",
					OwnerReferences: []metav1.OwnerReference{
						{
							APIVersion: ecv1alpha1.GroupVersion.String(),
							Kind:       "EtcdCluster",
							Name:       "etcd",
							UID:        "2",
							Controller: pointerToBool(true),
						},
					},
				},
			},
			req: ctrl.Request{NamespacedName: types.NamespacedName{Name: "etcd", Namespace: "default"}},
			assert: func(t *testing.T, state *reconcileState, res ctrl.Result, err error, ec *ecv1alpha1.EtcdCluster, sts *appsv1.StatefulSet) {
				require.NotNil(t, state)
				assert.Equal(t, ec.Name, state.cluster.Name)
				require.NotNil(t, state.sts)
				assert.Equal(t, sts.Name, state.sts.Name)
				assert.NoError(t, err)
				assert.Equal(t, ctrl.Result{}, res)
			},
		},
		{
			name: "StatefulSet Not Owned",
			ec: &ecv1alpha1.EtcdCluster{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "etcd",
					Namespace: "default",
					UID:       "3",
				},
				Spec: ecv1alpha1.EtcdClusterSpec{Size: 1, Version: "3.5.17"},
			},
			sts: &appsv1.StatefulSet{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "etcd",
					Namespace: "default",
				},
			},
			req: ctrl.Request{NamespacedName: types.NamespacedName{Name: "etcd", Namespace: "default"}},
			assert: func(t *testing.T, state *reconcileState, res ctrl.Result, err error, _ *ecv1alpha1.EtcdCluster, _ *appsv1.StatefulSet) {
				require.NotNil(t, state)
				assert.Nil(t, state.sts) // foreign StatefulSet must not leak into status
				assert.Error(t, err)
				assert.Contains(t, err.Error(), "not controlled")
				assert.Equal(t, ctrl.Result{}, res)
			},
		},
		{
			name: "Valid upgrade path",
			ec: &ecv1alpha1.EtcdCluster{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "etcd",
					Namespace: "default",
					UID:       "2",
				},
				Spec: ecv1alpha1.EtcdClusterSpec{Size: 1, Version: "3.6.17"},
			},
			sts: &appsv1.StatefulSet{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "etcd",
					Namespace: "default",
					OwnerReferences: []metav1.OwnerReference{
						{
							APIVersion: ecv1alpha1.GroupVersion.String(),
							Kind:       "EtcdCluster",
							Name:       "etcd",
							UID:        "2",
							Controller: pointerToBool(true),
						},
					},
				},
				Spec: appsv1.StatefulSetSpec{
					Template: corev1.PodTemplateSpec{
						Spec: corev1.PodSpec{
							Containers: []corev1.Container{
								{Image: "gcr.io/etcd-development/etcd:3.5.17"},
							},
						},
					},
				},
			},
			req: ctrl.Request{NamespacedName: types.NamespacedName{Name: "etcd", Namespace: "default"}},
			assert: func(t *testing.T, state *reconcileState, res ctrl.Result, err error, ec *ecv1alpha1.EtcdCluster, sts *appsv1.StatefulSet) {
				require.NotNil(t, state)
				assert.NoError(t, err)
				assert.Equal(t, ctrl.Result{}, res)
			},
		},
		{
			name: "Cannot parse StatefulSet image tag",
			ec: &ecv1alpha1.EtcdCluster{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "etcd",
					Namespace: "default",
					UID:       "2",
				},
				Spec: ecv1alpha1.EtcdClusterSpec{Size: 1, Version: "3.6.17"},
			},
			sts: &appsv1.StatefulSet{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "etcd",
					Namespace: "default",
					OwnerReferences: []metav1.OwnerReference{
						{
							APIVersion: ecv1alpha1.GroupVersion.String(),
							Kind:       "EtcdCluster",
							Name:       "etcd",
							UID:        "2",
							Controller: pointerToBool(true),
						},
					},
				},
				Spec: appsv1.StatefulSetSpec{
					Template: corev1.PodTemplateSpec{
						Spec: corev1.PodSpec{
							Containers: []corev1.Container{
								{Image: "gcr.io/etcd-development/etcd#notag"},
							},
						},
					},
				},
			},
			req: ctrl.Request{NamespacedName: types.NamespacedName{Name: "etcd", Namespace: "default"}},
			assert: func(t *testing.T, state *reconcileState, res ctrl.Result, err error, ec *ecv1alpha1.EtcdCluster, sts *appsv1.StatefulSet) {
				require.NotNil(t, state)
				assert.NoError(t, err)
				assert.Equal(t, ctrl.Result{}, res)
			},
		},
		{
			name: "Invalid upgrade path",
			ec: &ecv1alpha1.EtcdCluster{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "etcd",
					Namespace: "default",
					UID:       "2",
				},
				Spec: ecv1alpha1.EtcdClusterSpec{Size: 1, Version: "3.7.1"},
			},
			sts: &appsv1.StatefulSet{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "etcd",
					Namespace: "default",
					OwnerReferences: []metav1.OwnerReference{
						{
							APIVersion: ecv1alpha1.GroupVersion.String(),
							Kind:       "EtcdCluster",
							Name:       "etcd",
							UID:        "2",
							Controller: pointerToBool(true),
						},
					},
				},
				Spec: appsv1.StatefulSetSpec{
					Template: corev1.PodTemplateSpec{
						Spec: corev1.PodSpec{
							Containers: []corev1.Container{
								{Image: "gcr.io/etcd-development/etcd:3.5.17"},
							},
						},
					},
				},
			},
			req: ctrl.Request{NamespacedName: types.NamespacedName{Name: "etcd", Namespace: "default"}},
			assert: func(t *testing.T, state *reconcileState, res ctrl.Result, err error, ec *ecv1alpha1.EtcdCluster, sts *appsv1.StatefulSet) {
				require.NotNil(t, state)
				assert.Error(t, err)
				assert.Equal(t, ctrl.Result{}, res)
			},
		},
		{
			name: "Downgrades are unsupported",
			ec: &ecv1alpha1.EtcdCluster{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "etcd",
					Namespace: "default",
					UID:       "2",
				},
				Spec: ecv1alpha1.EtcdClusterSpec{Size: 1, Version: "3.5.1"},
			},
			sts: &appsv1.StatefulSet{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "etcd",
					Namespace: "default",
					OwnerReferences: []metav1.OwnerReference{
						{
							APIVersion: ecv1alpha1.GroupVersion.String(),
							Kind:       "EtcdCluster",
							Name:       "etcd",
							UID:        "2",
							Controller: pointerToBool(true),
						},
					},
				},
				Spec: appsv1.StatefulSetSpec{
					Template: corev1.PodTemplateSpec{
						Spec: corev1.PodSpec{
							Containers: []corev1.Container{
								{Image: "gcr.io/etcd-development/etcd:3.6.10"},
							},
						},
					},
				},
			},
			req: ctrl.Request{NamespacedName: types.NamespacedName{Name: "etcd", Namespace: "default"}},
			assert: func(t *testing.T, state *reconcileState, res ctrl.Result, err error, ec *ecv1alpha1.EtcdCluster, sts *appsv1.StatefulSet) {
				require.NotNil(t, state)
				assert.Error(t, err)
				assert.Equal(t, ctrl.Result{}, res)
			},
		},
		{
			name: "Upgrade with non-semver versions",
			ec: &ecv1alpha1.EtcdCluster{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "etcd",
					Namespace: "default",
					UID:       "2",
				},
				Spec: ecv1alpha1.EtcdClusterSpec{Size: 1, Version: "foo"},
			},
			sts: &appsv1.StatefulSet{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "etcd",
					Namespace: "default",
					OwnerReferences: []metav1.OwnerReference{
						{
							APIVersion: ecv1alpha1.GroupVersion.String(),
							Kind:       "EtcdCluster",
							Name:       "etcd",
							UID:        "2",
							Controller: pointerToBool(true),
						},
					},
				},
				Spec: appsv1.StatefulSetSpec{
					Template: corev1.PodTemplateSpec{
						Spec: corev1.PodSpec{
							Containers: []corev1.Container{
								{Image: "gcr.io/etcd-development/etcd:bar"},
							},
						},
					},
				},
			},
			req: ctrl.Request{NamespacedName: types.NamespacedName{Name: "etcd", Namespace: "default"}},
			assert: func(t *testing.T, state *reconcileState, res ctrl.Result, err error, ec *ecv1alpha1.EtcdCluster, sts *appsv1.StatefulSet) {
				require.NotNil(t, state)
				assert.NoError(t, err)
				assert.Equal(t, ctrl.Result{}, res)
			},
		},
		{
			name: "Equal tags are a no-op even if they are not semver",
			ec: &ecv1alpha1.EtcdCluster{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "etcd",
					Namespace: "default",
					UID:       "2",
				},
				Spec: ecv1alpha1.EtcdClusterSpec{Size: 1, Version: "bar"},
			},
			sts: &appsv1.StatefulSet{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "etcd",
					Namespace: "default",
					OwnerReferences: []metav1.OwnerReference{
						{
							APIVersion: ecv1alpha1.GroupVersion.String(),
							Kind:       "EtcdCluster",
							Name:       "etcd",
							UID:        "2",
							Controller: pointerToBool(true),
						},
					},
				},
				Spec: appsv1.StatefulSetSpec{
					Template: corev1.PodTemplateSpec{
						Spec: corev1.PodSpec{
							Containers: []corev1.Container{
								{Image: "gcr.io/etcd-development/etcd:bar"},
							},
						},
					},
				},
			},
			req: ctrl.Request{NamespacedName: types.NamespacedName{Name: "etcd", Namespace: "default"}},
			assert: func(t *testing.T, state *reconcileState, res ctrl.Result, err error, ec *ecv1alpha1.EtcdCluster, sts *appsv1.StatefulSet) {
				require.NotNil(t, state)
				assert.NoError(t, err)
				assert.Equal(t, ctrl.Result{}, res)
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ctx := t.Context()

			objs := []client.Object{}
			if tc.ec != nil {
				objs = append(objs, tc.ec)
			}
			if tc.sts != nil {
				objs = append(objs, tc.sts)
			}

			builder := fake.NewClientBuilder().WithScheme(scheme)
			if len(objs) > 0 {
				builder.WithObjects(objs...)
			}
			fakeClient := builder.Build()

			r := &EtcdClusterReconciler{Client: fakeClient, Scheme: scheme}

			state, res, err := r.fetchAndValidateState(ctx, tc.req)
			tc.assert(t, state, res, err, tc.ec, tc.sts)
		})
	}
}

// TestFetchAndValidateStateClientCertificateError verifies that when the cluster
// requests TLS but the client certificate cannot be provisioned,
// fetchAndValidateState requeues with backoff instead of swallowing the error and
// proceeding. The fake client's scheme intentionally omits the cert-manager
// types, so the certificate lookup performed by createClientCertificate fails
// with a non-NotFound error, exercising the provisioning failure path.
func TestFetchAndValidateStateClientCertificateError(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = corev1.AddToScheme(scheme)
	_ = appsv1.AddToScheme(scheme)
	_ = ecv1alpha1.AddToScheme(scheme)
	// Note: cert-manager's certv1 scheme is deliberately NOT registered.

	ec := &ecv1alpha1.EtcdCluster{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "etcd",
			Namespace: "default",
			UID:       "1",
		},
		Spec: ecv1alpha1.EtcdClusterSpec{
			Size:    1,
			Version: "3.5.17",
			TLS: &ecv1alpha1.EtcdClusterTLS{
				Client: &ecv1alpha1.TLSSurface{
					Provider: "cert-manager",
					ProviderCfg: ecv1alpha1.ProviderConfig{
						CertManagerCfg: &ecv1alpha1.ProviderCertManagerConfig{
							IssuerKind: "Issuer",
							IssuerName: "test-issuer",
						},
					},
				},
			},
		},
	}

	ctx := t.Context()

	fakeClient := fake.NewClientBuilder().WithScheme(scheme).
		WithObjects(ec).
		WithStatusSubresource(&ecv1alpha1.EtcdCluster{}).
		Build()
	r := &EtcdClusterReconciler{Client: fakeClient, Scheme: scheme, Recorder: events.NewFakeRecorder(10)}

	req := ctrl.Request{NamespacedName: types.NamespacedName{Name: "etcd", Namespace: "default"}}
	state, res, err := r.fetchAndValidateState(ctx, req)

	// Reconciliation must not proceed past cert provisioning: no state is
	// returned, and the result is a requeue with the standard backoff.
	assert.Nil(t, state, "reconcile should not proceed past client-certificate provisioning")
	assert.NoError(t, err, "the failure should be surfaced via requeue, not a returned error")
	assert.Equal(t, ctrl.Result{RequeueAfter: requeueDuration}, res, "expected a requeue with backoff")
	assert.NotZero(t, res.RequeueAfter, "RequeueAfter must be non-zero")

	// The nil-error requeue bypasses the deferred updateStatus; the direct
	// status write must still surface the failure as conditions.
	assertClientCertificateFailureConditions(t, ctx, fakeClient, req.NamespacedName)
}

// TestReconcileUnparseableValidityDurationSurfacesStatus reproduces the live gap:
// a client surface whose certManagerCfg.validityDuration is unparseable is
// admitted, then createClientCertificate fails on every reconcile via a nil-error
// requeue that bypasses the deferred updateStatus. Full Reconcile must still
// leave a non-empty .status: Degraded=True and TLSReady=False, both with reason
// ClientCertificateError.
func TestReconcileUnparseableValidityDurationSurfacesStatus(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = corev1.AddToScheme(scheme)
	_ = appsv1.AddToScheme(scheme)
	_ = ecv1alpha1.AddToScheme(scheme)
	_ = certv1.AddToScheme(scheme)

	ec := &ecv1alpha1.EtcdCluster{
		ObjectMeta: metav1.ObjectMeta{
			Name:       "etcd",
			Namespace:  "default",
			UID:        "1",
			Generation: 1,
		},
		Spec: ecv1alpha1.EtcdClusterSpec{
			Size:    1,
			Version: "3.5.17",
			TLS: &ecv1alpha1.EtcdClusterTLS{
				Client: &ecv1alpha1.TLSSurface{
					Provider: "cert-manager",
					ProviderCfg: ecv1alpha1.ProviderConfig{
						CertManagerCfg: &ecv1alpha1.ProviderCertManagerConfig{
							CommonConfig: ecv1alpha1.CommonConfig{
								ValidityDuration: "garbage",
							},
							IssuerKind: "Issuer",
							IssuerName: "test-issuer",
						},
					},
				},
			},
		},
	}

	ctx := t.Context()

	fakeClient := fake.NewClientBuilder().WithScheme(scheme).
		WithObjects(ec).
		WithStatusSubresource(&ecv1alpha1.EtcdCluster{}).
		Build()
	r := &EtcdClusterReconciler{Client: fakeClient, Scheme: scheme, Recorder: events.NewFakeRecorder(10)}

	req := ctrl.Request{NamespacedName: types.NamespacedName{Name: "etcd", Namespace: "default"}}
	res, err := r.Reconcile(ctx, req)

	assert.NoError(t, err, "the failure should be surfaced via requeue, not a returned error")
	assert.Equal(t, ctrl.Result{RequeueAfter: requeueDuration}, res, "expected a requeue with backoff")

	assertClientCertificateFailureConditions(t, ctx, fakeClient, req.NamespacedName)
}

// assertClientCertificateFailureConditions asserts the persisted status of the
// named cluster carries Degraded=True and TLSReady=False, both with reason
// ClientCertificateError, and a matching observedGeneration.
func assertClientCertificateFailureConditions(t *testing.T, ctx context.Context, c client.Client, key types.NamespacedName) {
	t.Helper()

	got := &ecv1alpha1.EtcdCluster{}
	require.NoError(t, c.Get(ctx, key, got))
	require.NotEmpty(t, got.Status.Conditions, ".status.conditions must not be left empty")
	assert.Equal(t, got.Generation, got.Status.ObservedGeneration)

	degraded := meta.FindStatusCondition(got.Status.Conditions, "Degraded")
	require.NotNil(t, degraded, "Degraded condition must be set")
	assert.Equal(t, metav1.ConditionTrue, degraded.Status)
	assert.Equal(t, reasonClientCertificateError, degraded.Reason)
	assert.NotEmpty(t, degraded.Message)
	assert.Equal(t, got.Generation, degraded.ObservedGeneration)

	tlsReady := meta.FindStatusCondition(got.Status.Conditions, tlsReadyConditionType)
	require.NotNil(t, tlsReady, "TLSReady condition must be set")
	assert.Equal(t, metav1.ConditionFalse, tlsReady.Status)
	assert.Equal(t, reasonClientCertificateError, tlsReady.Reason)
}

// TestBootstrapStatefulSet outlines tests for ensuring StatefulSet and Service
// creation and bootstrap logic.
func TestBootstrapStatefulSet(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = corev1.AddToScheme(scheme)
	_ = appsv1.AddToScheme(scheme)
	_ = ecv1alpha1.AddToScheme(scheme)

	ec := &ecv1alpha1.EtcdCluster{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "etcd",
			Namespace: "default",
			UID:       "1",
		},
		Spec: ecv1alpha1.EtcdClusterSpec{
			Size:    1,
			Version: "3.5.17",
		},
	}

	t.Run("Initial Creation", func(t *testing.T) {
		ctx := t.Context()

		fakeClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(ec).Build()
		r := &EtcdClusterReconciler{Client: fakeClient, Scheme: scheme}
		state := &reconcileState{cluster: ec}

		res, err := r.bootstrapStatefulSet(ctx, state)
		assert.NoError(t, err)
		assert.Equal(t, ctrl.Result{RequeueAfter: requeueDuration}, res)
		require.NotNil(t, state.sts)
		assert.NotNil(t, state.sts.Spec.Replicas)
		assert.Equal(t, int32(0), *state.sts.Spec.Replicas)

		sts := &appsv1.StatefulSet{}
		err = fakeClient.Get(ctx, client.ObjectKey{Name: ec.Name, Namespace: ec.Namespace}, sts)
		assert.NoError(t, err)
		assert.NotNil(t, sts.Spec.Replicas)
		assert.Equal(t, int32(0), *sts.Spec.Replicas)

		svc := &corev1.Service{}
		err = fakeClient.Get(ctx, client.ObjectKey{Name: ec.Name, Namespace: ec.Namespace}, svc)
		assert.NoError(t, err)
		assert.Equal(t, "None", svc.Spec.ClusterIP)

		cm := &corev1.ConfigMap{}
		err = fakeClient.Get(ctx, client.ObjectKey{Name: configMapNameForEtcdCluster(ec), Namespace: ec.Namespace}, cm)
		assert.NoError(t, err)
	})

	t.Run("Bootstrap from Zero", func(t *testing.T) {
		ctx := t.Context()

		sts := &appsv1.StatefulSet{
			ObjectMeta: metav1.ObjectMeta{
				Name:      ec.Name,
				Namespace: ec.Namespace,
				OwnerReferences: []metav1.OwnerReference{{
					APIVersion: ecv1alpha1.GroupVersion.String(),
					Kind:       "EtcdCluster",
					Name:       ec.Name,
					UID:        ec.UID,
					Controller: pointerToBool(true),
				}},
			},
			Spec: appsv1.StatefulSetSpec{
				Replicas: pointerToInt32(0),
			},
			Status: appsv1.StatefulSetStatus{ReadyReplicas: 1},
		}

		cm := newEtcdClusterState(ec, 0)

		fakeClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(ec, sts, cm).Build()
		r := &EtcdClusterReconciler{Client: fakeClient, Scheme: scheme}
		state := &reconcileState{cluster: ec, sts: sts}

		oldRV := sts.ResourceVersion
		res, err := r.bootstrapStatefulSet(ctx, state)
		assert.NoError(t, err)
		assert.Equal(t, ctrl.Result{RequeueAfter: requeueDuration}, res)

		updatedSTS := &appsv1.StatefulSet{}
		err = fakeClient.Get(ctx, client.ObjectKey{Name: ec.Name, Namespace: ec.Namespace}, updatedSTS)
		assert.NoError(t, err)
		assert.Equal(t, int32(1), *updatedSTS.Spec.Replicas)
		assert.Equal(t, int32(1), updatedSTS.Status.ReadyReplicas)
		assert.NotEqual(t, oldRV, updatedSTS.ResourceVersion)

		require.NotNil(t, state.sts)
		assert.Equal(t, int32(1), *state.sts.Spec.Replicas)

		svc := &corev1.Service{}
		err = fakeClient.Get(ctx, client.ObjectKey{Name: ec.Name, Namespace: ec.Namespace}, svc)
		assert.NoError(t, err)
		assert.Equal(t, "None", svc.Spec.ClusterIP)

		cmUpdated := &corev1.ConfigMap{}
		err = fakeClient.Get(ctx, client.ObjectKey{Name: configMapNameForEtcdCluster(ec), Namespace: ec.Namespace}, cmUpdated)
		assert.NoError(t, err)
		assert.Equal(t, "new", cmUpdated.Data["ETCD_INITIAL_CLUSTER_STATE"])
		assert.Contains(t, cmUpdated.Data["ETCD_INITIAL_CLUSTER"], "etcd-0=")
	})

	t.Run("Resources Already Exist", func(t *testing.T) {
		ctx := t.Context()

		sts := &appsv1.StatefulSet{
			ObjectMeta: metav1.ObjectMeta{
				Name:      ec.Name,
				Namespace: ec.Namespace,
				OwnerReferences: []metav1.OwnerReference{{
					APIVersion: ecv1alpha1.GroupVersion.String(),
					Kind:       "EtcdCluster",
					Name:       ec.Name,
					UID:        ec.UID,
					Controller: pointerToBool(true),
				}},
			},
			Spec:   appsv1.StatefulSetSpec{Replicas: pointerToInt32(1)},
			Status: appsv1.StatefulSetStatus{ReadyReplicas: 1},
		}

		svc := &corev1.Service{
			ObjectMeta: metav1.ObjectMeta{
				Name:      ec.Name,
				Namespace: ec.Namespace,
			},
			Spec: corev1.ServiceSpec{ClusterIP: "None"},
		}

		cm := newEtcdClusterState(ec, 1)

		fakeClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(ec, sts.DeepCopy(), svc.DeepCopy(), cm.DeepCopy()).Build()
		r := &EtcdClusterReconciler{Client: fakeClient, Scheme: scheme}
		state := &reconcileState{cluster: ec, sts: sts}

		// Capture current objects to verify no updates occur.
		storedSTS := sts.DeepCopy()
		storedSvc := svc.DeepCopy()
		storedCM := cm.DeepCopy()
		res, err := r.bootstrapStatefulSet(ctx, state)
		assert.NoError(t, err)
		assert.Equal(t, ctrl.Result{}, res)

		fetchedSTS := &appsv1.StatefulSet{}
		err = fakeClient.Get(ctx, client.ObjectKey{Name: ec.Name, Namespace: ec.Namespace}, fetchedSTS)
		assert.NoError(t, err)
		assert.Equal(t, storedSTS.Spec, fetchedSTS.Spec)

		fetchedSvc := &corev1.Service{}
		err = fakeClient.Get(ctx, client.ObjectKey{Name: ec.Name, Namespace: ec.Namespace}, fetchedSvc)
		assert.NoError(t, err)
		assert.Equal(t, storedSvc.Spec, fetchedSvc.Spec)

		fetchedCM := &corev1.ConfigMap{}
		err = fakeClient.Get(ctx, client.ObjectKey{Name: configMapNameForEtcdCluster(ec), Namespace: ec.Namespace}, fetchedCM)
		assert.NoError(t, err)
		assert.Equal(t, storedCM.Data, fetchedCM.Data)
	})
}

// TestSetupWithManagerThreadsMaxConcurrentReconciles verifies that the
// --max-concurrent-reconciles knob is actually threaded through
// SetupWithManager into controller-runtime's builder. A real manager is built
// from the envtest rest.Config so the builder's option plumbing is exercised
// end-to-end; the controller is never started.
func TestSetupWithManagerThreadsMaxConcurrentReconciles(t *testing.T) {
	if restCfg == nil {
		t.Skip("envtest rest config unavailable; KUBEBUILDER_ASSETS not set")
	}

	testScheme := runtime.NewScheme()
	require.NoError(t, clientgoscheme.AddToScheme(testScheme))
	require.NoError(t, ecv1alpha1.AddToScheme(testScheme))

	tests := []struct {
		name                    string
		maxConcurrentReconciles int
	}{
		{name: "widened pool", maxConcurrentReconciles: 5},
		{name: "explicit single worker", maxConcurrentReconciles: 1},
		{name: "non-positive falls back to default safely", maxConcurrentReconciles: 0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mgr, err := ctrl.NewManager(restCfg, ctrl.Options{
				Scheme:  testScheme,
				Metrics: metricsserver.Options{BindAddress: "0"},
				// Subtests register the same controller name on distinct managers;
				// skip the global metric-name uniqueness guard so they can coexist.
				Controller: config.Controller{SkipNameValidation: ptr.To(true)},
			})
			require.NoError(t, err)

			r := &EtcdClusterReconciler{
				Client:                  mgr.GetClient(),
				Scheme:                  mgr.GetScheme(),
				MaxConcurrentReconciles: tt.maxConcurrentReconciles,
			}
			// SetupWithManager must accept the configured pool size and register
			// the controller without error for every value, including the
			// non-positive fallback path.
			require.NoError(t, r.SetupWithManager(mgr))
			assert.Equal(t, tt.maxConcurrentReconciles, r.MaxConcurrentReconciles)
		})
	}
}

// TestReconcileErrorSetsDegradedCondition verifies that a reconcile error is
// surfaced on the EtcdCluster status as a Degraded=True condition instead of
// leaving the status empty.
func TestReconcileErrorSetsDegradedCondition(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = corev1.AddToScheme(scheme)
	_ = appsv1.AddToScheme(scheme)
	_ = ecv1alpha1.AddToScheme(scheme)

	ec := &ecv1alpha1.EtcdCluster{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "etcd",
			Namespace: "default",
			UID:       "1",
		},
		Spec: ecv1alpha1.EtcdClusterSpec{
			Size:    1,
			Version: "3.5.17",
			TLS: &ecv1alpha1.EtcdClusterTLS{
				// The peer surface: a client-surface cert failure requeues without
				// an error in fetchAndValidateState, so it would never reach the
				// Degraded path this test covers.
				Peer: &ecv1alpha1.TLSSurface{
					Provider: "auto",
					ProviderCfg: ecv1alpha1.ProviderConfig{
						AutoCfg: &ecv1alpha1.ProviderAutoConfig{
							CommonConfig: ecv1alpha1.CommonConfig{
								ValidityDuration: "90days", // not a valid time.Duration
							},
						},
					},
				},
			},
		},
	}

	fakeClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&ecv1alpha1.EtcdCluster{}).
		WithObjects(ec).
		Build()
	r := &EtcdClusterReconciler{Client: fakeClient, Scheme: scheme, Recorder: events.NewFakeRecorder(10)}

	_, err := r.Reconcile(t.Context(), ctrl.Request{
		NamespacedName: types.NamespacedName{Name: ec.Name, Namespace: ec.Namespace},
	})
	require.Error(t, err)

	updated := &ecv1alpha1.EtcdCluster{}
	require.NoError(t, fakeClient.Get(t.Context(), client.ObjectKeyFromObject(ec), updated))

	degraded := meta.FindStatusCondition(updated.Status.Conditions, "Degraded")
	require.NotNil(t, degraded)
	assert.Equal(t, metav1.ConditionTrue, degraded.Status)
	assert.Equal(t, "ReconcileFailed", degraded.Reason)
	assert.Contains(t, degraded.Message, "failed to parse ValidityDuration")
}

// TestReconcileNotOwnedStatefulSetStatus verifies that when reconciliation
// fails because the StatefulSet is not owned by the EtcdCluster, the status
// reports Degraded=ReconcileFailed without leaking the foreign StatefulSet's
// replica counts or a Progressing=ScalingInProgress condition.
func TestReconcileNotOwnedStatefulSetStatus(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = corev1.AddToScheme(scheme)
	_ = appsv1.AddToScheme(scheme)
	_ = ecv1alpha1.AddToScheme(scheme)

	ec := &ecv1alpha1.EtcdCluster{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "etcd",
			Namespace: "default",
			UID:       "1",
		},
		Spec: ecv1alpha1.EtcdClusterSpec{Size: 3, Version: "3.5.17"},
	}
	replicas := int32(5)
	foreignSTS := &appsv1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "etcd",
			Namespace: "default",
		},
		Spec:   appsv1.StatefulSetSpec{Replicas: &replicas},
		Status: appsv1.StatefulSetStatus{ReadyReplicas: replicas},
	}

	fakeClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&ecv1alpha1.EtcdCluster{}).
		WithObjects(ec, foreignSTS).
		Build()
	r := &EtcdClusterReconciler{Client: fakeClient, Scheme: scheme, Recorder: events.NewFakeRecorder(10)}

	_, err := r.Reconcile(t.Context(), ctrl.Request{
		NamespacedName: types.NamespacedName{Name: ec.Name, Namespace: ec.Namespace},
	})
	require.Error(t, err)

	updated := &ecv1alpha1.EtcdCluster{}
	require.NoError(t, fakeClient.Get(t.Context(), client.ObjectKeyFromObject(ec), updated))

	degraded := meta.FindStatusCondition(updated.Status.Conditions, "Degraded")
	require.NotNil(t, degraded)
	assert.Equal(t, metav1.ConditionTrue, degraded.Status)
	assert.Equal(t, "ReconcileFailed", degraded.Reason)

	assert.Equal(t, int32(0), updated.Status.CurrentReplicas)
	assert.Equal(t, int32(0), updated.Status.ReadyReplicas)
	progressing := meta.FindStatusCondition(updated.Status.Conditions, "Progressing")
	require.NotNil(t, progressing)
	assert.Equal(t, metav1.ConditionFalse, progressing.Status)
	assert.NotEqual(t, "ScalingInProgress", progressing.Reason)
}

// TestUpdateConditionsReconcileError verifies that a reconcile error forces
// Degraded=ReconcileFailed even when all members are healthy, and that
// Degraded reverts to False once the error clears.
func TestUpdateConditionsReconcileError(t *testing.T) {
	healthyState := func() *reconcileState {
		return &reconcileState{
			cluster: &ecv1alpha1.EtcdCluster{
				ObjectMeta: metav1.ObjectMeta{Name: "etcd", Namespace: "default"},
				Spec:       ecv1alpha1.EtcdClusterSpec{Size: 1, Version: "3.5.17"},
			},
			memberListResp: &clientv3.MemberListResponse{
				Members: []*etcdserverpb.Member{{ID: 1, Name: "etcd-0"}},
			},
			memberHealth: []etcdutils.EpHealth{
				{
					Ep:     "http://etcd-0:2379",
					Health: true,
					Status: &clientv3.StatusResponse{
						Header: &etcdserverpb.ResponseHeader{MemberId: 1},
						Leader: 1,
					},
				},
			},
		}
	}
	r := &EtcdClusterReconciler{}

	s := healthyState()
	r.updateConditions(s, errors.New("couldn't find leader"))
	degraded := meta.FindStatusCondition(s.cluster.Status.Conditions, "Degraded")
	require.NotNil(t, degraded)
	assert.Equal(t, metav1.ConditionTrue, degraded.Status)
	assert.Equal(t, "ReconcileFailed", degraded.Reason)
	assert.Equal(t, "couldn't find leader", degraded.Message)

	r.updateConditions(s, nil)
	degraded = meta.FindStatusCondition(s.cluster.Status.Conditions, "Degraded")
	require.NotNil(t, degraded)
	assert.Equal(t, metav1.ConditionFalse, degraded.Status)
	assert.Equal(t, "ClusterHealthy", degraded.Reason)
}

// TestUpdateStatusMemberHealthCorrelation verifies that member statuses are
// correlated with health reports by member ID. The member list from etcd is
// ordered by member ID while the health report is ordered by endpoint, so the
// two lists cannot be paired by index.
func TestUpdateStatusMemberHealthCorrelation(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = ecv1alpha1.AddToScheme(scheme)

	ec := &ecv1alpha1.EtcdCluster{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "etcd",
			Namespace: "default",
		},
		Spec: ecv1alpha1.EtcdClusterSpec{Size: 3, Version: "3.5.17"},
	}

	fakeClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(ec).
		WithStatusSubresource(ec).
		Build()
	r := &EtcdClusterReconciler{Client: fakeClient, Scheme: scheme}

	// etcd-2 is the leader and has the lowest member ID, so it comes first in
	// the ID-ordered member list while its health entry comes last in the
	// endpoint-ordered health report.
	const (
		leaderID    uint64 = 0x100 // etcd-2
		followerID0 uint64 = 0x200 // etcd-0
		followerID1 uint64 = 0x300 // etcd-1
	)

	state := &reconcileState{
		cluster: ec,
		memberListResp: &clientv3.MemberListResponse{
			Members: []*etcdserverpb.Member{
				{ID: leaderID, Name: "etcd-2"},
				{ID: followerID0, Name: "etcd-0"},
				{ID: followerID1, Name: "etcd-1"},
			},
		},
		memberHealth: []etcdutils.EpHealth{
			{
				Ep:     "http://etcd-0.etcd.default.svc.cluster.local:2379",
				Health: true,
				Status: &clientv3.StatusResponse{
					Header:  &etcdserverpb.ResponseHeader{MemberId: followerID0},
					Leader:  leaderID,
					Version: "3.5.17",
				},
			},
			{
				Ep:     "http://etcd-1.etcd.default.svc.cluster.local:2379",
				Health: true,
				Status: &clientv3.StatusResponse{
					Header:  &etcdserverpb.ResponseHeader{MemberId: followerID1},
					Leader:  leaderID,
					Version: "3.5.16",
				},
			},
			{
				Ep:     "http://etcd-2.etcd.default.svc.cluster.local:2379",
				Health: true,
				Status: &clientv3.StatusResponse{
					Header:  &etcdserverpb.ResponseHeader{MemberId: leaderID},
					Leader:  leaderID,
					Version: "3.5.17",
				},
			},
		},
	}

	require.NoError(t, r.updateStatus(t.Context(), state, nil))

	status := ec.Status
	assert.Equal(t, fmt.Sprintf("%x", leaderID), status.LeaderID)
	require.Len(t, status.Members, 3)

	leaders := 0
	for _, m := range status.Members {
		if m.IsLeader {
			leaders++
			assert.Equal(t, status.LeaderID, m.ID, "isLeader member must match status.leaderID")
		}
	}
	assert.Equal(t, 1, leaders, "exactly one member should be leader")

	byName := map[string]ecv1alpha1.MemberStatus{}
	for _, m := range status.Members {
		byName[m.Name] = m
	}
	assert.True(t, byName["etcd-2"].IsLeader)
	assert.False(t, byName["etcd-0"].IsLeader)
	assert.False(t, byName["etcd-1"].IsLeader)
	assert.Equal(t, "3.5.17", byName["etcd-0"].Version)
	assert.Equal(t, "3.5.16", byName["etcd-1"].Version)
	assert.Equal(t, "3.5.17", byName["etcd-2"].Version)
	for _, m := range status.Members {
		assert.True(t, m.IsHealthy, "member %s should be healthy", m.Name)
	}
}
