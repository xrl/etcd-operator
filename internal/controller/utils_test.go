package controller

import (
	"crypto/tls"
	"errors"
	"fmt"
	"net"
	"sort"
	"testing"
	"time"

	"github.com/coreos/go-semver/semver"
	"github.com/go-logr/logr"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/log"

	ecv1alpha1 "go.etcd.io/etcd-operator/api/v1alpha1"
	"go.etcd.io/etcd-operator/internal/etcdutils"
	"go.etcd.io/etcd-operator/pkg/certificate"
	certInterface "go.etcd.io/etcd-operator/pkg/certificate/interfaces"
	"go.etcd.io/etcd/api/v3/etcdserverpb"
	clientv3 "go.etcd.io/etcd/client/v3"
)

func pointerToInt32(value int32) *int32 {
	return &value
}

func TestReconcileStatefulSet(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = ecv1alpha1.AddToScheme(scheme)

	fakeClient := fake.NewClientBuilder().Build()
	logger := log.FromContext(t.Context())

	ec := &ecv1alpha1.EtcdCluster{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-etcd",
			Namespace: "default",
		},
		Spec: ecv1alpha1.EtcdClusterSpec{
			Size:    3,
			Version: "3.5.17",
		},
	}

	_, _ = reconcileStatefulSet(t.Context(), logger, ec, fakeClient, 3, scheme)

	sts := &appsv1.StatefulSet{}
	err := fakeClient.Get(t.Context(), client.ObjectKey{Name: "test-etcd", Namespace: "default"}, sts)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	if *sts.Spec.Replicas != 3 {
		t.Fatalf("expected 3 replicas, got %d", *sts.Spec.Replicas)
	}
}

func TestWaitForStatefulSetReady(t *testing.T) {
	// Create a scheme and register the necessary types
	scheme := runtime.NewScheme()
	_ = corev1.AddToScheme(scheme)
	_ = ecv1alpha1.AddToScheme(scheme)
	_ = appsv1.AddToScheme(scheme)

	tests := []struct {
		name           string
		statefulSet    *appsv1.StatefulSet
		expectedResult bool
		expectedError  error
	}{
		{
			name: "StatefulSet is ready",
			statefulSet: &appsv1.StatefulSet{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-sts",
					Namespace: "default",
				},
				Spec: appsv1.StatefulSetSpec{
					Replicas: pointerToInt32(3),
				},
				Status: appsv1.StatefulSetStatus{
					ReadyReplicas: 3,
				},
			},
			expectedResult: true,
			expectedError:  nil,
		},
		{
			name: "StatefulSet is not ready",
			statefulSet: &appsv1.StatefulSet{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-sts",
					Namespace: "default",
				},
				Spec: appsv1.StatefulSetSpec{
					Replicas: pointerToInt32(3),
				},
				Status: appsv1.StatefulSetStatus{
					ReadyReplicas: 2,
				},
			},
			expectedResult: false,
			expectedError:  errors.New("StatefulSet default/test-sts did not become ready: timed out waiting for the condition"),
		},
		{
			name:           "StatefulSet does not exist",
			statefulSet:    nil,
			expectedResult: false,
			expectedError:  errors.New("statefulsets.apps \"test-sts\" not found"),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var clientBuilder *fake.ClientBuilder
			if tt.statefulSet != nil {
				clientBuilder = fake.NewClientBuilder().WithScheme(scheme).WithObjects(tt.statefulSet)
			} else {
				clientBuilder = fake.NewClientBuilder().WithScheme(scheme)
			}
			fakeClient := clientBuilder.Build()

			ctx := t.Context()
			logger := log.FromContext(ctx)

			err := waitForStatefulSetReady(ctx, logger, fakeClient, "test-sts", "default")
			if tt.expectedError != nil {
				assert.Error(t, err)
				assert.Contains(t, err.Error(), tt.expectedError.Error())
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestCreateHeadlessServiceIfNotExist(t *testing.T) {
	ctx := t.Context()
	logger := log.FromContext(ctx)

	// Create a scheme and register the necessary types
	scheme := runtime.NewScheme()
	_ = corev1.AddToScheme(scheme)
	_ = ecv1alpha1.AddToScheme(scheme)

	// Create a fake client
	fakeClient := fake.NewClientBuilder().WithScheme(scheme).Build()

	// Create an EtcdCluster instance
	ec := &ecv1alpha1.EtcdCluster{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-etcd",
			Namespace: "default",
		},
	}

	t.Run("creates headless service if it does not exist", func(t *testing.T) {
		err := createHeadlessServiceIfNotExist(ctx, logger, fakeClient, ec, scheme)
		assert.NoError(t, err)

		// Verify that the service was created
		service := &corev1.Service{}
		err = fakeClient.Get(ctx, client.ObjectKey{Name: "test-etcd", Namespace: "default"}, service)
		assert.NoError(t, err)
		assert.Equal(t, "None", service.Spec.ClusterIP)
		// Required for peer-TLS cluster formation: a joining (not-yet-Ready) member
		// must be resolvable so etcd's peer cert SAN check (isHostInDNS) accepts it.
		assert.True(t, service.Spec.PublishNotReadyAddresses,
			"headless service must publish not-ready addresses so peer-TLS members can join")
		assert.Equal(t, map[string]string{
			"app":        "test-etcd",
			"controller": "test-etcd",
		}, service.Spec.Selector)
		// Verify service is controlled by EtcdCluster
		require.Len(t, service.OwnerReferences, 1)
		require.Equal(t, service.OwnerReferences[0].Name, ec.Name)
	})

	t.Run("does not create service if it already exists", func(t *testing.T) {
		// Service was already created in previous test. Call the function again to ensure no error
		err := createHeadlessServiceIfNotExist(ctx, logger, fakeClient, ec, scheme)
		assert.NoError(t, err)
	})
}

func TestClientEndpointForOrdinalIndex(t *testing.T) {
	sts := &appsv1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-sts",
			Namespace: "default",
		},
	}

	tests := []struct {
		index          int
		expectedResult string
	}{
		{index: 0, expectedResult: "http://test-sts-0.test-sts.default.svc.cluster.local:2379"},
		{index: 1, expectedResult: "http://test-sts-1.test-sts.default.svc.cluster.local:2379"},
		{index: 2, expectedResult: "http://test-sts-2.test-sts.default.svc.cluster.local:2379"},
	}

	for _, tt := range tests {
		t.Run(fmt.Sprintf("index %d", tt.index), func(t *testing.T) {
			result := clientEndpointForOrdinalIndex(sts, tt.index, "http")
			assert.Equal(t, tt.expectedResult, result)
		})
	}
}

func TestIsLearnerReady(t *testing.T) {
	tests := []struct {
		name           string
		leaderStatus   *clientv3.StatusResponse
		learnerStatus  *clientv3.StatusResponse
		expectedResult bool
	}{
		{
			name: "Learner is ready",
			leaderStatus: &clientv3.StatusResponse{
				Header: &etcdserverpb.ResponseHeader{Revision: 100},
			},
			learnerStatus: &clientv3.StatusResponse{
				Header: &etcdserverpb.ResponseHeader{Revision: 95},
			},
			expectedResult: true,
		},
		{
			name: "Learner is not ready",
			leaderStatus: &clientv3.StatusResponse{
				Header: &etcdserverpb.ResponseHeader{Revision: 100},
			},
			learnerStatus: &clientv3.StatusResponse{
				Header: &etcdserverpb.ResponseHeader{Revision: 80},
			},
			expectedResult: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := etcdutils.IsLearnerReady(tt.leaderStatus, tt.learnerStatus)
			assert.Equal(t, tt.expectedResult, result)
		})
	}
}

func TestCheckStatefulSetControlledByEtcdOperator(t *testing.T) {
	tests := []struct {
		name          string
		ec            *ecv1alpha1.EtcdCluster
		sts           *appsv1.StatefulSet
		expectedError error
	}{
		{
			name: "StatefulSet controlled by EtcdCluster",
			ec: &ecv1alpha1.EtcdCluster{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "etcd-cluster",
					Namespace: "default",
					UID:       "1234",
				},
			},
			sts: &appsv1.StatefulSet{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "etcd-sts",
					Namespace: "default",
					OwnerReferences: []metav1.OwnerReference{
						{
							APIVersion: "ecv1alpha1/v1alpha1",
							Kind:       "EtcdCluster",
							Name:       "etcd-cluster",
							UID:        "1234",
							Controller: pointerToBool(true),
						},
					},
				},
			},
			expectedError: nil,
		},
		{
			name: "StatefulSet not controlled by EtcdCluster",
			ec: &ecv1alpha1.EtcdCluster{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "etcd-cluster",
					Namespace: "default",
					UID:       "1234",
				},
			},
			sts: &appsv1.StatefulSet{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "etcd-sts",
					Namespace: "default",
					OwnerReferences: []metav1.OwnerReference{
						{
							APIVersion: "ecv1alpha1/v1alpha1",
							Kind:       "EtcdCluster",
							Name:       "other-etcd-cluster",
							UID:        "5678",
							Controller: pointerToBool(true),
						},
					},
				},
			},
			expectedError: fmt.Errorf("StatefulSet default/etcd-sts is not controlled by EtcdCluster default/etcd-cluster"),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := checkStatefulSetControlledByEtcdOperator(tt.ec, tt.sts)

			if (err != nil) != (tt.expectedError != nil) {
				t.Errorf("expected error: %v, got: %v", tt.expectedError, err)
				return
			}

			if err != nil && err.Error() != tt.expectedError.Error() {
				t.Errorf("unexpected error: got %v, want %v", err, tt.expectedError)
			}
		})
	}
}

func pointerToBool(value bool) *bool {
	return &value
}

func TestClientEndpointsFromStatefulsets(t *testing.T) {
	tests := []struct {
		name           string
		statefulSet    *appsv1.StatefulSet
		expectedResult []string
	}{
		{
			name: "StatefulSet with 3 replicas",
			statefulSet: &appsv1.StatefulSet{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-sts",
					Namespace: "default",
				},
				Spec: appsv1.StatefulSetSpec{
					Replicas: pointerToInt32(3),
				},
			},
			expectedResult: []string{
				"http://test-sts-0.test-sts.default.svc.cluster.local:2379",
				"http://test-sts-1.test-sts.default.svc.cluster.local:2379",
				"http://test-sts-2.test-sts.default.svc.cluster.local:2379",
			},
		},
		{
			name: "StatefulSet with 1 replica",
			statefulSet: &appsv1.StatefulSet{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-sts",
					Namespace: "default",
				},
				Spec: appsv1.StatefulSetSpec{
					Replicas: pointerToInt32(1),
				},
			},
			expectedResult: []string{
				"http://test-sts-0.test-sts.default.svc.cluster.local:2379",
			},
		},
		{
			name: "StatefulSet with 0 replicas",
			statefulSet: &appsv1.StatefulSet{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-sts",
					Namespace: "default",
				},
				Spec: appsv1.StatefulSetSpec{
					Replicas: pointerToInt32(0),
				},
			},
			expectedResult: []string(nil),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := clientEndpointsFromStatefulsets(tt.statefulSet, "http")
			assert.Equal(t, tt.expectedResult, result)
		})
	}
}

func TestAreAllMembersHealthy(t *testing.T) {
	tests := []struct {
		name           string
		statefulSet    *appsv1.StatefulSet
		healthInfos    []etcdutils.EpHealth
		expectedResult bool
		expectedError  error
	}{
		// TODO: Add test cases for healthy members and non healthy members
		{
			name: "Error during health check",
			statefulSet: &appsv1.StatefulSet{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-sts",
					Namespace: "default",
				},
				Spec: appsv1.StatefulSetSpec{
					Replicas: pointerToInt32(3),
				},
				Status: appsv1.StatefulSetStatus{
					ReadyReplicas: 3,
				},
			},
			healthInfos:    nil,
			expectedResult: false,
			expectedError:  errors.New("context deadline exceeded"),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			logger := logr.Discard() // Use a no-op logger for testing

			// Cleartext cluster: buildClientTLSConfig returns nil and the dial
			// times out, exactly as before the TLS-config threading.
			ec := &ecv1alpha1.EtcdCluster{
				ObjectMeta: metav1.ObjectMeta{Name: "test-sts", Namespace: "default"},
			}
			fakeClient := fake.NewClientBuilder().Build()
			result, err := areAllMembersHealthy(t.Context(), ec, fakeClient, tt.statefulSet, logger)
			assert.Equal(t, tt.expectedResult, result)
			if tt.expectedError != nil {
				assert.Error(t, err)
				assert.Contains(t, err.Error(), tt.expectedError.Error())
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestApplyEtcdClusterState(t *testing.T) {
	ctx := t.Context()
	logger := log.FromContext(ctx)

	// Create a scheme and register the necessary types
	scheme := runtime.NewScheme()
	_ = corev1.AddToScheme(scheme)
	_ = ecv1alpha1.AddToScheme(scheme)

	// Create a fake client
	fakeClient := fake.NewClientBuilder().WithScheme(scheme).Build()

	// Create an EtcdCluster instance
	ec := &ecv1alpha1.EtcdCluster{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-etcd",
			Namespace: "default",
		},
	}

	t.Run("creates configmap if it does not exist", func(t *testing.T) {
		err := applyEtcdClusterState(ctx, ec, 3, fakeClient, scheme, logger)
		assert.NoError(t, err)
		// Verify that the configmap was created
		configMap := &corev1.ConfigMap{}
		err = fakeClient.Get(ctx, client.ObjectKey{Name: configMapNameForEtcdCluster(ec), Namespace: "default"}, configMap)
		t.Cleanup(func() {
			err = fakeClient.Delete(ctx, configMap) // Delete the configmap to avoid conflicts in future tests
			assert.NoError(t, err)
		})
		assert.NoError(t, err)
		assert.Equal(t, "existing", configMap.Data["ETCD_INITIAL_CLUSTER_STATE"])
		assert.Contains(t, configMap.Data["ETCD_INITIAL_CLUSTER"], "test-etcd-0=http://test-etcd-0.test-etcd.default.svc.cluster.local:2380")
		// Verify configmap is controlled by EtcdCluster
		require.Len(t, configMap.OwnerReferences, 1)
		require.Equal(t, configMap.OwnerReferences[0].Name, ec.Name)
	})

	t.Run("updates configmap if it already exists", func(t *testing.T) {
		// Create the configmap first
		configMap := newEtcdClusterState(ec, 3)
		err := fakeClient.Create(ctx, configMap)
		assert.NoError(t, err)

		// Call the function again to ensure it updates the configmap
		err = applyEtcdClusterState(ctx, ec, 3, fakeClient, scheme, logger)
		assert.NoError(t, err)

		// Verify that the configmap was updated
		updatedConfigMap := &corev1.ConfigMap{}
		err = fakeClient.Get(ctx, client.ObjectKey{Name: configMapNameForEtcdCluster(ec), Namespace: "default"}, updatedConfigMap)
		assert.NoError(t, err)
		assert.Equal(t, "existing", updatedConfigMap.Data["ETCD_INITIAL_CLUSTER_STATE"])
		assert.Contains(t, updatedConfigMap.Data["ETCD_INITIAL_CLUSTER"], "test-etcd-0=http://test-etcd-0.test-etcd.default.svc.cluster.local:2380")
		// Verify configmap is controlled by EtcdCluster
		require.Len(t, updatedConfigMap.OwnerReferences, 1)
		require.Equal(t, updatedConfigMap.OwnerReferences[0].Name, ec.Name)
	})
}

func TestCreateOrPatchStatefulSetWithPodAnnotations(t *testing.T) {
	ctx := t.Context()
	logger := log.FromContext(ctx)

	// Create a scheme and register the necessary types
	scheme := runtime.NewScheme()
	_ = corev1.AddToScheme(scheme)
	_ = ecv1alpha1.AddToScheme(scheme)
	_ = appsv1.AddToScheme(scheme)

	tests := []struct {
		name                string
		etcdClusterName     string
		podTemplate         *ecv1alpha1.PodTemplate
		expectedAnnotations map[string]string
		expectNil           bool
	}{
		{
			name:            "creates statefulset with pod annotations",
			etcdClusterName: "test-etcd",
			podTemplate: &ecv1alpha1.PodTemplate{
				Metadata: &ecv1alpha1.PodMetadata{
					Annotations: map[string]string{
						"prometheus.io/scrape": "true",
						"prometheus.io/port":   "2379",
					},
				},
			},
			expectedAnnotations: map[string]string{
				"prometheus.io/scrape": "true",
				"prometheus.io/port":   "2379",
			},
			expectNil: false,
		},
		{
			name:                "creates statefulset without pod annotations when PodTemplate is nil",
			etcdClusterName:     "test-etcd-no-podtemplate",
			podTemplate:         nil,
			expectedAnnotations: nil,
			expectNil:           true,
		},
		{
			name:            "creates statefulset without pod annotations when annotations are empty",
			etcdClusterName: "test-etcd-empty-annotations",
			podTemplate: &ecv1alpha1.PodTemplate{
				Metadata: &ecv1alpha1.PodMetadata{
					Annotations: map[string]string{},
				},
			},
			expectedAnnotations: nil,
			expectNil:           true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create a fake client for each test case to avoid interference
			fakeClient := fake.NewClientBuilder().WithScheme(scheme).Build()

			// Create an EtcdCluster instance
			ec := &ecv1alpha1.EtcdCluster{
				ObjectMeta: metav1.ObjectMeta{
					Name:      tt.etcdClusterName,
					Namespace: "default",
				},
				Spec: ecv1alpha1.EtcdClusterSpec{
					Size:        3,
					Version:     "3.5.17",
					PodTemplate: tt.podTemplate,
				},
			}

			err := createOrPatchStatefulSet(ctx, logger, ec, fakeClient, 3, scheme)
			assert.NoError(t, err)

			// Verify that the StatefulSet was created
			sts := &appsv1.StatefulSet{}
			err = fakeClient.Get(ctx, client.ObjectKey{Name: tt.etcdClusterName, Namespace: "default"}, sts)
			assert.NoError(t, err)
			// Check annotations
			if tt.expectNil {
				assert.Nil(t, sts.Spec.Template.Annotations)
			} else {
				assert.Equal(t, tt.expectedAnnotations, sts.Spec.Template.Annotations)
			}
			// Verify statefulset is controlled by EtcdCluster
			require.Len(t, sts.OwnerReferences, 1)
			require.Equal(t, sts.OwnerReferences[0].Name, ec.Name)
		})
	}
}

func TestCreateOrPatchStatefulSetWithPodLabels(t *testing.T) {
	ctx := t.Context()
	logger := log.FromContext(ctx)

	// Create a scheme and register the necessary types
	scheme := runtime.NewScheme()
	_ = corev1.AddToScheme(scheme)
	_ = ecv1alpha1.AddToScheme(scheme)
	_ = appsv1.AddToScheme(scheme)

	tests := []struct {
		name            string
		etcdClusterName string
		podTemplate     *ecv1alpha1.PodTemplate
		expectedLabels  map[string]string
	}{
		{
			name:            "creates statefulset with pod labels merged with default labels",
			etcdClusterName: "test-etcd",
			podTemplate: &ecv1alpha1.PodTemplate{
				Metadata: &ecv1alpha1.PodMetadata{
					Labels: map[string]string{
						"environment": "production",
						"version":     "v1.0.0",
						"team":        "platform",
					},
				},
			},
			expectedLabels: map[string]string{
				// Default labels that should always be present
				"app":        "test-etcd",
				"controller": "test-etcd",
				// Custom labels from PodTemplate
				"environment": "production",
				"version":     "v1.0.0",
				"team":        "platform",
			},
		},
		{
			name:            "creates statefulset with default labels when PodTemplate is nil",
			etcdClusterName: "test-etcd-no-podtemplate",
			podTemplate:     nil,
			expectedLabels: map[string]string{
				"app":        "test-etcd-no-podtemplate",
				"controller": "test-etcd-no-podtemplate",
			},
		},
		{
			name:            "creates statefulset with default labels when labels are empty",
			etcdClusterName: "test-etcd-empty-labels",
			podTemplate: &ecv1alpha1.PodTemplate{
				Metadata: &ecv1alpha1.PodMetadata{
					Labels: map[string]string{},
				},
			},
			expectedLabels: map[string]string{
				"app":        "test-etcd-empty-labels",
				"controller": "test-etcd-empty-labels",
			},
		},
		{
			name:            "default labels override custom labels when same key exists",
			etcdClusterName: "test-etcd-override",
			podTemplate: &ecv1alpha1.PodTemplate{
				Metadata: &ecv1alpha1.PodMetadata{
					Labels: map[string]string{
						"app":         "custom-app-name",   // Override default app label
						"controller":  "custom-controller", // Override default controller label
						"environment": "staging",
					},
				},
			},
			expectedLabels: map[string]string{
				"app":         "test-etcd-override", // Default labels are applied last, so they override custom ones
				"controller":  "test-etcd-override", // Default labels are applied last, so they override custom ones
				"environment": "staging",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create a fake client for each test case to avoid interference
			fakeClient := fake.NewClientBuilder().WithScheme(scheme).Build()

			// Create an EtcdCluster instance
			ec := &ecv1alpha1.EtcdCluster{
				ObjectMeta: metav1.ObjectMeta{
					Name:      tt.etcdClusterName,
					Namespace: "default",
				},
				Spec: ecv1alpha1.EtcdClusterSpec{
					Size:        3,
					Version:     "3.5.17",
					PodTemplate: tt.podTemplate,
				},
			}

			err := createOrPatchStatefulSet(ctx, logger, ec, fakeClient, 3, scheme)
			assert.NoError(t, err)

			// Verify that the StatefulSet was created with correct labels
			sts := &appsv1.StatefulSet{}
			err = fakeClient.Get(ctx, client.ObjectKey{Name: tt.etcdClusterName, Namespace: "default"}, sts)
			assert.NoError(t, err)
			// Check that pod template has the expected labels
			assert.Equal(t, tt.expectedLabels, sts.Spec.Template.Labels)
			// Verify statefulset is controlled by EtcdCluster
			require.Len(t, sts.OwnerReferences, 1)
			require.Equal(t, sts.OwnerReferences[0].Name, ec.Name)
		})
	}
}

// TestEtcdImageTag guards the registry-convention normalization: the default
// etcd-development registry only publishes v-prefixed tags, so a bare semver
// spec.version must be rendered with a leading "v" (otherwise the image is
// NotFound -> ImagePullBackOff), while an already-v-prefixed value and an
// explicit non-semver custom tag must be passed through unchanged.
func TestEtcdImageTag(t *testing.T) {
	tests := []struct {
		name    string
		version string
		want    string
	}{
		{name: "bare semver gets v prefix for the v-tagged registry", version: "3.6.1", want: "v3.6.1"},
		{name: "v-prefixed semver passes through unchanged", version: "v3.6.1", want: "v3.6.1"},
		{name: "default sample version (v-prefixed) is preserved", version: "v3.5.21", want: "v3.5.21"},
		{name: "surrounding whitespace is trimmed", version: " 3.5.21 ", want: "v3.5.21"},
		{name: "non-semver custom tag is left untouched", version: "latest", want: "latest"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, etcdImageTag(tt.version))
		})
	}
}

// TestRenderedImageRefIsVPrefixed asserts end-to-end that a bare spec.version
// renders a v-prefixed image ref against the default registry, so the StatefulSet
// pulls a tag the registry actually publishes.
func TestRenderedImageRefIsVPrefixed(t *testing.T) {
	ctx := t.Context()
	logger := log.FromContext(ctx)

	scheme := runtime.NewScheme()
	_ = corev1.AddToScheme(scheme)
	_ = ecv1alpha1.AddToScheme(scheme)
	_ = appsv1.AddToScheme(scheme)

	fakeClient := fake.NewClientBuilder().WithScheme(scheme).Build()
	ec := &ecv1alpha1.EtcdCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "test-etcd", Namespace: "default"},
		Spec: ecv1alpha1.EtcdClusterSpec{
			Size:          3,
			Version:       "3.5.21", // bare semver, as a user might write it
			ImageRegistry: "gcr.io/etcd-development/etcd",
		},
	}

	require.NoError(t, createOrPatchStatefulSet(ctx, logger, ec, fakeClient, 3, scheme))

	sts := &appsv1.StatefulSet{}
	require.NoError(t, fakeClient.Get(ctx, client.ObjectKey{Name: "test-etcd", Namespace: "default"}, sts))
	require.Len(t, sts.Spec.Template.Spec.Containers, 1)
	assert.Equal(t, "gcr.io/etcd-development/etcd:v3.5.21",
		sts.Spec.Template.Spec.Containers[0].Image,
		"bare spec.version must render a v-prefixed tag against the v-only etcd-development registry")
}

func TestCreateOrPatchStatefulSetWithSchedulingFields(t *testing.T) {
	ctx := t.Context()
	logger := log.FromContext(ctx)

	scheme := runtime.NewScheme()
	_ = corev1.AddToScheme(scheme)
	_ = ecv1alpha1.AddToScheme(scheme)
	_ = appsv1.AddToScheme(scheme)

	affinity := &corev1.Affinity{
		NodeAffinity: &corev1.NodeAffinity{
			RequiredDuringSchedulingIgnoredDuringExecution: &corev1.NodeSelector{
				NodeSelectorTerms: []corev1.NodeSelectorTerm{{
					MatchExpressions: []corev1.NodeSelectorRequirement{{
						Key:      "node-role.kubernetes.io/etcd",
						Operator: corev1.NodeSelectorOpExists,
					}},
				}},
			},
		},
	}
	nodeSelector := map[string]string{"disktype": "ssd"}
	tolerations := []corev1.Toleration{{
		Key:      "dedicated",
		Operator: corev1.TolerationOpEqual,
		Value:    "etcd",
		Effect:   corev1.TaintEffectNoSchedule,
	}}
	topologySpreadConstraints := []corev1.TopologySpreadConstraint{{
		MaxSkew:           1,
		TopologyKey:       "topology.kubernetes.io/zone",
		WhenUnsatisfiable: corev1.DoNotSchedule,
		LabelSelector: &metav1.LabelSelector{
			MatchLabels: map[string]string{"app": "test-etcd-scheduling"},
		},
	}}
	resources := &corev1.ResourceRequirements{
		Requests: corev1.ResourceList{
			corev1.ResourceCPU:    resource.MustParse("500m"),
			corev1.ResourceMemory: resource.MustParse("1Gi"),
		},
		Limits: corev1.ResourceList{
			corev1.ResourceMemory: resource.MustParse("2Gi"),
		},
	}

	fakeClient := fake.NewClientBuilder().WithScheme(scheme).Build()
	ec := &ecv1alpha1.EtcdCluster{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-etcd-scheduling",
			Namespace: "default",
		},
		Spec: ecv1alpha1.EtcdClusterSpec{
			Size:    3,
			Version: "3.5.17",
			PodTemplate: &ecv1alpha1.PodTemplate{
				Spec: &ecv1alpha1.PodSpec{
					Affinity:                  affinity,
					NodeSelector:              nodeSelector,
					Tolerations:               tolerations,
					TopologySpreadConstraints: topologySpreadConstraints,
					Resources:                 resources,
					PriorityClassName:         "system-cluster-critical",
					SchedulerName:             "custom-scheduler",
				},
			},
		},
	}

	err := createOrPatchStatefulSet(ctx, logger, ec, fakeClient, 3, scheme)
	assert.NoError(t, err)

	sts := &appsv1.StatefulSet{}
	err = fakeClient.Get(ctx, client.ObjectKey{Name: ec.Name, Namespace: "default"}, sts)
	assert.NoError(t, err)

	podSpec := sts.Spec.Template.Spec
	assert.Equal(t, affinity, podSpec.Affinity)
	assert.Equal(t, nodeSelector, podSpec.NodeSelector)
	assert.Equal(t, tolerations, podSpec.Tolerations)
	assert.Equal(t, topologySpreadConstraints, podSpec.TopologySpreadConstraints)
	assert.Equal(t, "system-cluster-critical", podSpec.PriorityClassName)
	assert.Equal(t, "custom-scheduler", podSpec.SchedulerName)
	assert.Equal(t, *resources, podSpec.Containers[0].Resources)
}

func TestCreateOrPatchStatefulSetWithoutSchedulingFields(t *testing.T) {
	ctx := t.Context()
	logger := log.FromContext(ctx)

	scheme := runtime.NewScheme()
	_ = corev1.AddToScheme(scheme)
	_ = ecv1alpha1.AddToScheme(scheme)
	_ = appsv1.AddToScheme(scheme)

	fakeClient := fake.NewClientBuilder().WithScheme(scheme).Build()
	ec := &ecv1alpha1.EtcdCluster{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-etcd-no-scheduling",
			Namespace: "default",
		},
		Spec: ecv1alpha1.EtcdClusterSpec{
			Size:    3,
			Version: "3.5.17",
		},
	}

	err := createOrPatchStatefulSet(ctx, logger, ec, fakeClient, 3, scheme)
	assert.NoError(t, err)

	sts := &appsv1.StatefulSet{}
	err = fakeClient.Get(ctx, client.ObjectKey{Name: ec.Name, Namespace: "default"}, sts)
	assert.NoError(t, err)

	podSpec := sts.Spec.Template.Spec
	assert.Nil(t, podSpec.TopologySpreadConstraints)
	assert.Empty(t, podSpec.PriorityClassName)
	// Without podTemplate resources the --etcd-cpu-request default applies.
	assert.Equal(t, etcdContainerResources(), podSpec.Containers[0].Resources)
}

func TestCreatingArgs(t *testing.T) {
	tests := []struct {
		testName       string
		etcdOptions    []string
		clusterName    string
		expectedResult []string
	}{
		{
			testName:    "No etcdOptions provided",
			etcdOptions: nil,
			clusterName: "testCluster",
			expectedResult: []string{
				"--name=$(POD_NAME)",
				"--listen-peer-urls=http://0.0.0.0:2380",
				"--listen-client-urls=http://0.0.0.0:2379",
				"--initial-advertise-peer-urls=http://$(POD_NAME).testCluster.$(POD_NAMESPACE).svc.cluster.local:2380",
				"--advertise-client-urls=http://$(POD_NAME).testCluster.$(POD_NAMESPACE).svc.cluster.local:2379",
			},
		},
		{
			testName: "Etcd options with = sign",
			etcdOptions: []string{
				"--max-wals=7",
				"--discovery-failbox=proxy",
			},
			clusterName: "testCluster",
			expectedResult: []string{
				"--name=$(POD_NAME)",
				"--listen-peer-urls=http://0.0.0.0:2380",
				"--listen-client-urls=http://0.0.0.0:2379",
				"--initial-advertise-peer-urls=http://$(POD_NAME).testCluster.$(POD_NAMESPACE).svc.cluster.local:2380",
				"--advertise-client-urls=http://$(POD_NAME).testCluster.$(POD_NAMESPACE).svc.cluster.local:2379",
				"--max-wals=7",
				"--discovery-failbox=proxy",
			},
		},
		{
			testName: "Etcd options with spaces",
			etcdOptions: []string{
				"--max-wals 7",
				"--discovery-failbox proxy",
			},
			clusterName: "testCluster",
			expectedResult: []string{
				"--name=$(POD_NAME)",
				"--listen-peer-urls=http://0.0.0.0:2380",
				"--listen-client-urls=http://0.0.0.0:2379",
				"--initial-advertise-peer-urls=http://$(POD_NAME).testCluster.$(POD_NAMESPACE).svc.cluster.local:2380",
				"--advertise-client-urls=http://$(POD_NAME).testCluster.$(POD_NAMESPACE).svc.cluster.local:2379",
				"--max-wals 7",
				"--discovery-failbox proxy",
			},
		},
		{
			testName: "Etcd switch options",
			etcdOptions: []string{
				"--experimental-peer-skip-client-san-verification",
			},
			clusterName: "testCluster",
			expectedResult: []string{
				"--name=$(POD_NAME)",
				"--listen-peer-urls=http://0.0.0.0:2380",
				"--listen-client-urls=http://0.0.0.0:2379",
				"--initial-advertise-peer-urls=http://$(POD_NAME).testCluster.$(POD_NAMESPACE).svc.cluster.local:2380",
				"--advertise-client-urls=http://$(POD_NAME).testCluster.$(POD_NAMESPACE).svc.cluster.local:2379",
				"--experimental-peer-skip-client-san-verification",
			},
		},
		{
			testName: "Overwrite default arg",
			etcdOptions: []string{
				"--listen-peer-urls=http://0.0.0.0:3200",
				"--experimental-peer-skip-client-san-verification",
			},
			clusterName: "testCluster",
			expectedResult: []string{
				"--name=$(POD_NAME)",
				"--listen-client-urls=http://0.0.0.0:2379",
				"--initial-advertise-peer-urls=http://$(POD_NAME).testCluster.$(POD_NAMESPACE).svc.cluster.local:2380",
				"--advertise-client-urls=http://$(POD_NAME).testCluster.$(POD_NAMESPACE).svc.cluster.local:2379",
				"--listen-peer-urls=http://0.0.0.0:3200",
				"--experimental-peer-skip-client-san-verification",
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.testName, func(t *testing.T) {
			result := createArgs(tt.clusterName, tt.etcdOptions, tlsArgs{})
			assert.Equal(t, tt.expectedResult, result)
		})
	}

}

func TestValidateEtcdUpgradePath(t *testing.T) {
	etcdVersions := []semver.Version{
		{Major: 3, Minor: 0},
		{Major: 3, Minor: 1},
		{Major: 3, Minor: 2},
		{Major: 3, Minor: 3},
		{Major: 3, Minor: 4},
		{Major: 3, Minor: 5},
		{Major: 3, Minor: 6},
		{Major: 3, Minor: 7},
		{Major: 4, Minor: 0},
	}

	tests := []struct {
		name      string
		current   string
		target    string
		canParse  bool
		expectErr bool
	}{
		{
			name:      "equal versions",
			current:   "3.2.0",
			target:    "3.2.0",
			canParse:  true,
			expectErr: false,
		},
		{
			name:      "valid minor level upgrade",
			current:   "3.4.0",
			target:    "3.5.0",
			canParse:  true,
			expectErr: false,
		},
		{
			name:      "valid patch level upgrade",
			current:   "3.4.0",
			target:    "3.4.1",
			canParse:  true,
			expectErr: false,
		},
		{
			name:      "invalid current version",
			current:   "invalid",
			target:    "3.1.0",
			canParse:  false,
			expectErr: true,
		},
		{
			name:      "invalid target version",
			current:   "3.1.0",
			target:    "invalid",
			canParse:  false,
			expectErr: true,
		},
		{
			name:      "minor downgrade not allowed",
			current:   "3.2.0",
			target:    "3.1.0",
			canParse:  true,
			expectErr: true,
		},
		{
			name:      "patch downgrade not allowed",
			current:   "3.5.1",
			target:    "3.5.0",
			canParse:  true,
			expectErr: true,
		},
		{
			name:      "unknown current version",
			current:   "3.9.0",
			target:    "4.0.0",
			canParse:  true,
			expectErr: true,
		},
		{
			name:      "unknown target version",
			current:   "4.0.0",
			target:    "4.1.0",
			canParse:  true,
			expectErr: true,
		},
		{
			name:      "invalid upgrade skipping minor",
			current:   "3.4.0",
			target:    "3.6.0",
			canParse:  true,
			expectErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			canParse, err := validateEtcdUpgradePath(etcdVersions, tt.current, tt.target)

			if canParse != tt.canParse {
				t.Fatalf("expected canParse=%v, got %v", tt.canParse, canParse)
			}

			if tt.expectErr && err == nil {
				t.Fatalf("expected error, got nil")
			}

			if !tt.expectErr && err != nil {
				t.Fatalf("did not expect error, got %v", err)
			}
		})
	}
}

func TestCreateAutoCertificateConfig(t *testing.T) {
	tests := []struct {
		name     string
		ec       *ecv1alpha1.EtcdCluster
		surface  *ecv1alpha1.TLSSurface
		expected *certInterface.Config
		wantErr  bool
	}{
		{
			name: "auto config with all fields set",
			ec: &ecv1alpha1.EtcdCluster{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-cluster",
					Namespace: "test-namespace",
				},
			},
			surface: &ecv1alpha1.TLSSurface{
				Provider: string(certificate.Auto),
				ProviderCfg: ecv1alpha1.ProviderConfig{
					AutoCfg: &ecv1alpha1.ProviderAutoConfig{
						CommonConfig: ecv1alpha1.CommonConfig{
							CommonName:       "custom.example.com",
							Organization:     []string{"Test Org"},
							ValidityDuration: "720h", // 30 days
							AltNames: ecv1alpha1.AltNames{
								DNSNames: []string{"custom1.example.com", "custom2.example.com"},
								IPs:      []net.IP{net.ParseIP("10.96.99.99")},
							},
						},
					},
				},
			},
			expected: &certInterface.Config{
				CommonName:       "custom.example.com",
				Organization:     []string{"Test Org"},
				ValidityDuration: 720 * time.Hour, // 30 days
				AltNames: certInterface.AltNames{
					DNSNames: []string{"custom1.example.com", "custom2.example.com"},
					IPs:      []net.IP{net.ParseIP("10.96.99.99")},
				},
			},
			wantErr: false,
		},
		{
			name: "auto config with only ipAddresses - IPs kept alongside default DNS names",
			ec: &ecv1alpha1.EtcdCluster{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-cluster",
					Namespace: "test-namespace",
				},
			},
			surface: &ecv1alpha1.TLSSurface{
				Provider: string(certificate.Auto),
				ProviderCfg: ecv1alpha1.ProviderConfig{
					AutoCfg: &ecv1alpha1.ProviderAutoConfig{
						CommonConfig: ecv1alpha1.CommonConfig{
							CommonName: "custom.example.com",
							AltNames: ecv1alpha1.AltNames{
								IPs: []net.IP{net.ParseIP("10.96.99.99")},
							},
						},
					},
				},
			},
			expected: &certInterface.Config{
				CommonName:       "custom.example.com",
				ValidityDuration: certInterface.DefaultAutoValidity,
				AltNames: certInterface.AltNames{
					DNSNames: []string{
						"*.test-cluster.test-namespace.svc.cluster.local",
						"test-cluster.test-namespace.svc.cluster.local",
					},
					IPs: []net.IP{net.ParseIP("10.96.99.99")},
				},
			},
			wantErr: false,
		},
		{
			name: "auto config with invalid IP entry",
			ec: &ecv1alpha1.EtcdCluster{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-cluster",
					Namespace: "test-namespace",
				},
			},
			surface: &ecv1alpha1.TLSSurface{
				Provider: string(certificate.Auto),
				ProviderCfg: ecv1alpha1.ProviderConfig{
					AutoCfg: &ecv1alpha1.ProviderAutoConfig{
						CommonConfig: ecv1alpha1.CommonConfig{
							CommonName: "custom.example.com",
							AltNames: ecv1alpha1.AltNames{
								IPs: []net.IP{nil},
							},
						},
					},
				},
			},
			expected: nil,
			wantErr:  true,
		},
		{
			name: "auto config with nil AutoCfg - should use defaults",
			ec: &ecv1alpha1.EtcdCluster{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-cluster",
					Namespace: "test-namespace",
				},
			},
			surface: &ecv1alpha1.TLSSurface{
				Provider: string(certificate.Auto),
				ProviderCfg: ecv1alpha1.ProviderConfig{
					AutoCfg: nil,
				},
			},
			expected: &certInterface.Config{
				CommonName:       "test-cluster.test-namespace.svc.cluster.local",
				Organization:     nil,
				ValidityDuration: certInterface.DefaultAutoValidity,
				AltNames: certInterface.AltNames{
					DNSNames: []string{
						"*.test-cluster.test-namespace.svc.cluster.local",
						"test-cluster.test-namespace.svc.cluster.local",
					},
				},
			},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := createAutoCertificateConfig(tt.ec, tt.surface)

			if tt.wantErr {
				require.Error(t, err)
				assert.Nil(t, result)
			} else {
				require.NoError(t, err)
				require.NotNil(t, result)
				assert.Equal(t, tt.expected.CommonName, result.CommonName)
				assert.Equal(t, tt.expected.Organization, result.Organization)
				assert.Equal(t, tt.expected.ValidityDuration, result.ValidityDuration)
				assert.Equal(t, tt.expected.AltNames.DNSNames, result.AltNames.DNSNames)
				assert.Equal(t, tt.expected.AltNames.IPs, result.AltNames.IPs)
			}
		})
	}
}

func TestCreateCMCertificateConfig(t *testing.T) {
	tests := []struct {
		name     string
		ec       *ecv1alpha1.EtcdCluster
		surface  *ecv1alpha1.TLSSurface
		expected *certInterface.Config
		wantErr  bool
	}{
		{
			name: "cert-manager config with all fields set",
			ec: &ecv1alpha1.EtcdCluster{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-cluster",
					Namespace: "test-namespace",
				},
			},
			surface: &ecv1alpha1.TLSSurface{
				Provider: string(certificate.CertManager),
				ProviderCfg: ecv1alpha1.ProviderConfig{
					CertManagerCfg: &ecv1alpha1.ProviderCertManagerConfig{
						CommonConfig: ecv1alpha1.CommonConfig{
							CommonName:       "cm.example.com",
							Organization:     []string{"CM Org"},
							ValidityDuration: "1440h", // 60 days
							AltNames: ecv1alpha1.AltNames{
								DNSNames: []string{"cm1.example.com", "cm2.example.com"},
								IPs:      []net.IP{net.ParseIP("10.96.99.99")},
							},
						},
						IssuerName:  "test-issuer",
						IssuerKind:  "ClusterIssuer",
						IssuerGroup: "example.io",
					},
				},
			},
			expected: &certInterface.Config{
				CommonName:       "cm.example.com",
				Organization:     []string{"CM Org"},
				ValidityDuration: 1440 * time.Hour, // 60 days
				AltNames: certInterface.AltNames{
					DNSNames: []string{"cm1.example.com", "cm2.example.com"},
					IPs:      []net.IP{net.ParseIP("10.96.99.99")},
				},
				ExtraConfig: map[string]any{
					"issuerName":  "test-issuer",
					"issuerKind":  "ClusterIssuer",
					"issuerGroup": "example.io",
				},
			},
			wantErr: false,
		},
		{
			name: "cert-manager config with only ipAddresses - IPs kept alongside default DNS names",
			ec: &ecv1alpha1.EtcdCluster{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-cluster",
					Namespace: "test-namespace",
				},
			},
			surface: &ecv1alpha1.TLSSurface{
				Provider: string(certificate.CertManager),
				ProviderCfg: ecv1alpha1.ProviderConfig{
					CertManagerCfg: &ecv1alpha1.ProviderCertManagerConfig{
						CommonConfig: ecv1alpha1.CommonConfig{
							CommonName: "cm.example.com",
							AltNames: ecv1alpha1.AltNames{
								IPs: []net.IP{net.ParseIP("10.96.99.99")},
							},
						},
						IssuerName:  "test-issuer",
						IssuerKind:  "ClusterIssuer",
						IssuerGroup: "example.io",
					},
				},
			},
			expected: &certInterface.Config{
				CommonName:       "cm.example.com",
				ValidityDuration: certInterface.DefaultCertManagerValidity,
				AltNames: certInterface.AltNames{
					DNSNames: []string{
						"*.test-cluster.test-namespace.svc.cluster.local",
						"test-cluster.test-namespace.svc.cluster.local",
					},
					IPs: []net.IP{net.ParseIP("10.96.99.99")},
				},
				ExtraConfig: map[string]any{
					"issuerName":  "test-issuer",
					"issuerKind":  "ClusterIssuer",
					"issuerGroup": "example.io",
				},
			},
			wantErr: false,
		},
		{
			name: "cert-manager config with nil CertManagerCfg",
			ec: &ecv1alpha1.EtcdCluster{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-cluster",
					Namespace: "test-namespace",
				},
			},
			surface: &ecv1alpha1.TLSSurface{
				Provider: string(certificate.CertManager),
				ProviderCfg: ecv1alpha1.ProviderConfig{
					CertManagerCfg: nil,
				},
			},
			expected: nil,
			wantErr:  true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := createCMCertificateConfig(tt.ec, tt.surface)

			if tt.wantErr {
				require.Error(t, err)
				assert.Nil(t, result)
			} else {
				require.NoError(t, err)
				require.NotNil(t, result)
				assert.Equal(t, tt.expected.CommonName, result.CommonName)
				assert.Equal(t, tt.expected.Organization, result.Organization)
				assert.Equal(t, tt.expected.ValidityDuration, result.ValidityDuration)
				assert.Equal(t, tt.expected.AltNames.DNSNames, result.AltNames.DNSNames)
				assert.Equal(t, tt.expected.AltNames.IPs, result.AltNames.IPs)
				assert.Equal(t, tt.expected.ExtraConfig, result.ExtraConfig)
			}
		})
	}
}

// getEtcdContainer returns the etcd container from a StatefulSet, or fails the test.
func getEtcdContainer(t *testing.T, sts *appsv1.StatefulSet) corev1.Container {
	t.Helper()
	for _, c := range sts.Spec.Template.Spec.Containers {
		if c.Name == "etcd" {
			return c
		}
	}
	t.Fatalf("no etcd container found in StatefulSet %q", sts.Name)
	return corev1.Container{}
}

func TestEtcdContainerCPURequest(t *testing.T) {
	ctx := t.Context()
	logger := log.FromContext(ctx)

	scheme := runtime.NewScheme()
	require.NoError(t, corev1.AddToScheme(scheme))
	require.NoError(t, ecv1alpha1.AddToScheme(scheme))
	require.NoError(t, appsv1.AddToScheme(scheme))

	// Preserve and restore the package-level knob so this test does not leak
	// state into other tests in the package.
	orig := EtcdCPURequest
	defer func() { EtcdCPURequest = orig }()

	tests := []struct {
		name        string
		cpuRequest  string
		wantRequest bool
		wantQty     string
	}{
		{
			name:        "default 50m sets a CPU request (Burstable)",
			cpuRequest:  DefaultEtcdCPURequest,
			wantRequest: true,
			wantQty:     "50m",
		},
		{
			name:        "explicit override is honored",
			cpuRequest:  "100m",
			wantRequest: true,
			wantQty:     "100m",
		},
		{
			name:        "empty string disables the request (BestEffort)",
			cpuRequest:  "",
			wantRequest: false,
		},
		{
			name:        "zero disables the request (BestEffort)",
			cpuRequest:  "0",
			wantRequest: false,
		},
		{
			name:        "malformed quantity is treated as unset (BestEffort)",
			cpuRequest:  "not-a-quantity",
			wantRequest: false,
		},
	}

	for i, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			EtcdCPURequest = tt.cpuRequest

			fakeClient := fake.NewClientBuilder().WithScheme(scheme).Build()
			name := fmt.Sprintf("test-etcd-cpu-%d", i)
			ec := &ecv1alpha1.EtcdCluster{
				ObjectMeta: metav1.ObjectMeta{
					Name:      name,
					Namespace: "default",
				},
				Spec: ecv1alpha1.EtcdClusterSpec{
					Size:    3,
					Version: "3.5.17",
				},
			}

			err := createOrPatchStatefulSet(ctx, logger, ec, fakeClient, 3, scheme)
			require.NoError(t, err)

			sts := &appsv1.StatefulSet{}
			require.NoError(t, fakeClient.Get(ctx, client.ObjectKey{Name: name, Namespace: "default"}, sts))

			c := getEtcdContainer(t, sts)
			cpu, hasCPU := c.Resources.Requests[corev1.ResourceCPU]

			if tt.wantRequest {
				require.True(t, hasCPU, "expected etcd container to carry a CPU request")
				assert.True(t, cpu.Equal(resource.MustParse(tt.wantQty)),
					"expected CPU request %s, got %s", tt.wantQty, cpu.String())
				// It is a request, never a limit: etcd must not be throttled.
				_, hasLimit := c.Resources.Limits[corev1.ResourceCPU]
				assert.False(t, hasLimit, "etcd container must not carry a CPU limit")
			} else {
				assert.False(t, hasCPU, "expected no CPU request (BestEffort), got %s", cpu.String())
			}
		})
	}
}

func TestParseValidityDuration(t *testing.T) {
	tests := []struct {
		name        string
		input       string
		expected    time.Duration
		expectedErr bool
	}{
		{name: "day suffix", input: "365d", expected: 365 * 24 * time.Hour},
		{name: "days and hours", input: "100d12h", expected: 100*24*time.Hour + 12*time.Hour},
		{name: "plain go duration", input: "24h", expected: 24 * time.Hour},
		{name: "minutes", input: "90m", expected: 90 * time.Minute},
		{name: "empty returns default", input: "", expected: certInterface.DefaultCertManagerValidity},
		{name: "invalid string", input: "abc", expectedErr: true},
		{name: "day unit without value", input: "d", expectedErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			duration, err := parseValidityDuration(tt.input, certInterface.DefaultCertManagerValidity)
			if tt.expectedErr {
				require.Error(t, err)
				assert.ErrorContains(t, err, "failed to parse ValidityDuration")
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.expected, duration)
		})
	}
}

// scaleInSts returns a StatefulSet whose client endpoints match scaleInEpHealth.
func scaleInSts() *appsv1.StatefulSet {
	return &appsv1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{Name: "test-etcd", Namespace: "default"},
	}
}

// scaleInEpHealth builds an EpHealth for the given ordinal using the real
// endpoint format produced by clientEndpointForOrdinalIndex.
func scaleInEpHealth(ordinal int, memberID uint64, healthy, learner bool) etcdutils.EpHealth {
	return etcdutils.EpHealth{
		Ep:     fmt.Sprintf("http://test-etcd-%d.test-etcd.default.svc.cluster.local:2379", ordinal),
		Health: healthy,
		Status: &clientv3.StatusResponse{
			Header:    &etcdserverpb.ResponseHeader{MemberId: memberID},
			IsLearner: learner,
		},
	}
}

func scaleInMember(ordinal int, id uint64) *etcdserverpb.Member {
	return &etcdserverpb.Member{Name: fmt.Sprintf("test-etcd-%d", ordinal), ID: id}
}

// sortLexically reproduces etcdutils.ClusterHealth's healthReport sort order.
func sortLexically(infos []etcdutils.EpHealth) {
	sort.Slice(infos, func(i, j int) bool { return infos[i].Ep < infos[j].Ep })
}

func TestScaleInTargetID(t *testing.T) {
	sts := scaleInSts()

	t.Run("3 members happy path", func(t *testing.T) {
		members := []*etcdserverpb.Member{
			scaleInMember(1, 101),
			scaleInMember(0, 100),
			scaleInMember(2, 102),
		}
		id, err := scaleInTargetID(sts, members)
		require.NoError(t, err)
		assert.Equal(t, uint64(102), id)
	})

	t.Run("11 members: highest ordinal wins regardless of order", func(t *testing.T) {
		var members []*etcdserverpb.Member
		// Reverse order — member list order carries no ordinal meaning.
		for i := 10; i >= 0; i-- {
			members = append(members, scaleInMember(i, uint64(100+i)))
		}
		id, err := scaleInTargetID(sts, members)
		require.NoError(t, err)
		assert.Equal(t, uint64(110), id)
	})

	t.Run("empty member list", func(t *testing.T) {
		_, err := scaleInTargetID(sts, nil)
		assert.Error(t, err)
	})

	t.Run("target name missing from member list", func(t *testing.T) {
		// Unstarted members report an empty name.
		members := []*etcdserverpb.Member{
			scaleInMember(0, 100),
			{ID: 101},
		}
		_, err := scaleInTargetID(sts, members)
		assert.Error(t, err)
	})
}

func TestTransfereeForScaleIn(t *testing.T) {
	sts := scaleInSts()

	t.Run("returns lowest ordinal voting member", func(t *testing.T) {
		infos := []etcdutils.EpHealth{
			scaleInEpHealth(0, 100, true, false),
			scaleInEpHealth(1, 101, true, false),
			scaleInEpHealth(2, 102, true, false),
		}
		transferee, ok := transfereeForScaleIn(sts, infos, 102, "http")
		assert.True(t, ok)
		assert.Equal(t, uint64(100), transferee)
	})

	t.Run("skips learner at ordinal 0", func(t *testing.T) {
		infos := []etcdutils.EpHealth{
			scaleInEpHealth(0, 100, true, true),
			scaleInEpHealth(1, 101, true, false),
			scaleInEpHealth(2, 102, true, false),
		}
		transferee, ok := transfereeForScaleIn(sts, infos, 102, "http")
		assert.True(t, ok)
		assert.Equal(t, uint64(101), transferee)
	})

	t.Run("only remaining member is a learner", func(t *testing.T) {
		infos := []etcdutils.EpHealth{
			scaleInEpHealth(0, 100, true, true),
			scaleInEpHealth(1, 101, true, false),
		}
		_, ok := transfereeForScaleIn(sts, infos, 101, "http")
		assert.False(t, ok)
	})

	t.Run("skips unhealthy members", func(t *testing.T) {
		infos := []etcdutils.EpHealth{
			scaleInEpHealth(0, 100, false, false),
			scaleInEpHealth(1, 101, true, false),
			scaleInEpHealth(2, 102, true, false),
		}
		transferee, ok := transfereeForScaleIn(sts, infos, 102, "http")
		assert.True(t, ok)
		assert.Equal(t, uint64(101), transferee)
	})

	t.Run("never returns the removal target", func(t *testing.T) {
		infos := []etcdutils.EpHealth{
			scaleInEpHealth(0, 100, true, false),
		}
		_, ok := transfereeForScaleIn(sts, infos, 100, "http")
		assert.False(t, ok)
	})
}

func TestRemoveScaleInMember(t *testing.T) {
	logger := logr.Discard()
	eps := []string{"ep0", "ep1"}

	type call struct {
		fn  string
		eps []string
		id  uint64
	}
	// Per-test stubs capture into a local slice — no shared state between sub-tests.
	stubs := func(calls *[]call, moveErr error) (moveLeader, removeMember func([]string, uint64, *tls.Config) error) {
		return func(eps []string, id uint64, _ *tls.Config) error {
				*calls = append(*calls, call{fn: "move", eps: eps, id: id})
				return moveErr
			}, func(eps []string, id uint64, _ *tls.Config) error {
				*calls = append(*calls, call{fn: "remove", eps: eps, id: id})
				return nil
			}
	}
	newState := func(memberHealth []etcdutils.EpHealth, members ...*etcdserverpb.Member) *reconcileState {
		return &reconcileState{
			sts:            scaleInSts(),
			memberListResp: &clientv3.MemberListResponse{Members: members},
			memberHealth:   memberHealth,
		}
	}

	threeHealthy := []etcdutils.EpHealth{
		scaleInEpHealth(0, 100, true, false),
		scaleInEpHealth(1, 101, true, false),
		scaleInEpHealth(2, 102, true, false),
	}
	threeMembers := []*etcdserverpb.Member{
		scaleInMember(0, 100), scaleInMember(1, 101), scaleInMember(2, 102),
	}

	t.Run("target is leader: transfer before removal", func(t *testing.T) {
		var calls []call
		moveLeader, removeMember := stubs(&calls, nil)
		err := removeScaleInMember(logger, newState(threeHealthy, threeMembers...), 102, eps,
			"http", nil, moveLeader, removeMember)
		require.NoError(t, err)
		require.Len(t, calls, 2)
		assert.Equal(t, "move", calls[0].fn)
		assert.Equal(t, []string{threeHealthy[2].Ep}, calls[0].eps)
		assert.Equal(t, uint64(100), calls[0].id)
		assert.Equal(t, "remove", calls[1].fn)
		assert.Equal(t, eps, calls[1].eps)
		assert.Equal(t, uint64(102), calls[1].id)
	})

	t.Run("target not leader: no transfer", func(t *testing.T) {
		var calls []call
		moveLeader, removeMember := stubs(&calls, nil)
		err := removeScaleInMember(logger, newState(threeHealthy, threeMembers...), 100, eps,
			"http", nil, moveLeader, removeMember)
		require.NoError(t, err)
		require.Len(t, calls, 1)
		assert.Equal(t, "remove", calls[0].fn)
		assert.Equal(t, uint64(102), calls[0].id)
	})

	t.Run("transfer failure does not block removal", func(t *testing.T) {
		var calls []call
		moveLeader, removeMember := stubs(&calls, errors.New("transfer failed"))
		err := removeScaleInMember(logger, newState(threeHealthy, threeMembers...), 102, eps,
			"http", nil, moveLeader, removeMember)
		require.NoError(t, err)
		require.Len(t, calls, 2)
		assert.Equal(t, "move", calls[0].fn)
		assert.Equal(t, "remove", calls[1].fn)
	})

	t.Run("leader target with no eligible transferee", func(t *testing.T) {
		var calls []call
		moveLeader, removeMember := stubs(&calls, nil)
		infos := []etcdutils.EpHealth{
			scaleInEpHealth(0, 100, true, true), // learner survivor
			scaleInEpHealth(1, 101, true, false),
		}
		s := newState(infos, scaleInMember(0, 100), scaleInMember(1, 101))
		err := removeScaleInMember(logger, s, 101, eps, "http", nil, moveLeader, removeMember)
		require.NoError(t, err)
		require.Len(t, calls, 1)
		assert.Equal(t, "remove", calls[0].fn)
		assert.Equal(t, uint64(101), calls[0].id)
	})

	t.Run("unreachable target still removed via member list", func(t *testing.T) {
		var calls []call
		moveLeader, removeMember := stubs(&calls, nil)
		infos := []etcdutils.EpHealth{
			scaleInEpHealth(0, 100, true, false),
			scaleInEpHealth(1, 101, true, false),
			{Ep: "http://test-etcd-2.test-etcd.default.svc.cluster.local:2379"}, // no status
		}
		err := removeScaleInMember(logger, newState(infos, threeMembers...), 100, eps,
			"http", nil, moveLeader, removeMember)
		require.NoError(t, err)
		require.Len(t, calls, 1)
		assert.Equal(t, "remove", calls[0].fn)
		assert.Equal(t, uint64(102), calls[0].id)
	})

	t.Run("11 members: lexical health order, leader at ordinal 10", func(t *testing.T) {
		var calls []call
		moveLeader, removeMember := stubs(&calls, nil)
		var infos []etcdutils.EpHealth
		var members []*etcdserverpb.Member
		for i := 0; i <= 10; i++ {
			infos = append(infos, scaleInEpHealth(i, uint64(100+i), true, false))
			members = append(members, scaleInMember(i, uint64(100+i)))
		}
		sortLexically(infos)
		// The lexically-last health entry is ordinal 9 — the old
		// memberHealth[memberCnt-1] selection would remove the wrong member.
		assert.Equal(t, scaleInEpHealth(9, 109, true, false).Ep, infos[len(infos)-1].Ep)

		err := removeScaleInMember(logger, newState(infos, members...), 110, eps,
			"http", nil, moveLeader, removeMember)
		require.NoError(t, err)
		require.Len(t, calls, 2)
		assert.Equal(t, "move", calls[0].fn)
		assert.Equal(t, []string{"http://test-etcd-10.test-etcd.default.svc.cluster.local:2379"}, calls[0].eps)
		assert.Equal(t, uint64(100), calls[0].id)
		assert.Equal(t, "remove", calls[1].fn)
		assert.Equal(t, uint64(110), calls[1].id)
	})
}
