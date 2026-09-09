package resourceclaims

import (
	"testing"

	rootconfig "github.com/loft-sh/vcluster/config"
	"github.com/loft-sh/vcluster/pkg/config"
	"github.com/loft-sh/vcluster/pkg/mappings"
	"github.com/loft-sh/vcluster/pkg/scheme"
	"github.com/loft-sh/vcluster/pkg/syncer/synccontext"
	syncertesting "github.com/loft-sh/vcluster/pkg/syncer/testing"
	testingutil "github.com/loft-sh/vcluster/pkg/util/testing"
	"github.com/loft-sh/vcluster/pkg/util/translate"
	"gotest.tools/assert"
	corev1 "k8s.io/api/core/v1"
	resourcev1 "k8s.io/api/resource/v1"
	kerrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/ptr"
)

func TestSync(t *testing.T) {
	translate.Default = translate.NewSingleNamespaceTranslator(testingutil.DefaultTestTargetNamespace)
	vObjectMeta := metav1.ObjectMeta{
		Name:            "gpu-claim",
		Namespace:       "test",
		ResourceVersion: syncertesting.FakeClientResourceVersion,
	}
	hostName := translate.Default.HostNameShort(nil, vObjectMeta.Name, vObjectMeta.Namespace)
	pObjectMeta := metav1.ObjectMeta{
		Name:            hostName.Name,
		Namespace:       "test",
		ResourceVersion: syncertesting.FakeClientResourceVersion,
		Annotations: map[string]string{
			translate.NameAnnotation:          vObjectMeta.Name,
			translate.NamespaceAnnotation:     vObjectMeta.Namespace,
			translate.UIDAnnotation:           "",
			translate.KindAnnotation:          resourcev1.SchemeGroupVersion.WithKind("ResourceClaim").String(),
			translate.HostNameAnnotation:      hostName.Name,
			translate.HostNamespaceAnnotation: "test",
		},
		Labels: map[string]string{
			translate.MarkerLabel:    translate.VClusterName,
			translate.NamespaceLabel: vObjectMeta.Namespace,
		},
	}

	claimSpec := resourcev1.ResourceClaimSpec{
		Devices: resourcev1.DeviceClaim{
			Requests: []resourcev1.DeviceRequest{
				{
					Name: "gpu",
					Exactly: &resourcev1.ExactDeviceRequest{
						DeviceClassName: "gpu.nvidia.com",
					},
				},
			},
		},
	}

	vClaim := &resourcev1.ResourceClaim{
		ObjectMeta: vObjectMeta,
		Spec:       claimSpec,
	}
	pClaim := &resourcev1.ResourceClaim{
		ObjectMeta: pObjectMeta,
		Spec:       claimSpec,
	}

	allocatedHost := pClaim.DeepCopy()
	allocatedHost.Status = resourcev1.ResourceClaimStatus{
		Allocation: &resourcev1.AllocationResult{
			Devices: resourcev1.DeviceAllocationResult{
				Results: []resourcev1.DeviceRequestAllocationResult{
					{
						Request: "gpu",
						Driver:  "gpu.nvidia.com",
						Pool:    "node-a",
						Device:  "gpu-0",
					},
				},
			},
		},
		ReservedFor: []resourcev1.ResourceClaimConsumerReference{
			{
				Resource: "pods",
				Name:     "host-pod",
				UID:      types.UID("host-uid"),
			},
		},
	}
	allocatedVirtual := vClaim.DeepCopy()
	allocatedVirtual.Status = allocatedHost.Status

	hostDeviceClass := &resourcev1.DeviceClass{
		ObjectMeta: metav1.ObjectMeta{
			Name: "gpu.nvidia.com",
			Labels: map[string]string{
				"environment": "prod",
			},
		},
	}

	syncertesting.RunTestsWithContext(t, func(vConfig *config.VirtualClusterConfig, pClient *testingutil.FakeIndexClient, vClient *testingutil.FakeIndexClient) *synccontext.RegisterContext {
		vConfig.Sync.ToHost.ResourceClaims.Enabled = true
		return syncertesting.NewFakeRegisterContext(vConfig, pClient, vClient)
	}, []*syncertesting.SyncTest{
		{
			Name:                "Create forward",
			InitialVirtualState: []runtime.Object{vClaim.DeepCopy()},
			ExpectedVirtualState: map[schema.GroupVersionKind][]runtime.Object{
				resourcev1.SchemeGroupVersion.WithKind("ResourceClaim"): {vClaim.DeepCopy()},
			},
			ExpectedPhysicalState: map[schema.GroupVersionKind][]runtime.Object{
				resourcev1.SchemeGroupVersion.WithKind("ResourceClaim"): {pClaim.DeepCopy()},
			},
			Sync: func(ctx *synccontext.RegisterContext) {
				syncCtx, syncer := syncertesting.FakeStartSyncer(t, ctx, New)
				_, err := syncer.(*resourceClaimSyncer).SyncToHost(syncCtx, synccontext.NewSyncToHostEvent(vClaim.DeepCopy()))
				assert.NilError(t, err)
			},
		},
		{
			Name:                 "Copy allocation status back",
			InitialVirtualState:  []runtime.Object{vClaim.DeepCopy()},
			InitialPhysicalState: []runtime.Object{allocatedHost.DeepCopy()},
			ExpectedVirtualState: map[schema.GroupVersionKind][]runtime.Object{
				resourcev1.SchemeGroupVersion.WithKind("ResourceClaim"): {allocatedVirtual.DeepCopy()},
			},
			ExpectedPhysicalState: map[schema.GroupVersionKind][]runtime.Object{
				resourcev1.SchemeGroupVersion.WithKind("ResourceClaim"): {allocatedHost.DeepCopy()},
			},
			Sync: func(ctx *synccontext.RegisterContext) {
				syncCtx, syncer := syncertesting.FakeStartSyncer(t, ctx, New)
				_, err := syncer.(*resourceClaimSyncer).Sync(syncCtx, synccontext.NewSyncEventWithOld(
					allocatedHost.DeepCopy(),
					allocatedHost.DeepCopy(),
					vClaim.DeepCopy(),
					vClaim.DeepCopy(),
				))
				assert.NilError(t, err)
			},
		},
		{
			Name:                 "Delete host when virtual is gone",
			InitialPhysicalState: []runtime.Object{pClaim.DeepCopy()},
			ExpectedVirtualState: map[schema.GroupVersionKind][]runtime.Object{},
			ExpectedPhysicalState: map[schema.GroupVersionKind][]runtime.Object{
				resourcev1.SchemeGroupVersion.WithKind("ResourceClaim"): {},
			},
			Sync: func(ctx *synccontext.RegisterContext) {
				syncCtx, syncer := syncertesting.FakeStartSyncer(t, ctx, New)
				_, err := syncer.(*resourceClaimSyncer).SyncToVirtual(syncCtx, synccontext.NewSyncToVirtualEvent(pClaim.DeepCopy()))
				assert.NilError(t, err)
			},
		},
	})

	syncertesting.RunTestsWithContext(t, func(vConfig *config.VirtualClusterConfig, pClient *testingutil.FakeIndexClient, vClient *testingutil.FakeIndexClient) *synccontext.RegisterContext {
		vConfig.Sync.ToHost.ResourceClaims.Enabled = true
		vConfig.Sync.FromHost.DeviceClasses.Enabled = true
		vConfig.Sync.FromHost.DeviceClasses.Selector = rootconfig.StandardLabelSelector{
			MatchLabels: map[string]string{"environment": "dev"},
		}
		return syncertesting.NewFakeRegisterContext(vConfig, pClient, vClient)
	}, []*syncertesting.SyncTest{
		{
			Name:                "Reject claim whose device class misses the selector",
			InitialVirtualState: []runtime.Object{vClaim.DeepCopy()},
			InitialPhysicalState: []runtime.Object{
				hostDeviceClass.DeepCopy(),
			},
			ExpectedVirtualState: map[schema.GroupVersionKind][]runtime.Object{
				resourcev1.SchemeGroupVersion.WithKind("ResourceClaim"): {vClaim.DeepCopy()},
			},
			ExpectedPhysicalState: map[schema.GroupVersionKind][]runtime.Object{
				resourcev1.SchemeGroupVersion.WithKind("DeviceClass"): {hostDeviceClass.DeepCopy()},
			},
			Sync: func(ctx *synccontext.RegisterContext) {
				syncCtx, syncer := syncertesting.FakeStartSyncer(t, ctx, New)
				_, err := syncer.(*resourceClaimSyncer).SyncToHost(syncCtx, synccontext.NewSyncToHostEvent(vClaim.DeepCopy()))
				assert.NilError(t, err)
			},
		},
	})
}

func TestGeneratedResourceClaimImportAndBackSync(t *testing.T) {
	translate.Default = translate.NewSingleNamespaceTranslator(testingutil.DefaultTestTargetNamespace)

	const (
		vNamespace    = "tenant"
		vPodName      = "dra-pod"
		pPodName      = "dra-pod-x-tenant-x-host"
		podClaimName  = "roce"
		vTemplateName = "test-roce"
		pTemplateName = "v1udye8nbz13pd"
		pClaimName    = "dra-pod-x-tenant-x-host-roce-abcde"
		vPodUID       = types.UID("virtual-pod-uid")
		pPodUID       = types.UID("host-pod-uid")
		pNamespace    = testingutil.DefaultTestTargetNamespace
	)

	claimSpec := resourcev1.ResourceClaimSpec{
		Devices: resourcev1.DeviceClaim{
			Requests: []resourcev1.DeviceRequest{
				{
					Name: "rdma",
					Exactly: &resourcev1.ExactDeviceRequest{
						DeviceClassName: "roce.networking.k8s.aws",
					},
				},
			},
		},
	}

	vPod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      vPodName,
			Namespace: vNamespace,
			UID:       vPodUID,
		},
		Spec: corev1.PodSpec{
			ResourceClaims: []corev1.PodResourceClaim{
				{
					Name:                      podClaimName,
					ResourceClaimTemplateName: ptr.To(vTemplateName),
				},
			},
		},
	}
	pPod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      pPodName,
			Namespace: pNamespace,
			UID:       pPodUID,
			Annotations: map[string]string{
				translate.NameAnnotation:          vPodName,
				translate.NamespaceAnnotation:     vNamespace,
				translate.KindAnnotation:          corev1.SchemeGroupVersion.WithKind("Pod").String(),
				translate.HostNameAnnotation:      pPodName,
				translate.HostNamespaceAnnotation: pNamespace,
			},
			Labels: map[string]string{
				translate.MarkerLabel:    translate.VClusterName,
				translate.NamespaceLabel: vNamespace,
			},
		},
		Spec: corev1.PodSpec{
			ResourceClaims: []corev1.PodResourceClaim{
				{
					Name:                      podClaimName,
					ResourceClaimTemplateName: ptr.To(pTemplateName),
				},
			},
		},
		Status: corev1.PodStatus{
			ResourceClaimStatuses: []corev1.PodResourceClaimStatus{
				{
					Name:              podClaimName,
					ResourceClaimName: ptr.To(pClaimName),
				},
			},
		},
	}
	pClaim := &resourcev1.ResourceClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:      pClaimName,
			Namespace: pNamespace,
			Annotations: map[string]string{
				resourcev1.PodResourceClaimAnnotation: podClaimName,
			},
			OwnerReferences: []metav1.OwnerReference{
				{
					APIVersion: corev1.SchemeGroupVersion.String(),
					Kind:       "Pod",
					Name:       pPodName,
					UID:        pPodUID,
				},
			},
		},
		Spec: claimSpec,
		Status: resourcev1.ResourceClaimStatus{
			Allocation: &resourcev1.AllocationResult{
				Devices: resourcev1.DeviceAllocationResult{
					Results: []resourcev1.DeviceRequestAllocationResult{
						{
							Request: "rdma",
							Driver:  "roce.networking.k8s.aws",
							Pool:    "node-a",
							Device:  "mlx5_0",
						},
					},
				},
			},
			ReservedFor: []resourcev1.ResourceClaimConsumerReference{
				{
					Resource: "pods",
					Name:     pPodName,
					UID:      pPodUID,
				},
			},
		},
	}

	statusSubresourceSeed := &resourcev1.ResourceClaim{ObjectMeta: metav1.ObjectMeta{Name: "status-subresource-seed", Namespace: vNamespace}}
	pClient := testingutil.NewFakeClient(scheme.Scheme, pPod.DeepCopy(), pClaim.DeepCopy())
	vClient := testingutil.NewFakeClient(scheme.Scheme, vPod.DeepCopy(), statusSubresourceSeed.DeepCopy())
	vConfig := testingutil.NewFakeConfig()
	vConfig.Sync.ToHost.ResourceClaims.Enabled = true
	registerCtx := syncertesting.NewFakeRegisterContext(vConfig, pClient, vClient)
	syncCtx, object := syncertesting.FakeStartSyncer(t, registerCtx, New)
	s := object.(*resourceClaimSyncer)
	assert.NilError(t, vClient.Delete(syncCtx, statusSubresourceSeed))

	imported, err := s.Import(syncCtx, pClaim.DeepCopy())
	assert.NilError(t, err)
	assert.Equal(t, imported, true)

	vClaimName, ok := registerCtx.Mappings.Store().HostToVirtualName(syncCtx, synccontext.Object{
		GroupVersionKind: mappings.ResourceClaims(),
		NamespacedName:   types.NamespacedName{Namespace: pNamespace, Name: pClaimName},
	})
	assert.Equal(t, ok, true)
	assert.Equal(t, vClaimName.Namespace, vNamespace)
	assert.Equal(t, vClaimName.Name, pClaimName)

	_, err = s.SyncToVirtual(syncCtx, synccontext.NewSyncToVirtualEvent(pClaim.DeepCopy()))
	assert.NilError(t, err)

	vClaim := &resourcev1.ResourceClaim{}
	err = vClient.Get(syncCtx, types.NamespacedName{Namespace: vNamespace, Name: pClaimName}, vClaim)
	assert.NilError(t, err)
	assert.Equal(t, vClaim.Labels[GeneratedResourceClaimLabel], "true")
	assert.Equal(t, vClaim.Annotations[resourcev1.PodResourceClaimAnnotation], podClaimName)
	assert.Equal(t, vClaim.Spec.Devices.Requests[0].Exactly.DeviceClassName, "roce.networking.k8s.aws")
	assert.Equal(t, vClaim.Status.Allocation.Devices.Results[0].Device, "mlx5_0")
	assert.Equal(t, len(vClaim.Status.ReservedFor), 1)
	assert.Equal(t, vClaim.Status.ReservedFor[0].Name, vPodName)
	assert.Equal(t, vClaim.Status.ReservedFor[0].UID, vPodUID)
}

func TestGeneratedResourceClaimImportRequiresMappedTemplatePod(t *testing.T) {
	translate.Default = translate.NewSingleNamespaceTranslator(testingutil.DefaultTestTargetNamespace)

	pClaim := &resourcev1.ResourceClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "unrelated-claim",
			Namespace: testingutil.DefaultTestTargetNamespace,
			Annotations: map[string]string{
				resourcev1.PodResourceClaimAnnotation: "roce",
			},
			OwnerReferences: []metav1.OwnerReference{
				{
					APIVersion: corev1.SchemeGroupVersion.String(),
					Kind:       "Pod",
					Name:       "unmapped-pod",
					UID:        types.UID("host-pod-uid"),
				},
			},
		},
		Status: resourcev1.ResourceClaimStatus{
			ReservedFor: []resourcev1.ResourceClaimConsumerReference{
				{
					Resource: "pods",
					Name:     "unmapped-pod",
					UID:      types.UID("host-pod-uid"),
				},
			},
		},
	}
	pPod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "unmapped-pod",
			Namespace: testingutil.DefaultTestTargetNamespace,
			UID:       types.UID("host-pod-uid"),
		},
		Spec: corev1.PodSpec{
			ResourceClaims: []corev1.PodResourceClaim{
				{
					Name:                      "roce",
					ResourceClaimTemplateName: ptr.To("host-roce-template"),
				},
			},
		},
	}

	pClient := testingutil.NewFakeClient(scheme.Scheme, pPod, pClaim.DeepCopy())
	vClient := testingutil.NewFakeClient(scheme.Scheme)
	vConfig := testingutil.NewFakeConfig()
	vConfig.Sync.ToHost.ResourceClaims.Enabled = true
	registerCtx := syncertesting.NewFakeRegisterContext(vConfig, pClient, vClient)
	syncCtx, object := syncertesting.FakeStartSyncer(t, registerCtx, New)

	imported, err := object.(*resourceClaimSyncer).Import(syncCtx, pClaim.DeepCopy())
	assert.NilError(t, err)
	assert.Equal(t, imported, false)
}

func TestGeneratedResourceClaimMirrorDeletesOnlyVirtualWhenHostIsMissing(t *testing.T) {
	translate.Default = translate.NewSingleNamespaceTranslator(testingutil.DefaultTestTargetNamespace)

	vClaim := &resourcev1.ResourceClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "generated-claim",
			Namespace: "tenant",
			Labels: map[string]string{
				GeneratedResourceClaimLabel: "true",
			},
		},
	}

	pClient := testingutil.NewFakeClient(scheme.Scheme)
	vClient := testingutil.NewFakeClient(scheme.Scheme, vClaim.DeepCopy())
	vConfig := testingutil.NewFakeConfig()
	vConfig.Sync.ToHost.ResourceClaims.Enabled = true
	registerCtx := syncertesting.NewFakeRegisterContext(vConfig, pClient, vClient)
	syncCtx, object := syncertesting.FakeStartSyncer(t, registerCtx, New)

	_, err := object.(*resourceClaimSyncer).SyncToHost(syncCtx, synccontext.NewSyncToHostEvent(vClaim.DeepCopy()))
	assert.NilError(t, err)

	found := &resourcev1.ResourceClaim{}
	err = vClient.Get(syncCtx, types.NamespacedName{Namespace: vClaim.Namespace, Name: vClaim.Name}, found)
	assert.Equal(t, kerrors.IsNotFound(err), true)

	hostClaims := &resourcev1.ResourceClaimList{}
	err = pClient.List(syncCtx, hostClaims)
	assert.NilError(t, err)
	assert.Equal(t, len(hostClaims.Items), 0)
}

func TestGeneratedResourceClaimMirrorVirtualDeletionDoesNotDeleteHost(t *testing.T) {
	translate.Default = translate.NewSingleNamespaceTranslator(testingutil.DefaultTestTargetNamespace)

	deletionTime := metav1.Now()
	pClaim := &resourcev1.ResourceClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "generated-claim",
			Namespace: testingutil.DefaultTestTargetNamespace,
			Annotations: map[string]string{
				resourcev1.PodResourceClaimAnnotation: "roce",
			},
		},
	}
	vClaim := &resourcev1.ResourceClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:              pClaim.Name,
			Namespace:         "tenant",
			DeletionTimestamp: &deletionTime,
			Labels: map[string]string{
				GeneratedResourceClaimLabel: "true",
			},
		},
	}

	pClient := testingutil.NewFakeClient(scheme.Scheme, pClaim.DeepCopy())
	vClient := testingutil.NewFakeClient(scheme.Scheme)
	vConfig := testingutil.NewFakeConfig()
	vConfig.Sync.ToHost.ResourceClaims.Enabled = true
	registerCtx := syncertesting.NewFakeRegisterContext(vConfig, pClient, vClient)
	syncCtx, object := syncertesting.FakeStartSyncer(t, registerCtx, New)

	_, err := object.(*resourceClaimSyncer).Sync(syncCtx, synccontext.NewSyncEvent(pClaim.DeepCopy(), vClaim.DeepCopy()))
	assert.NilError(t, err)

	found := &resourcev1.ResourceClaim{}
	err = pClient.Get(syncCtx, types.NamespacedName{Namespace: pClaim.Namespace, Name: pClaim.Name}, found)
	assert.NilError(t, err)
}
