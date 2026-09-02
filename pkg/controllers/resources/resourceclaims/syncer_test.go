package resourceclaims

import (
	"testing"

	rootconfig "github.com/loft-sh/vcluster/config"
	"github.com/loft-sh/vcluster/pkg/config"
	"github.com/loft-sh/vcluster/pkg/syncer/synccontext"
	syncertesting "github.com/loft-sh/vcluster/pkg/syncer/testing"
	testingutil "github.com/loft-sh/vcluster/pkg/util/testing"
	"github.com/loft-sh/vcluster/pkg/util/translate"
	"gotest.tools/assert"
	resourcev1 "k8s.io/api/resource/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
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
