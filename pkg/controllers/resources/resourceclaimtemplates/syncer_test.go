package resourceclaimtemplates

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
)

func TestSync(t *testing.T) {
	translate.Default = translate.NewSingleNamespaceTranslator(testingutil.DefaultTestTargetNamespace)
	vObjectMeta := metav1.ObjectMeta{
		Name:            "gpu-template",
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
			translate.KindAnnotation:          resourcev1.SchemeGroupVersion.WithKind("ResourceClaimTemplate").String(),
			translate.HostNameAnnotation:      hostName.Name,
			translate.HostNamespaceAnnotation: "test",
		},
		Labels: map[string]string{
			translate.MarkerLabel:    translate.VClusterName,
			translate.NamespaceLabel: vObjectMeta.Namespace,
		},
	}

	templateSpec := resourcev1.ResourceClaimTemplateSpec{
		Spec: resourcev1.ResourceClaimSpec{
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
		},
	}

	vTemplate := &resourcev1.ResourceClaimTemplate{
		ObjectMeta: vObjectMeta,
		Spec:       templateSpec,
	}
	pTemplate := &resourcev1.ResourceClaimTemplate{
		ObjectMeta: pObjectMeta,
		Spec:       templateSpec,
	}

	vTemplateLabeled := vTemplate.DeepCopy()
	vTemplateLabeled.Labels = map[string]string{"team": "ml"}
	pTemplateLabeled := pTemplate.DeepCopy()
	pTemplateLabeled.Labels = map[string]string{
		"team":                   "ml",
		translate.MarkerLabel:    translate.VClusterName,
		translate.NamespaceLabel: vObjectMeta.Namespace,
	}

	hostDeviceClass := &resourcev1.DeviceClass{
		ObjectMeta: metav1.ObjectMeta{
			Name: "gpu.nvidia.com",
			Labels: map[string]string{
				"environment": "prod",
			},
		},
	}

	syncertesting.RunTestsWithContext(t, func(vConfig *config.VirtualClusterConfig, pClient *testingutil.FakeIndexClient, vClient *testingutil.FakeIndexClient) *synccontext.RegisterContext {
		vConfig.Sync.ToHost.ResourceClaimTemplates.Enabled = true
		return syncertesting.NewFakeRegisterContext(vConfig, pClient, vClient)
	}, []*syncertesting.SyncTest{
		{
			Name:                "Create forward",
			InitialVirtualState: []runtime.Object{vTemplate.DeepCopy()},
			ExpectedVirtualState: map[schema.GroupVersionKind][]runtime.Object{
				resourcev1.SchemeGroupVersion.WithKind("ResourceClaimTemplate"): {vTemplate.DeepCopy()},
			},
			ExpectedPhysicalState: map[schema.GroupVersionKind][]runtime.Object{
				resourcev1.SchemeGroupVersion.WithKind("ResourceClaimTemplate"): {pTemplate.DeepCopy()},
			},
			Sync: func(ctx *synccontext.RegisterContext) {
				syncCtx, syncer := syncertesting.FakeStartSyncer(t, ctx, New)
				_, err := syncer.(*resourceClaimTemplateSyncer).SyncToHost(syncCtx, synccontext.NewSyncToHostEvent(vTemplate.DeepCopy()))
				assert.NilError(t, err)
			},
		},
		{
			Name:                 "Update labels",
			InitialVirtualState:  []runtime.Object{vTemplateLabeled.DeepCopy()},
			InitialPhysicalState: []runtime.Object{pTemplate.DeepCopy()},
			ExpectedVirtualState: map[schema.GroupVersionKind][]runtime.Object{
				resourcev1.SchemeGroupVersion.WithKind("ResourceClaimTemplate"): {vTemplateLabeled.DeepCopy()},
			},
			ExpectedPhysicalState: map[schema.GroupVersionKind][]runtime.Object{
				resourcev1.SchemeGroupVersion.WithKind("ResourceClaimTemplate"): {pTemplateLabeled.DeepCopy()},
			},
			Sync: func(ctx *synccontext.RegisterContext) {
				syncCtx, syncer := syncertesting.FakeStartSyncer(t, ctx, New)
				_, err := syncer.(*resourceClaimTemplateSyncer).Sync(syncCtx, synccontext.NewSyncEventWithOld(
					pTemplate.DeepCopy(),
					pTemplate.DeepCopy(),
					vTemplate.DeepCopy(),
					vTemplateLabeled.DeepCopy(),
				))
				assert.NilError(t, err)
			},
		},
		{
			Name:                 "Delete host when virtual is gone",
			InitialPhysicalState: []runtime.Object{pTemplate.DeepCopy()},
			ExpectedVirtualState: map[schema.GroupVersionKind][]runtime.Object{},
			ExpectedPhysicalState: map[schema.GroupVersionKind][]runtime.Object{
				resourcev1.SchemeGroupVersion.WithKind("ResourceClaimTemplate"): {},
			},
			Sync: func(ctx *synccontext.RegisterContext) {
				syncCtx, syncer := syncertesting.FakeStartSyncer(t, ctx, New)
				_, err := syncer.(*resourceClaimTemplateSyncer).SyncToVirtual(syncCtx, synccontext.NewSyncToVirtualEvent(pTemplate.DeepCopy()))
				assert.NilError(t, err)
			},
		},
	})

	syncertesting.RunTestsWithContext(t, func(vConfig *config.VirtualClusterConfig, pClient *testingutil.FakeIndexClient, vClient *testingutil.FakeIndexClient) *synccontext.RegisterContext {
		vConfig.Sync.ToHost.ResourceClaimTemplates.Enabled = true
		vConfig.Sync.FromHost.DeviceClasses.Enabled = true
		vConfig.Sync.FromHost.DeviceClasses.Selector = rootconfig.StandardLabelSelector{
			MatchLabels: map[string]string{"environment": "dev"},
		}
		return syncertesting.NewFakeRegisterContext(vConfig, pClient, vClient)
	}, []*syncertesting.SyncTest{
		{
			Name:                "Reject template whose device class misses the selector",
			InitialVirtualState: []runtime.Object{vTemplate.DeepCopy()},
			InitialPhysicalState: []runtime.Object{
				hostDeviceClass.DeepCopy(),
			},
			ExpectedVirtualState: map[schema.GroupVersionKind][]runtime.Object{
				resourcev1.SchemeGroupVersion.WithKind("ResourceClaimTemplate"): {vTemplate.DeepCopy()},
			},
			ExpectedPhysicalState: map[schema.GroupVersionKind][]runtime.Object{
				resourcev1.SchemeGroupVersion.WithKind("DeviceClass"): {hostDeviceClass.DeepCopy()},
			},
			Sync: func(ctx *synccontext.RegisterContext) {
				syncCtx, syncer := syncertesting.FakeStartSyncer(t, ctx, New)
				_, err := syncer.(*resourceClaimTemplateSyncer).SyncToHost(syncCtx, synccontext.NewSyncToHostEvent(vTemplate.DeepCopy()))
				assert.NilError(t, err)
			},
		},
	})
}
