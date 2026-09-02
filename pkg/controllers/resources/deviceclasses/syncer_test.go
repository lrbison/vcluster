package deviceclasses

import (
	"testing"

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
	"k8s.io/utils/ptr"
)

func TestSync(t *testing.T) {
	translate.Default = translate.NewSingleNamespaceTranslator(testingutil.DefaultTestTargetNamespace)
	vObjectMeta := metav1.ObjectMeta{
		Name: "gpu.nvidia.com",
		Annotations: map[string]string{
			translate.NameAnnotation: "gpu.nvidia.com",
			translate.UIDAnnotation:  "",
			translate.KindAnnotation: resourcev1.SchemeGroupVersion.WithKind("DeviceClass").String(),
		},
		ResourceVersion: "999",
	}

	vObj := &resourcev1.DeviceClass{
		ObjectMeta: vObjectMeta,
		Spec: resourcev1.DeviceClassSpec{
			ExtendedResourceName: ptr.To("nvidia.com/gpu"),
		},
	}

	pObj := &resourcev1.DeviceClass{
		ObjectMeta: metav1.ObjectMeta{
			Name: vObjectMeta.Name,
			Labels: map[string]string{
				translate.MarkerLabel: translate.VClusterName,
			},
			Annotations: map[string]string{
				translate.NameAnnotation: "gpu.nvidia.com",
				translate.UIDAnnotation:  "",
				translate.KindAnnotation: resourcev1.SchemeGroupVersion.WithKind("DeviceClass").String(),
			},
		},
		Spec: resourcev1.DeviceClassSpec{
			ExtendedResourceName: ptr.To("nvidia.com/gpu"),
		},
	}

	vObjUpdated := &resourcev1.DeviceClass{
		ObjectMeta: vObjectMeta,
		Spec: resourcev1.DeviceClassSpec{
			ExtendedResourceName: ptr.To("nvidia.com/gpu"),
			Selectors: []resourcev1.DeviceSelector{
				{CEL: &resourcev1.CELDeviceSelector{Expression: `device.driver == "gpu.nvidia.com"`}},
			},
		},
	}

	pObjUpdated := &resourcev1.DeviceClass{
		ObjectMeta: metav1.ObjectMeta{
			Name: vObjectMeta.Name,
			Labels: map[string]string{
				translate.MarkerLabel: translate.VClusterName,
			},
			Annotations: map[string]string{
				translate.NameAnnotation: "gpu.nvidia.com",
				translate.UIDAnnotation:  "",
				translate.KindAnnotation: resourcev1.SchemeGroupVersion.WithKind("DeviceClass").String(),
			},
		},
		Spec: vObjUpdated.Spec,
	}

	syncertesting.RunTestsWithContext(t, func(vConfig *config.VirtualClusterConfig, pClient *testingutil.FakeIndexClient, vClient *testingutil.FakeIndexClient) *synccontext.RegisterContext {
		vConfig.Sync.FromHost.DeviceClasses.Enabled = true
		return syncertesting.NewFakeRegisterContext(vConfig, pClient, vClient)
	}, []*syncertesting.SyncTest{
		{
			Name:                 "Import",
			InitialVirtualState:  []runtime.Object{},
			InitialPhysicalState: []runtime.Object{pObj},
			ExpectedVirtualState: map[schema.GroupVersionKind][]runtime.Object{
				resourcev1.SchemeGroupVersion.WithKind("DeviceClass"): {vObj},
			},
			ExpectedPhysicalState: map[schema.GroupVersionKind][]runtime.Object{
				resourcev1.SchemeGroupVersion.WithKind("DeviceClass"): {pObj},
			},
			Sync: func(ctx *synccontext.RegisterContext) {
				syncCtx, syncer := syncertesting.FakeStartSyncer(t, ctx, New)
				_, err := syncer.(*deviceClassSyncer).SyncToVirtual(syncCtx, synccontext.NewSyncToVirtualEvent(pObj))
				assert.NilError(t, err)
			},
		},
		{
			Name:                  "Delete virtual",
			InitialVirtualState:   []runtime.Object{vObj},
			ExpectedVirtualState:  map[schema.GroupVersionKind][]runtime.Object{},
			ExpectedPhysicalState: map[schema.GroupVersionKind][]runtime.Object{},
			Sync: func(ctx *synccontext.RegisterContext) {
				syncCtx, syncer := syncertesting.FakeStartSyncer(t, ctx, New)
				_, err := syncer.(*deviceClassSyncer).SyncToHost(syncCtx, synccontext.NewSyncToHostEvent(vObj))
				assert.NilError(t, err)
			},
		},
		{
			Name:                 "Sync",
			InitialVirtualState:  []runtime.Object{vObj},
			InitialPhysicalState: []runtime.Object{pObjUpdated},
			ExpectedVirtualState: map[schema.GroupVersionKind][]runtime.Object{
				resourcev1.SchemeGroupVersion.WithKind("DeviceClass"): {vObjUpdated},
			},
			ExpectedPhysicalState: map[schema.GroupVersionKind][]runtime.Object{
				resourcev1.SchemeGroupVersion.WithKind("DeviceClass"): {pObjUpdated},
			},
			Sync: func(ctx *synccontext.RegisterContext) {
				syncCtx, syncer := syncertesting.FakeStartSyncer(t, ctx, New)
				_, err := syncer.(*deviceClassSyncer).Sync(syncCtx, synccontext.NewSyncEvent(pObjUpdated, vObj))
				assert.NilError(t, err)
			},
		},
	})
}
