package resourceclaimtemplates

import (
	"testing"

	rootconfig "github.com/loft-sh/vcluster/config"
	nvidiaapis "github.com/loft-sh/vcluster/pkg/apis/nvidia"
	"github.com/loft-sh/vcluster/pkg/config"
	"github.com/loft-sh/vcluster/pkg/mappings"
	"github.com/loft-sh/vcluster/pkg/scheme"
	"github.com/loft-sh/vcluster/pkg/syncer/synccontext"
	syncertesting "github.com/loft-sh/vcluster/pkg/syncer/testing"
	testingutil "github.com/loft-sh/vcluster/pkg/util/testing"
	"github.com/loft-sh/vcluster/pkg/util/translate"
	"gotest.tools/assert"
	resourcev1 "k8s.io/api/resource/v1"
	kerrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
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

func TestComputeDomainChannelTemplateImportAndBackSync(t *testing.T) {
	translate.Default = translate.NewSingleNamespaceTranslator(testingutil.DefaultTestTargetNamespace)

	const (
		vNamespace    = "tenant"
		cdName        = "test-compute-domain"
		vTemplateName = "test-compute-domain-channel"
	)

	vComputeDomain := newTestComputeDomain(cdName, vNamespace, vTemplateName)
	pClient := testingutil.NewFakeClient(scheme.Scheme)
	vClient := testingutil.NewFakeClient(scheme.Scheme, vComputeDomain.DeepCopy())
	registerCtx := newTemplateComputeDomainRegisterContext(pClient, vClient)
	syncCtx := registerCtx.ToSyncContext("seed")
	pComputeDomain := newHostComputeDomainForTemplateTest(syncCtx, vComputeDomain)
	pTemplateName := hostComputeDomainChannelTemplateName(t, pComputeDomain)
	pTemplate := newHostChannelTemplate(pTemplateName, pComputeDomain)
	assert.NilError(t, pClient.Create(syncCtx, pComputeDomain.DeepCopy()))
	assert.NilError(t, pClient.Create(syncCtx, pTemplate.DeepCopy()))

	syncCtx, object := syncertesting.FakeStartSyncer(t, registerCtx, New)
	s := object.(*resourceClaimTemplateSyncer)

	imported, err := s.Import(syncCtx, pTemplate.DeepCopy())
	assert.NilError(t, err)
	assert.Equal(t, imported, true)

	hostName, ok := registerCtx.Mappings.Store().VirtualToHostName(syncCtx, synccontext.Object{
		GroupVersionKind: mappings.ResourceClaimTemplates(),
		NamespacedName:   types.NamespacedName{Namespace: vNamespace, Name: vTemplateName},
	})
	assert.Equal(t, ok, true)
	assert.Equal(t, hostName.Namespace, pTemplate.Namespace)
	assert.Equal(t, hostName.Name, pTemplate.Name)

	_, err = s.SyncToVirtual(syncCtx, synccontext.NewSyncToVirtualEvent(pTemplate.DeepCopy()))
	assert.NilError(t, err)

	vTemplate := &resourcev1.ResourceClaimTemplate{}
	err = vClient.Get(syncCtx, types.NamespacedName{Namespace: vNamespace, Name: vTemplateName}, vTemplate)
	assert.NilError(t, err)
	assert.Equal(t, vTemplate.Labels[GeneratedComputeDomainChannelTemplateLabel], "true")
	assert.Equal(t, vTemplate.Annotations[managedByAnnotation], "vcluster")
	assert.Equal(t, vTemplate.Spec.Spec.Devices.Requests[0].Name, "channel")
}

func TestComputeDomainChannelTemplateImportRejectsUnrelatedTemplates(t *testing.T) {
	translate.Default = translate.NewSingleNamespaceTranslator(testingutil.DefaultTestTargetNamespace)

	pTemplate := newHostChannelTemplate("unrelated-template", nil)
	pClient := testingutil.NewFakeClient(scheme.Scheme, pTemplate.DeepCopy())
	vClient := testingutil.NewFakeClient(scheme.Scheme)
	registerCtx := newTemplateComputeDomainRegisterContext(pClient, vClient)
	syncCtx, object := syncertesting.FakeStartSyncer(t, registerCtx, New)

	imported, err := object.(*resourceClaimTemplateSyncer).Import(syncCtx, pTemplate.DeepCopy())
	assert.NilError(t, err)
	assert.Equal(t, imported, false)
}

func TestComputeDomainChannelTemplateMirrorVirtualDeletionDoesNotDeleteHost(t *testing.T) {
	translate.Default = translate.NewSingleNamespaceTranslator(testingutil.DefaultTestTargetNamespace)

	const (
		vNamespace    = "tenant"
		cdName        = "test-compute-domain"
		vTemplateName = "test-compute-domain-channel"
	)

	vComputeDomain := newTestComputeDomain(cdName, vNamespace, vTemplateName)
	pClient := testingutil.NewFakeClient(scheme.Scheme)
	vClient := testingutil.NewFakeClient(scheme.Scheme, vComputeDomain.DeepCopy())
	registerCtx := newTemplateComputeDomainRegisterContext(pClient, vClient)
	syncCtx := registerCtx.ToSyncContext("seed")
	pComputeDomain := newHostComputeDomainForTemplateTest(syncCtx, vComputeDomain)
	pTemplate := newHostChannelTemplate(hostComputeDomainChannelTemplateName(t, pComputeDomain), pComputeDomain)
	assert.NilError(t, pClient.Create(syncCtx, pComputeDomain.DeepCopy()))
	assert.NilError(t, pClient.Create(syncCtx, pTemplate.DeepCopy()))

	deletionTime := metav1.Now()
	vTemplate := virtualGeneratedComputeDomainChannelTemplate(pTemplate.DeepCopy(), types.NamespacedName{Namespace: vNamespace, Name: vTemplateName})
	vTemplate.DeletionTimestamp = &deletionTime

	syncCtx, object := syncertesting.FakeStartSyncer(t, registerCtx, New)
	_, err := object.(*resourceClaimTemplateSyncer).Sync(syncCtx, synccontext.NewSyncEvent(pTemplate.DeepCopy(), vTemplate.DeepCopy()))
	assert.NilError(t, err)

	foundHost := &resourcev1.ResourceClaimTemplate{}
	err = pClient.Get(syncCtx, types.NamespacedName{Namespace: pTemplate.Namespace, Name: pTemplate.Name}, foundHost)
	assert.NilError(t, err)
}

func TestComputeDomainChannelTemplateMirrorDeletesOnlyVirtualWhenHostIsMissing(t *testing.T) {
	translate.Default = translate.NewSingleNamespaceTranslator(testingutil.DefaultTestTargetNamespace)

	vTemplate := &resourcev1.ResourceClaimTemplate{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-compute-domain-channel",
			Namespace: "tenant",
			Labels: map[string]string{
				GeneratedComputeDomainChannelTemplateLabel: "true",
			},
		},
	}

	pClient := testingutil.NewFakeClient(scheme.Scheme)
	vClient := testingutil.NewFakeClient(scheme.Scheme, vTemplate.DeepCopy())
	registerCtx := newTemplateComputeDomainRegisterContext(pClient, vClient)
	syncCtx, object := syncertesting.FakeStartSyncer(t, registerCtx, New)

	_, err := object.(*resourceClaimTemplateSyncer).SyncToHost(syncCtx, synccontext.NewSyncToHostEvent(vTemplate.DeepCopy()))
	assert.NilError(t, err)

	foundVirtual := &resourcev1.ResourceClaimTemplate{}
	err = vClient.Get(syncCtx, types.NamespacedName{Namespace: vTemplate.Namespace, Name: vTemplate.Name}, foundVirtual)
	assert.Equal(t, kerrors.IsNotFound(err), true)
}

func newTemplateComputeDomainRegisterContext(pClient, vClient *testingutil.FakeIndexClient) *synccontext.RegisterContext {
	vConfig := testingutil.NewFakeConfig()
	vConfig.Sync.ToHost.ComputeDomains.Enabled = true
	vConfig.Sync.ToHost.ResourceClaims.Enabled = true
	vConfig.Sync.ToHost.ResourceClaimTemplates.Enabled = true
	return syncertesting.NewFakeRegisterContext(vConfig, pClient, vClient)
}

func newTestComputeDomain(name, namespace, templateName string) *unstructured.Unstructured {
	obj := nvidiaapis.NewComputeDomain()
	obj.SetName(name)
	obj.SetNamespace(namespace)
	_ = unstructured.SetNestedMap(obj.Object, map[string]interface{}{
		"numNodes": int64(0),
		"channel": map[string]interface{}{
			"allocationMode": "Single",
			"resourceClaimTemplate": map[string]interface{}{
				"name": templateName,
			},
		},
	}, "spec")
	return obj
}

func newHostComputeDomainForTemplateTest(ctx *synccontext.SyncContext, vComputeDomain *unstructured.Unstructured) *unstructured.Unstructured {
	hostName := mappings.VirtualToHost(ctx, vComputeDomain.GetName(), vComputeDomain.GetNamespace(), mappings.ComputeDomains())
	pComputeDomain := translate.HostMetadata(vComputeDomain, hostName)
	spec, _, _ := unstructured.NestedMap(vComputeDomain.Object, "spec")
	_ = unstructured.SetNestedField(
		spec,
		mappings.VirtualToHostName(ctx, hostComputeDomainChannelTemplateNameFromVirtual(vComputeDomain), vComputeDomain.GetNamespace(), mappings.ResourceClaimTemplates()),
		"channel",
		"resourceClaimTemplate",
		"name",
	)
	_ = unstructured.SetNestedMap(pComputeDomain.Object, spec, "spec")
	return pComputeDomain
}

func hostComputeDomainChannelTemplateNameFromVirtual(computeDomain *unstructured.Unstructured) string {
	name, _, _ := unstructured.NestedString(computeDomain.Object, "spec", "channel", "resourceClaimTemplate", "name")
	return name
}

func hostComputeDomainChannelTemplateName(t *testing.T, computeDomain *unstructured.Unstructured) string {
	t.Helper()
	name, found, err := unstructured.NestedString(computeDomain.Object, "spec", "channel", "resourceClaimTemplate", "name")
	assert.NilError(t, err)
	assert.Assert(t, found)
	return name
}

func newHostChannelTemplate(name string, owner *unstructured.Unstructured) *resourcev1.ResourceClaimTemplate {
	template := &resourcev1.ResourceClaimTemplate{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: testingutil.DefaultTestTargetNamespace,
		},
		Spec: resourcev1.ResourceClaimTemplateSpec{
			Spec: resourcev1.ResourceClaimSpec{
				Devices: resourcev1.DeviceClaim{
					Requests: []resourcev1.DeviceRequest{
						{
							Name: "channel",
							Exactly: &resourcev1.ExactDeviceRequest{
								DeviceClassName: "imex.nvidia.com",
							},
						},
					},
				},
			},
		},
	}
	if owner != nil {
		template.OwnerReferences = []metav1.OwnerReference{
			{
				APIVersion: nvidiaapis.SchemeGroupVersion.String(),
				Kind:       "ComputeDomain",
				Name:       owner.GetName(),
				UID:        owner.GetUID(),
			},
		}
	}
	return template
}
