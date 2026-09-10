package computedomains

import (
	"testing"

	nvidiaapis "github.com/loft-sh/vcluster/pkg/apis/nvidia"
	"github.com/loft-sh/vcluster/pkg/mappings"
	"github.com/loft-sh/vcluster/pkg/scheme"
	"github.com/loft-sh/vcluster/pkg/syncer/synccontext"
	syncertesting "github.com/loft-sh/vcluster/pkg/syncer/testing"
	testingutil "github.com/loft-sh/vcluster/pkg/util/testing"
	"github.com/loft-sh/vcluster/pkg/util/translate"
	"gotest.tools/assert"
	kerrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/types"
)

func TestComputeDomainSyncToHostTranslatesChannelTemplate(t *testing.T) {
	translate.Default = translate.NewSingleNamespaceTranslator(testingutil.DefaultTestTargetNamespace)

	const (
		vNamespace   = "tenant"
		cdName       = "test-compute-domain"
		templateName = "test-compute-domain-channel"
	)

	vComputeDomain := newComputeDomain(cdName, vNamespace, templateName)
	pClient := testingutil.NewFakeClient(scheme.Scheme)
	vClient := testingutil.NewFakeClient(scheme.Scheme, vComputeDomain.DeepCopy())
	registerCtx := newComputeDomainRegisterContext(pClient, vClient)
	syncCtx, object := syncertesting.FakeStartSyncer(t, registerCtx, New)

	_, err := object.(*computeDomainSyncer).SyncToHost(syncCtx, synccontext.NewSyncToHostEvent(vComputeDomain.DeepCopy()))
	assert.NilError(t, err)

	hostName := mappings.VirtualToHost(syncCtx, cdName, vNamespace, mappings.ComputeDomains())
	hostTemplateName := mappings.VirtualToHostName(syncCtx, templateName, vNamespace, mappings.ResourceClaimTemplates())
	hostComputeDomain := nvidiaapis.NewComputeDomain()
	err = pClient.Get(syncCtx, hostName, hostComputeDomain)
	assert.NilError(t, err)

	actualTemplateName, found, err := unstructured.NestedString(hostComputeDomain.Object, "spec", "channel", "resourceClaimTemplate", "name")
	assert.NilError(t, err)
	assert.Assert(t, found)
	assert.Equal(t, actualTemplateName, hostTemplateName)
	assert.Equal(t, hostComputeDomain.GetAnnotations()[translate.NameAnnotation], cdName)
	assert.Equal(t, hostComputeDomain.GetAnnotations()[translate.NamespaceAnnotation], vNamespace)
}

func TestComputeDomainStatusSyncsBackToVirtual(t *testing.T) {
	translate.Default = translate.NewSingleNamespaceTranslator(testingutil.DefaultTestTargetNamespace)

	const (
		vNamespace   = "tenant"
		cdName       = "test-compute-domain"
		templateName = "test-compute-domain-channel"
	)

	vComputeDomain := newComputeDomain(cdName, vNamespace, templateName)
	pClient := testingutil.NewFakeClient(scheme.Scheme)
	vClient := testingutil.NewFakeClient(scheme.Scheme, vComputeDomain.DeepCopy())
	registerCtx := newComputeDomainRegisterContext(pClient, vClient)
	syncCtx, object := syncertesting.FakeStartSyncer(t, registerCtx, New)
	s := object.(*computeDomainSyncer)
	hostComputeDomain, err := s.translate(syncCtx, vComputeDomain.DeepCopy())
	assert.NilError(t, err)
	err = unstructured.SetNestedMap(hostComputeDomain.Object, map[string]interface{}{"phase": "Ready"}, "status")
	assert.NilError(t, err)
	assert.NilError(t, pClient.Create(syncCtx, hostComputeDomain.DeepCopy()))

	_, err = s.Sync(syncCtx, synccontext.NewSyncEventWithOld(
		hostComputeDomain.DeepCopy(),
		hostComputeDomain.DeepCopy(),
		vComputeDomain.DeepCopy(),
		vComputeDomain.DeepCopy(),
	))
	assert.NilError(t, err)

	updatedVirtual := nvidiaapis.NewComputeDomain()
	err = vClient.Get(syncCtx, types.NamespacedName{Namespace: vNamespace, Name: cdName}, updatedVirtual)
	assert.NilError(t, err)
	phase, found, err := unstructured.NestedString(updatedVirtual.Object, "status", "phase")
	assert.NilError(t, err)
	assert.Assert(t, found)
	assert.Equal(t, phase, "Ready")
}

func TestComputeDomainDeletesHostWhenVirtualIsGone(t *testing.T) {
	translate.Default = translate.NewSingleNamespaceTranslator(testingutil.DefaultTestTargetNamespace)

	const (
		vNamespace   = "tenant"
		cdName       = "test-compute-domain"
		templateName = "test-compute-domain-channel"
	)

	vComputeDomain := newComputeDomain(cdName, vNamespace, templateName)
	pClient := testingutil.NewFakeClient(scheme.Scheme)
	vClient := testingutil.NewFakeClient(scheme.Scheme, vComputeDomain.DeepCopy())
	registerCtx := newComputeDomainRegisterContext(pClient, vClient)
	syncCtx, object := syncertesting.FakeStartSyncer(t, registerCtx, New)
	s := object.(*computeDomainSyncer)
	hostComputeDomain, err := s.translate(syncCtx, vComputeDomain.DeepCopy())
	assert.NilError(t, err)
	assert.NilError(t, pClient.Create(syncCtx, hostComputeDomain.DeepCopy()))

	_, err = s.SyncToVirtual(syncCtx, synccontext.NewSyncToVirtualEvent(hostComputeDomain.DeepCopy()))
	assert.NilError(t, err)

	foundHost := nvidiaapis.NewComputeDomain()
	err = pClient.Get(syncCtx, types.NamespacedName{Namespace: hostComputeDomain.GetNamespace(), Name: hostComputeDomain.GetName()}, foundHost)
	assert.Equal(t, kerrors.IsNotFound(err), true)
}

func newComputeDomainRegisterContext(pClient, vClient *testingutil.FakeIndexClient) *synccontext.RegisterContext {
	vConfig := testingutil.NewFakeConfig()
	vConfig.Sync.ToHost.ComputeDomains.Enabled = true
	vConfig.Sync.ToHost.ResourceClaims.Enabled = true
	vConfig.Sync.ToHost.ResourceClaimTemplates.Enabled = true
	return syncertesting.NewFakeRegisterContext(vConfig, pClient, vClient)
}

func newComputeDomain(name, namespace, templateName string) *unstructured.Unstructured {
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
