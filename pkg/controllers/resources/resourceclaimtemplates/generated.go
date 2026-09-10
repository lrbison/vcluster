package resourceclaimtemplates

import (
	"fmt"

	nvidiaapis "github.com/loft-sh/vcluster/pkg/apis/nvidia"
	"github.com/loft-sh/vcluster/pkg/mappings"
	"github.com/loft-sh/vcluster/pkg/patcher"
	"github.com/loft-sh/vcluster/pkg/pro"
	"github.com/loft-sh/vcluster/pkg/syncer/synccontext"
	syncertypes "github.com/loft-sh/vcluster/pkg/syncer/types"
	"github.com/loft-sh/vcluster/pkg/util/clienthelper"
	"github.com/loft-sh/vcluster/pkg/util/translate"
	resourcev1 "k8s.io/api/resource/v1"
	kerrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/types"
	utilerrors "k8s.io/apimachinery/pkg/util/errors"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

const (
	GeneratedComputeDomainChannelTemplateLabel = "vcluster.loft.sh/generated-computedomain-channel-template"
	managedByAnnotation                        = "vcluster.loft.sh/managed-by"
)

type computeDomainChannelTemplateImporter struct {
	delegate syncertypes.Importer
}

func newComputeDomainChannelTemplateImporter(delegate syncertypes.Importer) syncertypes.Importer {
	return &computeDomainChannelTemplateImporter{
		delegate: delegate,
	}
}

func (i *computeDomainChannelTemplateImporter) Import(ctx *synccontext.SyncContext, pObj client.Object) (bool, error) {
	imported, err := i.importComputeDomainChannelTemplate(ctx, pObj)
	if imported || err != nil {
		return imported, err
	}
	if i.delegate != nil {
		return i.delegate.Import(ctx, pObj)
	}
	return false, nil
}

func (i *computeDomainChannelTemplateImporter) IgnoreHostObject(ctx *synccontext.SyncContext, pObj client.Object) bool {
	if clienthelper.IsNilObject(pObj) {
		return false
	}
	if i.delegate != nil {
		return i.delegate.IgnoreHostObject(ctx, pObj)
	}
	return false
}

func (i *computeDomainChannelTemplateImporter) importComputeDomainChannelTemplate(ctx *synccontext.SyncContext, pObj client.Object) (bool, error) {
	template, ok := pObj.(*resourcev1.ResourceClaimTemplate)
	if !ok || ctx == nil || ctx.Mappings == nil || ctx.Mappings.Store() == nil || !ctx.Config.Sync.ToHost.ComputeDomains.Enabled {
		return false, nil
	}

	vName, computeDomainMapping, ok, err := computeDomainChannelTemplateVirtualName(ctx, template)
	if err != nil || !ok {
		return false, err
	}

	templateMapping := synccontext.NameMapping{
		GroupVersionKind: mappings.ResourceClaimTemplates(),
		HostName:         types.NamespacedName{Namespace: template.Namespace, Name: template.Name},
		VirtualName:      vName,
	}
	if err := ctx.Mappings.Store().AddReferenceAndSave(ctx, computeDomainMapping, computeDomainMapping); err != nil {
		return false, fmt.Errorf("record ComputeDomain mapping %s -> %s: %w", computeDomainMapping.HostName.String(), computeDomainMapping.VirtualName.String(), err)
	}
	if err := ctx.Mappings.Store().AddReferenceAndSave(ctx, templateMapping, computeDomainMapping); err != nil {
		return false, fmt.Errorf("record generated ComputeDomain channel ResourceClaimTemplate mapping %s -> %s: %w", templateMapping.HostName.String(), templateMapping.VirtualName.String(), err)
	}

	return true, nil
}

func isGeneratedComputeDomainChannelTemplateMirror(template *resourcev1.ResourceClaimTemplate) bool {
	return template != nil && template.Labels[GeneratedComputeDomainChannelTemplateLabel] == "true"
}

func isHostGeneratedComputeDomainChannelTemplate(ctx *synccontext.SyncContext, template *resourcev1.ResourceClaimTemplate) (bool, error) {
	_, _, ok, err := computeDomainChannelTemplateVirtualName(ctx, template)
	return ok, err
}

func computeDomainChannelTemplateVirtualName(ctx *synccontext.SyncContext, template *resourcev1.ResourceClaimTemplate) (types.NamespacedName, synccontext.NameMapping, bool, error) {
	if ctx == nil || ctx.Mappings == nil || ctx.Mappings.Store() == nil || template == nil || !ctx.Config.Sync.ToHost.ComputeDomains.Enabled {
		return types.NamespacedName{}, synccontext.NameMapping{}, false, nil
	}

	hostComputeDomains, err := computeDomainCandidatesForTemplate(ctx, template)
	if err != nil {
		return types.NamespacedName{}, synccontext.NameMapping{}, false, err
	}
	for _, hostComputeDomain := range hostComputeDomains {
		templateName, found, err := computeDomainChannelTemplateName(hostComputeDomain)
		if err != nil {
			return types.NamespacedName{}, synccontext.NameMapping{}, false, err
		}
		if !found || templateName != template.Name {
			continue
		}

		hostComputeDomainName := types.NamespacedName{Namespace: hostComputeDomain.GetNamespace(), Name: hostComputeDomain.GetName()}
		virtualComputeDomainName := computeDomainVirtualName(ctx, hostComputeDomain)
		if virtualComputeDomainName.Name == "" || virtualComputeDomainName.Namespace == "" {
			continue
		}

		virtualComputeDomain := nvidiaapis.NewComputeDomain()
		err = ctx.VirtualClient.Get(ctx, virtualComputeDomainName, virtualComputeDomain)
		if kerrors.IsNotFound(err) {
			continue
		} else if err != nil {
			return types.NamespacedName{}, synccontext.NameMapping{}, false, fmt.Errorf("get virtual ComputeDomain %s for generated ResourceClaimTemplate %s/%s: %w", virtualComputeDomainName.String(), template.Namespace, template.Name, err)
		}

		virtualTemplateName, found, err := computeDomainChannelTemplateName(virtualComputeDomain)
		if err != nil {
			return types.NamespacedName{}, synccontext.NameMapping{}, false, err
		}
		if !found || virtualTemplateName == "" {
			continue
		}

		computeDomainMapping := synccontext.NameMapping{
			GroupVersionKind: mappings.ComputeDomains(),
			HostName:         hostComputeDomainName,
			VirtualName:      virtualComputeDomainName,
		}
		return types.NamespacedName{Namespace: virtualComputeDomainName.Namespace, Name: virtualTemplateName}, computeDomainMapping, true, nil
	}

	return types.NamespacedName{}, synccontext.NameMapping{}, false, nil
}

func computeDomainCandidatesForTemplate(ctx *synccontext.SyncContext, template *resourcev1.ResourceClaimTemplate) ([]*unstructured.Unstructured, error) {
	ret := []*unstructured.Unstructured{}
	seen := map[types.NamespacedName]bool{}
	add := func(obj *unstructured.Unstructured) {
		if obj == nil || obj.GetName() == "" {
			return
		}
		key := types.NamespacedName{Namespace: obj.GetNamespace(), Name: obj.GetName()}
		if seen[key] {
			return
		}
		seen[key] = true
		ret = append(ret, obj)
	}

	for _, owner := range template.OwnerReferences {
		if !isComputeDomainOwner(owner) {
			continue
		}
		hostComputeDomain := nvidiaapis.NewComputeDomain()
		err := ctx.HostClient.Get(ctx, types.NamespacedName{Namespace: template.Namespace, Name: owner.Name}, hostComputeDomain)
		if kerrors.IsNotFound(err) {
			continue
		} else if err != nil {
			return nil, fmt.Errorf("get owner ComputeDomain %s/%s for generated ResourceClaimTemplate %s/%s: %w", template.Namespace, owner.Name, template.Namespace, template.Name, err)
		} else {
			add(hostComputeDomain)
		}
	}

	hostComputeDomains := nvidiaapis.NewComputeDomainList()
	err := ctx.HostClient.List(ctx, hostComputeDomains, client.InNamespace(template.Namespace))
	if err != nil {
		return nil, fmt.Errorf("list host ComputeDomains in namespace %s for generated ResourceClaimTemplate %s/%s: %w", template.Namespace, template.Namespace, template.Name, err)
	}
	for i := range hostComputeDomains.Items {
		obj := &hostComputeDomains.Items[i]
		add(obj)
	}

	return ret, nil
}

func isComputeDomainOwner(owner metav1.OwnerReference) bool {
	return owner.APIVersion == nvidiaapis.SchemeGroupVersion.String() && owner.Kind == "ComputeDomain"
}

func computeDomainVirtualName(ctx *synccontext.SyncContext, hostComputeDomain *unstructured.Unstructured) types.NamespacedName {
	if hostComputeDomain == nil {
		return types.NamespacedName{}
	}

	hostName := types.NamespacedName{Namespace: hostComputeDomain.GetNamespace(), Name: hostComputeDomain.GetName()}
	vName, ok := ctx.Mappings.Store().HostToVirtualName(ctx, synccontext.Object{
		GroupVersionKind: mappings.ComputeDomains(),
		NamespacedName:   hostName,
	})
	if ok {
		return vName
	}

	mapper, err := ctx.Mappings.ByGVK(mappings.ComputeDomains())
	if err != nil {
		return types.NamespacedName{}
	}
	return mapper.HostToVirtual(ctx, hostName, hostComputeDomain)
}

func computeDomainChannelTemplateName(computeDomain *unstructured.Unstructured) (string, bool, error) {
	name, found, err := unstructured.NestedString(computeDomain.Object, "spec", "channel", "resourceClaimTemplate", "name")
	if err != nil {
		return "", false, fmt.Errorf("read ComputeDomain channel ResourceClaimTemplate name: %w", err)
	}
	return name, found, nil
}

func (s *resourceClaimTemplateSyncer) syncGeneratedComputeDomainChannelTemplateMirror(ctx *synccontext.SyncContext, event *synccontext.SyncEvent[*resourcev1.ResourceClaimTemplate]) (_ ctrl.Result, retErr error) {
	if event.Host.DeletionTimestamp != nil {
		if event.Virtual.DeletionTimestamp == nil {
			return patcher.DeleteVirtualObject(ctx, event.Virtual, event.Host, "host generated ComputeDomain channel ResourceClaimTemplate is being deleted")
		}
		return ctrl.Result{}, nil
	}
	if event.Virtual.DeletionTimestamp != nil {
		return ctrl.Result{}, nil
	}

	patch, err := patcher.NewSyncerPatcher(ctx, event.Host, event.Virtual, patcher.TranslatePatches(ctx.Config.Sync.ToHost.ResourceClaimTemplates.Patches, false), patcher.SkipHostPatch())
	if err != nil {
		return ctrl.Result{}, fmt.Errorf("new syncer patcher: %w", err)
	}
	defer func() {
		if err := patch.Patch(ctx, event.Host, event.Virtual); err != nil {
			retErr = utilerrors.NewAggregate([]error{retErr, err})
		}
		if retErr != nil {
			s.EventRecorder().Eventf(
				event.Virtual,
				nil,
				"Warning",
				"SyncError",
				"SyncResourceClaimTemplate",
				"Error syncing generated ComputeDomain channel ResourceClaimTemplate mirror: %v",
				retErr,
			)
		}
	}()

	desired := virtualGeneratedComputeDomainChannelTemplate(event.Host, types.NamespacedName{Namespace: event.Virtual.Namespace, Name: event.Virtual.Name})
	event.Virtual.Labels = desired.Labels
	event.Virtual.Annotations = desired.Annotations
	event.Virtual.Spec = desired.Spec
	return ctrl.Result{}, nil
}

func (s *resourceClaimTemplateSyncer) syncGeneratedComputeDomainChannelTemplateToVirtual(ctx *synccontext.SyncContext, event *synccontext.SyncToVirtualEvent[*resourcev1.ResourceClaimTemplate]) (ctrl.Result, error) {
	if event.Host.DeletionTimestamp != nil {
		if event.VirtualOld != nil {
			return patcher.DeleteVirtualObject(ctx, event.VirtualOld, event.Host, "host generated ComputeDomain channel ResourceClaimTemplate is being deleted")
		}
		return ctrl.Result{}, nil
	}

	vName, _, ok, err := computeDomainChannelTemplateVirtualName(ctx, event.Host)
	if err != nil || !ok {
		return ctrl.Result{}, err
	}
	vObj := virtualGeneratedComputeDomainChannelTemplate(event.Host, vName)

	err = pro.ApplyPatchesVirtualObject(ctx, nil, vObj, event.Host, ctx.Config.Sync.ToHost.ResourceClaimTemplates.Patches, false)
	if err != nil {
		return ctrl.Result{}, err
	}

	return patcher.CreateVirtualObject(ctx, event.Host, vObj, s.EventRecorder(), false)
}

func virtualGeneratedComputeDomainChannelTemplate(host *resourcev1.ResourceClaimTemplate, vName types.NamespacedName) *resourcev1.ResourceClaimTemplate {
	vObj := translate.VirtualMetadata(host, vName)
	vObj.Spec = *host.Spec.DeepCopy()

	if vObj.Labels == nil {
		vObj.Labels = map[string]string{}
	}
	vObj.Labels[GeneratedComputeDomainChannelTemplateLabel] = "true"

	if vObj.Annotations == nil {
		vObj.Annotations = map[string]string{}
	}
	vObj.Annotations[managedByAnnotation] = "vcluster"

	return vObj
}
