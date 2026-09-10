package computedomains

import (
	"fmt"

	nvidiaapis "github.com/loft-sh/vcluster/pkg/apis/nvidia"
	"github.com/loft-sh/vcluster/pkg/mappings"
	"github.com/loft-sh/vcluster/pkg/patcher"
	"github.com/loft-sh/vcluster/pkg/pro"
	"github.com/loft-sh/vcluster/pkg/syncer"
	"github.com/loft-sh/vcluster/pkg/syncer/synccontext"
	"github.com/loft-sh/vcluster/pkg/syncer/translator"
	syncertypes "github.com/loft-sh/vcluster/pkg/syncer/types"
	"github.com/loft-sh/vcluster/pkg/util/translate"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/types"
	utilerrors "k8s.io/apimachinery/pkg/util/errors"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

func New(ctx *synccontext.RegisterContext) (syncertypes.Object, error) {
	mapper, err := ctx.Mappings.ByGVK(mappings.ComputeDomains())
	if err != nil {
		return nil, err
	}

	return &computeDomainSyncer{
		GenericTranslator: translator.NewGenericTranslator(ctx, "computedomain", nvidiaapis.NewComputeDomain(), mapper),
	}, nil
}

type computeDomainSyncer struct {
	syncertypes.GenericTranslator
}

var _ syncertypes.OptionsProvider = &computeDomainSyncer{}

func (s *computeDomainSyncer) Options() *syncertypes.Options {
	return &syncertypes.Options{
		ObjectCaching: true,
	}
}

var _ syncertypes.Syncer = &computeDomainSyncer{}

func (s *computeDomainSyncer) Syncer() syncertypes.Sync[client.Object] {
	return syncer.ToGenericSyncer(s)
}

func (s *computeDomainSyncer) SyncToHost(ctx *synccontext.SyncContext, event *synccontext.SyncToHostEvent[*unstructured.Unstructured]) (ctrl.Result, error) {
	if event.HostOld != nil || event.Virtual.GetDeletionTimestamp() != nil {
		return patcher.DeleteVirtualObject(ctx, event.Virtual, event.HostOld, "host object was deleted")
	}

	pObj, err := s.translate(ctx, event.Virtual)
	if err != nil {
		return ctrl.Result{}, err
	}
	err = pro.ApplyPatchesHostObject(ctx, nil, pObj, event.Virtual, ctx.Config.Sync.ToHost.ComputeDomains.Patches, false)
	if err != nil {
		return ctrl.Result{}, fmt.Errorf("apply patches: %w", err)
	}

	return patcher.CreateHostObject(ctx, event.Virtual, pObj, s.EventRecorder(), true)
}

func (s *computeDomainSyncer) Sync(ctx *synccontext.SyncContext, event *synccontext.SyncEvent[*unstructured.Unstructured]) (_ ctrl.Result, retErr error) {
	if event.Host.GetDeletionTimestamp() != nil {
		if event.Virtual.GetDeletionTimestamp() == nil {
			return patcher.DeleteVirtualObject(ctx, event.Virtual, event.Host, "host ComputeDomain is being deleted")
		}
		return ctrl.Result{}, nil
	} else if event.Virtual.GetDeletionTimestamp() != nil {
		return patcher.DeleteHostObject(ctx, event.Host, event.Virtual, "virtual ComputeDomain is being deleted")
	}

	patch, err := patcher.NewSyncerPatcher(ctx, event.Host, event.Virtual, patcher.TranslatePatches(ctx.Config.Sync.ToHost.ComputeDomains.Patches, false))
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
				"SyncComputeDomain",
				"Error syncing: %v",
				retErr,
			)
		}
	}()

	desiredHost, err := s.translate(ctx, event.Virtual)
	if err != nil {
		return ctrl.Result{}, err
	}
	event.Host.Object["spec"] = desiredHost.Object["spec"]
	virtualAnnotations, hostAnnotations := translate.AnnotationsBidirectionalUpdate(event)
	event.Virtual.SetAnnotations(virtualAnnotations)
	event.Host.SetAnnotations(hostAnnotations)
	virtualLabels, hostLabels := translate.LabelsBidirectionalUpdate(event)
	event.Virtual.SetLabels(virtualLabels)
	event.Host.SetLabels(hostLabels)
	copyStatus(event.Virtual, event.Host)

	return ctrl.Result{}, nil
}

func (s *computeDomainSyncer) SyncToVirtual(ctx *synccontext.SyncContext, event *synccontext.SyncToVirtualEvent[*unstructured.Unstructured]) (_ ctrl.Result, retErr error) {
	if event.VirtualOld != nil || translate.ShouldDeleteHostObject(event.Host) {
		return patcher.DeleteHostObject(ctx, event.Host, event.VirtualOld, "virtual object was deleted")
	}

	return ctrl.Result{}, nil
}

func (s *computeDomainSyncer) translate(ctx *synccontext.SyncContext, vObj *unstructured.Unstructured) (*unstructured.Unstructured, error) {
	pObj := translate.HostMetadata(vObj, s.VirtualToHost(ctx, types.NamespacedName{Name: vObj.GetName(), Namespace: vObj.GetNamespace()}, vObj))
	delete(pObj.Object, "status")

	spec, found, err := unstructured.NestedMap(vObj.Object, "spec")
	if err != nil {
		return nil, fmt.Errorf("copy ComputeDomain spec: %w", err)
	}
	if !found {
		delete(pObj.Object, "spec")
		return pObj, nil
	}

	templateName, found, err := unstructured.NestedString(spec, "channel", "resourceClaimTemplate", "name")
	if err != nil {
		return nil, fmt.Errorf("read ComputeDomain channel template name: %w", err)
	}
	if found && templateName != "" && ctx.Mappings.Has(mappings.ResourceClaimTemplates()) {
		unstructured.SetNestedField(
			spec,
			mappings.VirtualToHostName(ctx, templateName, vObj.GetNamespace(), mappings.ResourceClaimTemplates()),
			"channel",
			"resourceClaimTemplate",
			"name",
		)
	}

	if err := unstructured.SetNestedMap(pObj.Object, spec, "spec"); err != nil {
		return nil, fmt.Errorf("set ComputeDomain spec: %w", err)
	}
	return pObj, nil
}

func copyStatus(vObj, pObj *unstructured.Unstructured) {
	status, found, err := unstructured.NestedFieldCopy(pObj.Object, "status")
	if err != nil || !found {
		delete(vObj.Object, "status")
		return
	}
	unstructured.SetNestedField(vObj.Object, status, "status")
}
