package resourceclaims

import (
	"fmt"

	resourcev1 "k8s.io/api/resource/v1"
	"k8s.io/apimachinery/pkg/types"
	utilerrors "k8s.io/apimachinery/pkg/util/errors"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/loft-sh/vcluster/pkg/mappings"
	"github.com/loft-sh/vcluster/pkg/patcher"
	"github.com/loft-sh/vcluster/pkg/pro"
	"github.com/loft-sh/vcluster/pkg/syncer"
	"github.com/loft-sh/vcluster/pkg/syncer/synccontext"
	"github.com/loft-sh/vcluster/pkg/syncer/translator"
	syncertypes "github.com/loft-sh/vcluster/pkg/syncer/types"
	"github.com/loft-sh/vcluster/pkg/util/translate"
)

func New(ctx *synccontext.RegisterContext) (syncertypes.Object, error) {
	mapper, err := ctx.Mappings.ByGVK(mappings.ResourceClaims())
	if err != nil {
		return nil, err
	}

	return &resourceClaimSyncer{
		GenericTranslator: translator.NewGenericTranslator(ctx, "resourceclaim", &resourcev1.ResourceClaim{}, mapper),
		Importer:          newGeneratedResourceClaimImporter(pro.NewImporter(mapper)),
	}, nil
}

type resourceClaimSyncer struct {
	syncertypes.GenericTranslator
	syncertypes.Importer
}

var _ syncertypes.OptionsProvider = &resourceClaimSyncer{}

func (s *resourceClaimSyncer) Options() *syncertypes.Options {
	return &syncertypes.Options{
		ObjectCaching: true,
	}
}

var _ syncertypes.Syncer = &resourceClaimSyncer{}

func (s *resourceClaimSyncer) Syncer() syncertypes.Sync[client.Object] {
	return syncer.ToGenericSyncer(s)
}

func (s *resourceClaimSyncer) SyncToHost(ctx *synccontext.SyncContext, event *synccontext.SyncToHostEvent[*resourcev1.ResourceClaim]) (ctrl.Result, error) {
	if isGeneratedResourceClaimMirror(event.Virtual) {
		reason := fmt.Sprintf("host ResourceClaim for generated mirror %s/%s is missing", event.Virtual.Namespace, event.Virtual.Name)
		s.EventRecorder().Eventf(event.Virtual, nil, "Warning", "SyncWarning", "SyncResourceClaim", "Deleting virtual ResourceClaim: %s", reason)
		return patcher.DeleteVirtualObject(ctx, event.Virtual, event.HostOld, reason)
	}

	if s.applyLimitByClass(ctx, event.Virtual) {
		return ctrl.Result{}, nil
	}

	if event.HostOld != nil || event.Virtual.DeletionTimestamp != nil {
		return patcher.DeleteVirtualObject(ctx, event.Virtual, event.HostOld, "host object was deleted")
	}

	pObj := s.translate(ctx, event.Virtual)
	err := pro.ApplyPatchesHostObject(ctx, nil, pObj, event.Virtual, ctx.Config.Sync.ToHost.ResourceClaims.Patches, false)
	if err != nil {
		return ctrl.Result{}, fmt.Errorf("apply patches: %w", err)
	}

	return patcher.CreateHostObject(ctx, event.Virtual, pObj, s.EventRecorder(), true)
}

func (s *resourceClaimSyncer) Sync(ctx *synccontext.SyncContext, event *synccontext.SyncEvent[*resourcev1.ResourceClaim]) (_ ctrl.Result, retErr error) {
	if isGeneratedResourceClaimMirror(event.Virtual) || isHostGeneratedResourceClaim(event.Host) {
		return s.syncGeneratedResourceClaimMirror(ctx, event)
	}

	if s.applyLimitByClass(ctx, event.Virtual) {
		return ctrl.Result{}, nil
	}

	if event.Host.DeletionTimestamp != nil {
		if event.Virtual.DeletionTimestamp == nil {
			return patcher.DeleteVirtualObject(ctx, event.Virtual, event.Host, "host resource claim is being deleted")
		}
		return ctrl.Result{}, nil
	} else if event.Virtual.DeletionTimestamp != nil {
		return patcher.DeleteHostObject(ctx, event.Host, event.Virtual, "virtual resource claim is being deleted")
	}

	patch, err := patcher.NewSyncerPatcher(ctx, event.Host, event.Virtual, patcher.TranslatePatches(ctx.Config.Sync.ToHost.ResourceClaims.Patches, false))
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
				fmt.Sprintf("Sync%s", event.Virtual.GetObjectKind().GroupVersionKind().Kind),
				"Error syncing: %v",
				retErr,
			)
		}
	}()

	// Spec is immutable. Host DRA owns allocation; copy status back to the tenant.
	s.translateStatus(ctx, event.Host, event.Virtual)

	event.Virtual.Annotations, event.Host.Annotations = translate.AnnotationsBidirectionalUpdate(event)
	event.Virtual.Labels, event.Host.Labels = translate.LabelsBidirectionalUpdate(event)

	return ctrl.Result{}, nil
}

func (s *resourceClaimSyncer) SyncToVirtual(ctx *synccontext.SyncContext, event *synccontext.SyncToVirtualEvent[*resourcev1.ResourceClaim]) (_ ctrl.Result, retErr error) {
	if isHostGeneratedResourceClaim(event.Host) {
		return s.syncGeneratedResourceClaimToVirtual(ctx, event)
	}

	if event.VirtualOld != nil || translate.ShouldDeleteHostObject(event.Host) {
		return patcher.DeleteHostObject(ctx, event.Host, event.VirtualOld, "virtual object was deleted")
	}

	vObj := translate.VirtualMetadata(event.Host, s.HostToVirtual(ctx, types.NamespacedName{Name: event.Host.Name, Namespace: event.Host.Namespace}, event.Host))
	vObj.Spec = *event.Host.Spec.DeepCopy()
	s.translateStatus(ctx, event.Host, vObj)

	err := pro.ApplyPatchesVirtualObject(ctx, nil, vObj, event.Host, ctx.Config.Sync.ToHost.ResourceClaims.Patches, false)
	if err != nil {
		return ctrl.Result{}, err
	}

	return patcher.CreateVirtualObject(ctx, event.Host, vObj, s.EventRecorder(), true)
}
