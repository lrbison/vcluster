package resourceclaimtemplates

import (
	"fmt"

	resourcev1 "k8s.io/api/resource/v1"
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
	mapper, err := ctx.Mappings.ByGVK(mappings.ResourceClaimTemplates())
	if err != nil {
		return nil, err
	}

	return &resourceClaimTemplateSyncer{
		GenericTranslator: translator.NewGenericTranslator(ctx, "resourceclaimtemplate", &resourcev1.ResourceClaimTemplate{}, mapper),
	}, nil
}

type resourceClaimTemplateSyncer struct {
	syncertypes.GenericTranslator
}

var _ syncertypes.OptionsProvider = &resourceClaimTemplateSyncer{}

func (s *resourceClaimTemplateSyncer) Options() *syncertypes.Options {
	return &syncertypes.Options{
		ObjectCaching: true,
	}
}

var _ syncertypes.Syncer = &resourceClaimTemplateSyncer{}

func (s *resourceClaimTemplateSyncer) Syncer() syncertypes.Sync[client.Object] {
	return syncer.ToGenericSyncer(s)
}

func (s *resourceClaimTemplateSyncer) SyncToHost(ctx *synccontext.SyncContext, event *synccontext.SyncToHostEvent[*resourcev1.ResourceClaimTemplate]) (ctrl.Result, error) {
	if s.applyLimitByClass(ctx, event.Virtual) {
		return ctrl.Result{}, nil
	}

	if event.HostOld != nil || event.Virtual.DeletionTimestamp != nil {
		return patcher.DeleteVirtualObject(ctx, event.Virtual, event.HostOld, "host object was deleted")
	}

	pObj := s.translate(ctx, event.Virtual)
	err := pro.ApplyPatchesHostObject(ctx, nil, pObj, event.Virtual, ctx.Config.Sync.ToHost.ResourceClaimTemplates.Patches, false)
	if err != nil {
		return ctrl.Result{}, fmt.Errorf("apply patches: %w", err)
	}

	return patcher.CreateHostObject(ctx, event.Virtual, pObj, s.EventRecorder(), false)
}

func (s *resourceClaimTemplateSyncer) Sync(ctx *synccontext.SyncContext, event *synccontext.SyncEvent[*resourcev1.ResourceClaimTemplate]) (_ ctrl.Result, retErr error) {
	if s.applyLimitByClass(ctx, event.Virtual) {
		return ctrl.Result{}, nil
	}

	patch, err := patcher.NewSyncerPatcher(ctx, event.Host, event.Virtual, patcher.TranslatePatches(ctx.Config.Sync.ToHost.ResourceClaimTemplates.Patches, false))
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

	// Spec is immutable. Keep host metadata in sync with the tenant object.
	event.Virtual.Annotations, event.Host.Annotations = translate.AnnotationsBidirectionalUpdate(event)
	event.Virtual.Labels, event.Host.Labels = translate.LabelsBidirectionalUpdate(event)

	return ctrl.Result{}, nil
}

func (s *resourceClaimTemplateSyncer) SyncToVirtual(ctx *synccontext.SyncContext, event *synccontext.SyncToVirtualEvent[*resourcev1.ResourceClaimTemplate]) (_ ctrl.Result, retErr error) {
	return patcher.DeleteHostObject(ctx, event.Host, event.VirtualOld, "virtual object was deleted")
}
