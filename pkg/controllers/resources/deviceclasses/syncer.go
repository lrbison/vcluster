package deviceclasses

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
	mapper, err := ctx.Mappings.ByGVK(mappings.DeviceClasses())
	if err != nil {
		return nil, err
	}

	return &deviceClassSyncer{
		GenericTranslator: translator.NewGenericTranslator(ctx, "deviceclass", &resourcev1.DeviceClass{}, mapper),
	}, nil
}

type deviceClassSyncer struct {
	syncertypes.GenericTranslator
}

func (s *deviceClassSyncer) Name() string {
	return "deviceclass"
}

func (s *deviceClassSyncer) Resource() client.Object {
	return &resourcev1.DeviceClass{}
}

var _ syncertypes.Syncer = &deviceClassSyncer{}

func (s *deviceClassSyncer) Syncer() syncertypes.Sync[client.Object] {
	return syncer.ToGenericSyncer(s)
}

func (s *deviceClassSyncer) SyncToVirtual(ctx *synccontext.SyncContext, event *synccontext.SyncToVirtualEvent[*resourcev1.DeviceClass]) (ctrl.Result, error) {
	matches, err := ctx.Config.Sync.FromHost.DeviceClasses.Selector.Matches(event.Host)
	if err != nil {
		return ctrl.Result{}, fmt.Errorf("check device class selector: %w", err)
	}
	if !matches {
		ctx.Log.Infof("Warning: did not sync device class %q because it does not match the selector under 'sync.fromHost.deviceClasses.selector'", event.Host.Name)
		return ctrl.Result{}, nil
	}

	vObj := translate.CopyObjectWithName(event.Host, types.NamespacedName{Name: event.Host.Name, Namespace: event.Host.Namespace}, false)

	err = pro.ApplyPatchesVirtualObject(ctx, nil, vObj, event.Host, ctx.Config.Sync.FromHost.DeviceClasses.Patches, true)
	if err != nil {
		return ctrl.Result{}, fmt.Errorf("error applying patches: %w", err)
	}

	ctx.Log.Infof("create device class %s, because it does not exist in virtual cluster", vObj.Name)
	return ctrl.Result{}, ctx.VirtualClient.Create(ctx, vObj)
}

func (s *deviceClassSyncer) Sync(ctx *synccontext.SyncContext, event *synccontext.SyncEvent[*resourcev1.DeviceClass]) (_ ctrl.Result, retErr error) {
	matches, err := ctx.Config.Sync.FromHost.DeviceClasses.Selector.Matches(event.Host)
	if err != nil {
		return ctrl.Result{}, fmt.Errorf("check device class selector: %w", err)
	}
	if !matches {
		return patcher.DeleteVirtualObject(ctx, event.Virtual, event.Host, fmt.Sprintf("did not sync device class %q because it does not match the selector under 'sync.fromHost.deviceClasses.selector'", event.Host.Name))
	}

	patch, err := patcher.NewSyncerPatcher(ctx, event.Host, event.Virtual, patcher.TranslatePatches(ctx.Config.Sync.FromHost.DeviceClasses.Patches, true))
	if err != nil {
		return ctrl.Result{}, fmt.Errorf("new syncer patcher: %w", err)
	}
	defer func() {
		if err := patch.Patch(ctx, event.Host, event.Virtual); err != nil {
			retErr = utilerrors.NewAggregate([]error{retErr, err})
		}
	}()

	event.Virtual.Annotations = translate.VirtualAnnotations(event.Host, event.Virtual)
	event.Virtual.Labels = translate.VirtualLabels(event.Host, event.Virtual)
	event.Virtual.Spec = *event.Host.Spec.DeepCopy()
	return ctrl.Result{}, nil
}

func (s *deviceClassSyncer) SyncToHost(ctx *synccontext.SyncContext, event *synccontext.SyncToHostEvent[*resourcev1.DeviceClass]) (ctrl.Result, error) {
	ctx.Log.Infof("delete virtual device class %s, because physical object is missing", event.Virtual.Name)
	return ctrl.Result{}, ctx.VirtualClient.Delete(ctx, event.Virtual)
}
