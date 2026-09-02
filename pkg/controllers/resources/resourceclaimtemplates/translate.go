package resourceclaimtemplates

import (
	"fmt"

	resourcev1 "k8s.io/api/resource/v1"
	"k8s.io/apimachinery/pkg/types"

	"github.com/loft-sh/vcluster/pkg/syncer/synccontext"
	"github.com/loft-sh/vcluster/pkg/util/translate"
)

func (s *resourceClaimTemplateSyncer) translate(ctx *synccontext.SyncContext, vObj *resourcev1.ResourceClaimTemplate) *resourcev1.ResourceClaimTemplate {
	pObj := translate.HostMetadata(vObj, s.VirtualToHost(ctx, types.NamespacedName{Name: vObj.Name, Namespace: vObj.Namespace}, vObj))
	pObj.Spec = *vObj.Spec.DeepCopy()
	return pObj
}

func deviceClassNames(spec resourcev1.ResourceClaimSpec) []string {
	names := make([]string, 0, len(spec.Devices.Requests))
	for _, req := range spec.Devices.Requests {
		if req.Exactly != nil && req.Exactly.DeviceClassName != "" {
			names = append(names, req.Exactly.DeviceClassName)
		}
		for _, sub := range req.FirstAvailable {
			if sub.DeviceClassName != "" {
				names = append(names, sub.DeviceClassName)
			}
		}
	}
	return names
}

func (s *resourceClaimTemplateSyncer) applyLimitByClass(ctx *synccontext.SyncContext, virtual *resourcev1.ResourceClaimTemplate) bool {
	if !ctx.Config.Sync.FromHost.DeviceClasses.Enabled || ctx.Config.Sync.FromHost.DeviceClasses.Selector.Empty() {
		return false
	}

	for _, className := range deviceClassNames(virtual.Spec.Spec) {
		if className == "" {
			continue
		}

		pDeviceClass := &resourcev1.DeviceClass{}
		err := ctx.HostClient.Get(ctx, types.NamespacedName{Name: className}, pDeviceClass)
		if err != nil || pDeviceClass.GetDeletionTimestamp() != nil {
			s.EventRecorder().Eventf(
				virtual,
				nil,
				"Warning",
				"SyncWarning",
				fmt.Sprintf("Sync%s", virtual.GetObjectKind().GroupVersionKind().Kind),
				"did not sync resourceclaimtemplate %q to host. Failures : Device class %s couldn't be reached in the host: %s",
				virtual.GetName(),
				className,
				err,
			)
			return true
		}

		matches, err := ctx.Config.Sync.FromHost.DeviceClasses.Selector.Matches(pDeviceClass)
		if err != nil {
			s.EventRecorder().Eventf(
				virtual,
				nil,
				"Warning",
				"SyncWarning",
				fmt.Sprintf("Sync%s", virtual.GetObjectKind().GroupVersionKind().Kind),
				"did not sync resourceclaimtemplate %q to host. Failures : Device class %s in the host could not be checked against the selector under 'sync.fromHost.deviceClasses.selector': %s",
				virtual.GetName(),
				pDeviceClass.GetName(),
				err,
			)
			return true
		}
		if !matches {
			s.EventRecorder().Eventf(
				virtual,
				nil,
				"Warning",
				"SyncWarning",
				fmt.Sprintf("Sync%s", virtual.GetObjectKind().GroupVersionKind().Kind),
				"did not sync resourceclaimtemplate %q to host. Failures : Device class %s does not match the selector under 'sync.fromHost.deviceClasses.selector'",
				virtual.GetName(),
				pDeviceClass.GetName(),
			)
			return true
		}
	}

	return false
}
