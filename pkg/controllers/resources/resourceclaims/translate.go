package resourceclaims

import (
	"fmt"

	corev1 "k8s.io/api/core/v1"
	resourcev1 "k8s.io/api/resource/v1"
	kerrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"

	"github.com/loft-sh/vcluster/pkg/mappings"
	"github.com/loft-sh/vcluster/pkg/syncer/synccontext"
	"github.com/loft-sh/vcluster/pkg/util/translate"
)

func (s *resourceClaimSyncer) translate(ctx *synccontext.SyncContext, vObj *resourcev1.ResourceClaim) *resourcev1.ResourceClaim {
	pObj := translate.HostMetadata(vObj, s.VirtualToHost(ctx, types.NamespacedName{Name: vObj.Name, Namespace: vObj.Namespace}, vObj))
	pObj.Spec = *vObj.Spec.DeepCopy()
	return pObj
}

func (s *resourceClaimSyncer) translateStatus(ctx *synccontext.SyncContext, pObj, vObj *resourcev1.ResourceClaim) {
	status := pObj.Status.DeepCopy()
	translateReservedForToVirtual(ctx, status, pObj.Namespace)
	vObj.Status = *status
}

func translateReservedForToVirtual(ctx *synccontext.SyncContext, status *resourcev1.ResourceClaimStatus, hostNamespace string) {
	for i := range status.ReservedFor {
		ref := &status.ReservedFor[i]
		if ref.APIGroup != "" || ref.Resource != "pods" {
			continue
		}

		vName := mappings.HostToVirtual(ctx, ref.Name, hostNamespace, nil, mappings.Pods())
		if vName.Name == "" {
			continue
		}
		ref.Name = vName.Name

		vPod := &corev1.Pod{}
		err := ctx.VirtualClient.Get(ctx, vName, vPod)
		if err != nil {
			if !kerrors.IsNotFound(err) {
				ctx.Log.Infof("error looking up virtual pod %s/%s for resource claim reservedFor: %v", vName.Namespace, vName.Name, err)
			}
			continue
		}
		if vPod.UID != "" {
			ref.UID = vPod.UID
		}
	}
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

func (s *resourceClaimSyncer) applyLimitByClass(ctx *synccontext.SyncContext, virtual *resourcev1.ResourceClaim) bool {
	if !ctx.Config.Sync.FromHost.DeviceClasses.Enabled || ctx.Config.Sync.FromHost.DeviceClasses.Selector.Empty() {
		return false
	}

	for _, className := range deviceClassNames(virtual.Spec) {
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
				"did not sync resourceclaim %q to host. Failures : Device class %s couldn't be reached in the host: %s",
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
				"did not sync resourceclaim %q to host. Failures : Device class %s in the host could not be checked against the selector under 'sync.fromHost.deviceClasses.selector': %s",
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
				"did not sync resourceclaim %q to host. Failures : Device class %s does not match the selector under 'sync.fromHost.deviceClasses.selector'",
				virtual.GetName(),
				pDeviceClass.GetName(),
			)
			return true
		}
	}

	return false
}
