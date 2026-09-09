package resourceclaims

import (
	"fmt"

	"github.com/loft-sh/vcluster/pkg/mappings"
	"github.com/loft-sh/vcluster/pkg/patcher"
	"github.com/loft-sh/vcluster/pkg/pro"
	"github.com/loft-sh/vcluster/pkg/syncer/synccontext"
	syncertypes "github.com/loft-sh/vcluster/pkg/syncer/types"
	"github.com/loft-sh/vcluster/pkg/util/clienthelper"
	"github.com/loft-sh/vcluster/pkg/util/translate"
	corev1 "k8s.io/api/core/v1"
	resourcev1 "k8s.io/api/resource/v1"
	kerrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	utilerrors "k8s.io/apimachinery/pkg/util/errors"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

const (
	GeneratedResourceClaimLabel = "vcluster.loft.sh/generated-resource-claim"
	managedByAnnotation         = "vcluster.loft.sh/managed-by"
)

type generatedResourceClaimImporter struct {
	delegate syncertypes.Importer
}

func newGeneratedResourceClaimImporter(delegate syncertypes.Importer) syncertypes.Importer {
	return &generatedResourceClaimImporter{
		delegate: delegate,
	}
}

func (i *generatedResourceClaimImporter) Import(ctx *synccontext.SyncContext, pObj client.Object) (bool, error) {
	imported, err := i.importGeneratedResourceClaim(ctx, pObj)
	if imported || err != nil {
		return imported, err
	}
	if i.delegate != nil {
		return i.delegate.Import(ctx, pObj)
	}
	return false, nil
}

func (i *generatedResourceClaimImporter) IgnoreHostObject(ctx *synccontext.SyncContext, pObj client.Object) bool {
	if clienthelper.IsNilObject(pObj) {
		return false
	}
	if i.delegate != nil {
		return i.delegate.IgnoreHostObject(ctx, pObj)
	}
	return false
}

func (i *generatedResourceClaimImporter) importGeneratedResourceClaim(ctx *synccontext.SyncContext, pObj client.Object) (bool, error) {
	claim, ok := pObj.(*resourcev1.ResourceClaim)
	if !ok || !isHostGeneratedResourceClaim(claim) || ctx == nil || ctx.Mappings == nil || ctx.Mappings.Store() == nil {
		return false, nil
	}

	podClaimName := claim.Annotations[resourcev1.PodResourceClaimAnnotation]
	podMapping, ok, err := generatedResourceClaimPodMapping(ctx, claim, podClaimName)
	if err != nil || !ok {
		return false, err
	}

	claimMapping := synccontext.NameMapping{
		GroupVersionKind: mappings.ResourceClaims(),
		HostName:         types.NamespacedName{Namespace: claim.Namespace, Name: claim.Name},
		VirtualName:      types.NamespacedName{Namespace: podMapping.VirtualName.Namespace, Name: claim.Name},
	}
	if err := ctx.Mappings.Store().AddReferenceAndSave(ctx, claimMapping, podMapping); err != nil {
		return false, fmt.Errorf("record generated ResourceClaim mapping %s -> %s: %w", claimMapping.HostName.String(), claimMapping.VirtualName.String(), err)
	}

	return true, nil
}

func generatedResourceClaimPodMapping(ctx *synccontext.SyncContext, claim *resourcev1.ResourceClaim, podClaimName string) (synccontext.NameMapping, bool, error) {
	for _, candidate := range generatedResourceClaimPodCandidates(claim) {
		pPod := &corev1.Pod{}
		err := ctx.HostClient.Get(ctx, types.NamespacedName{Namespace: claim.Namespace, Name: candidate.name}, pPod)
		if kerrors.IsNotFound(err) {
			continue
		} else if err != nil {
			return synccontext.NameMapping{}, false, fmt.Errorf("get host pod %s/%s for generated ResourceClaim %s/%s: %w", claim.Namespace, candidate.name, claim.Namespace, claim.Name, err)
		}
		if candidate.uid != "" && pPod.UID != "" && candidate.uid != pPod.UID {
			continue
		}
		if !hostPodUsesGeneratedResourceClaim(pPod, podClaimName, claim.Name) {
			continue
		}

		vPodName := mappings.HostToVirtual(ctx, pPod.Name, pPod.Namespace, pPod, mappings.Pods())
		if vPodName.Name == "" {
			continue
		}

		vPod := &corev1.Pod{}
		err = ctx.VirtualClient.Get(ctx, vPodName, vPod)
		if kerrors.IsNotFound(err) {
			continue
		} else if err != nil {
			return synccontext.NameMapping{}, false, fmt.Errorf("get virtual pod %s/%s for generated ResourceClaim %s/%s: %w", vPodName.Namespace, vPodName.Name, claim.Namespace, claim.Name, err)
		}
		if !virtualPodUsesResourceClaimTemplate(vPod, podClaimName) {
			continue
		}

		return synccontext.NameMapping{
			GroupVersionKind: mappings.Pods(),
			HostName:         types.NamespacedName{Namespace: pPod.Namespace, Name: pPod.Name},
			VirtualName:      vPodName,
		}, true, nil
	}

	return synccontext.NameMapping{}, false, nil
}

type generatedResourceClaimPodCandidate struct {
	name string
	uid  types.UID
}

func generatedResourceClaimPodCandidates(claim *resourcev1.ResourceClaim) []generatedResourceClaimPodCandidate {
	var candidates []generatedResourceClaimPodCandidate
	seen := map[generatedResourceClaimPodCandidate]bool{}
	add := func(name string, uid types.UID) {
		if name == "" {
			return
		}
		candidate := generatedResourceClaimPodCandidate{name: name, uid: uid}
		if seen[candidate] {
			return
		}
		seen[candidate] = true
		candidates = append(candidates, candidate)
	}

	for _, ref := range claim.Status.ReservedFor {
		if ref.APIGroup == "" && ref.Resource == "pods" {
			add(ref.Name, ref.UID)
		}
	}
	for _, ref := range claim.OwnerReferences {
		if isPodOwnerReference(ref) {
			add(ref.Name, ref.UID)
		}
	}

	return candidates
}

func isPodOwnerReference(ref metav1.OwnerReference) bool {
	return ref.APIVersion == corev1.SchemeGroupVersion.String() && ref.Kind == "Pod"
}

func hostPodUsesGeneratedResourceClaim(pod *corev1.Pod, podClaimName, generatedClaimName string) bool {
	for _, claim := range pod.Spec.ResourceClaims {
		if claim.Name == podClaimName && claim.ResourceClaimTemplateName != nil {
			return true
		}
	}
	for _, status := range pod.Status.ResourceClaimStatuses {
		if status.Name == podClaimName && status.ResourceClaimName != nil && *status.ResourceClaimName == generatedClaimName {
			return true
		}
	}
	return false
}

func virtualPodUsesResourceClaimTemplate(pod *corev1.Pod, podClaimName string) bool {
	for _, claim := range pod.Spec.ResourceClaims {
		if claim.Name == podClaimName && claim.ResourceClaimTemplateName != nil {
			return true
		}
	}
	return false
}

func isHostGeneratedResourceClaim(claim *resourcev1.ResourceClaim) bool {
	return claim != nil && claim.Annotations[resourcev1.PodResourceClaimAnnotation] != ""
}

func isGeneratedResourceClaimMirror(claim *resourcev1.ResourceClaim) bool {
	return claim != nil && claim.Labels[GeneratedResourceClaimLabel] == "true"
}

func (s *resourceClaimSyncer) syncGeneratedResourceClaimMirror(ctx *synccontext.SyncContext, event *synccontext.SyncEvent[*resourcev1.ResourceClaim]) (_ ctrl.Result, retErr error) {
	if event.Host.DeletionTimestamp != nil {
		if event.Virtual.DeletionTimestamp == nil {
			return patcher.DeleteVirtualObject(ctx, event.Virtual, event.Host, "host generated ResourceClaim is being deleted")
		}
		return ctrl.Result{}, nil
	}
	if event.Virtual.DeletionTimestamp != nil {
		return ctrl.Result{}, nil
	}

	patch, err := patcher.NewSyncerPatcher(ctx, event.Host, event.Virtual, patcher.TranslatePatches(ctx.Config.Sync.ToHost.ResourceClaims.Patches, false), patcher.SkipHostPatch())
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
				"SyncResourceClaim",
				"Error syncing generated ResourceClaim mirror: %v",
				retErr,
			)
		}
	}()

	desired := virtualGeneratedResourceClaim(ctx, event.Host, types.NamespacedName{Namespace: event.Virtual.Namespace, Name: event.Virtual.Name})
	event.Virtual.Labels = desired.Labels
	event.Virtual.Annotations = desired.Annotations
	event.Virtual.Spec = desired.Spec
	event.Virtual.Status = desired.Status
	return ctrl.Result{}, nil
}

func (s *resourceClaimSyncer) syncGeneratedResourceClaimToVirtual(ctx *synccontext.SyncContext, event *synccontext.SyncToVirtualEvent[*resourcev1.ResourceClaim]) (ctrl.Result, error) {
	if event.Host.DeletionTimestamp != nil {
		if event.VirtualOld != nil {
			return patcher.DeleteVirtualObject(ctx, event.VirtualOld, event.Host, "host generated ResourceClaim is being deleted")
		}
		return ctrl.Result{}, nil
	}

	vName := generatedResourceClaimVirtualName(ctx, event.Host)
	if vName.Name == "" {
		return ctrl.Result{}, nil
	}
	vObj := virtualGeneratedResourceClaim(ctx, event.Host, vName)

	err := pro.ApplyPatchesVirtualObject(ctx, nil, vObj, event.Host, ctx.Config.Sync.ToHost.ResourceClaims.Patches, false)
	if err != nil {
		return ctrl.Result{}, err
	}

	return patcher.CreateVirtualObject(ctx, event.Host, vObj, s.EventRecorder(), true)
}

func generatedResourceClaimVirtualName(ctx *synccontext.SyncContext, host *resourcev1.ResourceClaim) types.NamespacedName {
	if ctx == nil || ctx.Mappings == nil || ctx.Mappings.Store() == nil || host == nil {
		return types.NamespacedName{}
	}

	vName, ok := ctx.Mappings.Store().HostToVirtualName(ctx, synccontext.Object{
		GroupVersionKind: mappings.ResourceClaims(),
		NamespacedName:   types.NamespacedName{Namespace: host.Namespace, Name: host.Name},
	})
	if !ok {
		return types.NamespacedName{}
	}
	return vName
}

func virtualGeneratedResourceClaim(ctx *synccontext.SyncContext, host *resourcev1.ResourceClaim, vName types.NamespacedName) *resourcev1.ResourceClaim {
	vObj := translate.VirtualMetadata(host, vName)
	vObj.Spec = *host.Spec.DeepCopy()
	vObj.Status = generatedResourceClaimStatusToVirtual(ctx, host.Status, host.Namespace)

	if vObj.Labels == nil {
		vObj.Labels = map[string]string{}
	}
	vObj.Labels[GeneratedResourceClaimLabel] = "true"

	if vObj.Annotations == nil {
		vObj.Annotations = map[string]string{}
	}
	vObj.Annotations[managedByAnnotation] = "vcluster"

	return vObj
}

func generatedResourceClaimStatusToVirtual(ctx *synccontext.SyncContext, status resourcev1.ResourceClaimStatus, hostNamespace string) resourcev1.ResourceClaimStatus {
	ret := *status.DeepCopy()
	translateReservedForToVirtualFromStore(ctx, &ret, hostNamespace)
	return ret
}

func translateReservedForToVirtualFromStore(ctx *synccontext.SyncContext, status *resourcev1.ResourceClaimStatus, hostNamespace string) {
	if ctx == nil || ctx.Mappings == nil || ctx.Mappings.Store() == nil {
		return
	}

	for i := range status.ReservedFor {
		ref := &status.ReservedFor[i]
		if ref.APIGroup != "" || ref.Resource != "pods" {
			continue
		}

		vName, ok := ctx.Mappings.Store().HostToVirtualName(ctx, synccontext.Object{
			GroupVersionKind: mappings.Pods(),
			NamespacedName:   types.NamespacedName{Namespace: hostNamespace, Name: ref.Name},
		})
		if !ok || vName.Name == "" {
			continue
		}
		ref.Name = vName.Name

		vPod := &corev1.Pod{}
		err := ctx.VirtualClient.Get(ctx, vName, vPod)
		if err != nil {
			if !kerrors.IsNotFound(err) {
				ctx.Log.Infof("error looking up virtual pod %s/%s for generated resource claim reservedFor: %v", vName.Namespace, vName.Name, err)
			}
			continue
		}
		if vPod.UID != "" {
			ref.UID = vPod.UID
		}
	}
}
