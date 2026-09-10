package resources

import (
	"fmt"

	nvidiaapis "github.com/loft-sh/vcluster/pkg/apis/nvidia"
	"github.com/loft-sh/vcluster/pkg/mappings"
	"github.com/loft-sh/vcluster/pkg/mappings/generic"
	"github.com/loft-sh/vcluster/pkg/syncer/synccontext"
	"github.com/loft-sh/vcluster/pkg/util"
	"github.com/loft-sh/vcluster/pkg/util/translate"
	kerrors "k8s.io/apimachinery/pkg/api/errors"
)

func CreateComputeDomainsMapper(ctx *synccontext.RegisterContext) (synccontext.Mapper, error) {
	gvk := mappings.ComputeDomains()
	apiResourceExistsOnHost, err := util.KindExists(ctx.HostManager.GetConfig(), gvk)
	if err != nil {
		return nil, fmt.Errorf("can't retrieve %v on host cluster: %w", gvk.String(), err)
	}
	if !apiResourceExistsOnHost {
		return nil, fmt.Errorf("%v not found on host cluster", gvk.String())
	}

	apiResourceExistsOnVirtual, err := util.KindExists(ctx.VirtualManager.GetConfig(), gvk)
	if err != nil {
		return nil, fmt.Errorf("can't retrieve %v on virtual cluster: %w", gvk.String(), err)
	}
	if !apiResourceExistsOnVirtual {
		_, _, err = translate.EnsureCRDFromPhysicalCluster(ctx.Context, ctx.HostManager.GetConfig(), ctx.VirtualManager.GetConfig(), gvk)
		if err != nil && !kerrors.IsAlreadyExists(err) {
			return nil, fmt.Errorf("ensure %v in virtual cluster: %w", gvk.String(), err)
		}
	}

	return generic.NewMapper(ctx, nvidiaapis.NewComputeDomain(), translate.Default.HostNameShort)
}
