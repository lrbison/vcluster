package nvidia

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

const (
	GroupName = "resource.nvidia.com"
	Version   = "v1beta1"
)

var SchemeGroupVersion = schema.GroupVersion{Group: GroupName, Version: Version}

func ComputeDomainGVK() schema.GroupVersionKind {
	return SchemeGroupVersion.WithKind("ComputeDomain")
}

func ComputeDomainListGVK() schema.GroupVersionKind {
	return SchemeGroupVersion.WithKind("ComputeDomainList")
}

func ComputeDomainCliqueGVK() schema.GroupVersionKind {
	return SchemeGroupVersion.WithKind("ComputeDomainClique")
}

func ComputeDomainCliqueListGVK() schema.GroupVersionKind {
	return SchemeGroupVersion.WithKind("ComputeDomainCliqueList")
}

func NewComputeDomain() *unstructured.Unstructured {
	obj := &unstructured.Unstructured{}
	obj.SetGroupVersionKind(ComputeDomainGVK())
	return obj
}

func NewComputeDomainList() *unstructured.UnstructuredList {
	list := &unstructured.UnstructuredList{}
	list.SetGroupVersionKind(ComputeDomainListGVK())
	return list
}

func NewComputeDomainClique() *unstructured.Unstructured {
	obj := &unstructured.Unstructured{}
	obj.SetGroupVersionKind(ComputeDomainCliqueGVK())
	return obj
}

func NewComputeDomainCliqueList() *unstructured.UnstructuredList {
	list := &unstructured.UnstructuredList{}
	list.SetGroupVersionKind(ComputeDomainCliqueListGVK())
	return list
}

func AddToScheme(scheme *runtime.Scheme) error {
	scheme.AddKnownTypeWithName(ComputeDomainGVK(), &unstructured.Unstructured{})
	scheme.AddKnownTypeWithName(ComputeDomainListGVK(), &unstructured.UnstructuredList{})
	scheme.AddKnownTypeWithName(ComputeDomainCliqueGVK(), &unstructured.Unstructured{})
	scheme.AddKnownTypeWithName(ComputeDomainCliqueListGVK(), &unstructured.UnstructuredList{})
	metav1.AddToGroupVersion(scheme, SchemeGroupVersion)
	return nil
}
