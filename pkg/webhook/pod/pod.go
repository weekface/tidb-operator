package pod

import (
	"encoding/json"
	"path/filepath"

	"k8s.io/api/admission/v1beta1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/klog"
)

const extraVolumeAnnKey = "extra-volume"

var ExtraVolumeHostPath string

type SidecarConfig struct {
	VolumeMounts []corev1.VolumeMount `yaml:"volumeMounts"`
	Volumes      []corev1.Volume      `yaml:"volumes"`
}

type patchOperation struct {
	Op    string      `json:"op"`
	Path  string      `json:"path"`
	Value interface{} `json:"value,omitempty"`
}

func AdmitPods(ar v1beta1.AdmissionReview) *v1beta1.AdmissionResponse {
	req := ar.Request
	var pod corev1.Pod
	if err := json.Unmarshal(req.Object.Raw, &pod); err != nil {
		klog.Errorf("Could not unmarshal raw object: %v", err)
		return toAdmissionResponseErr(err)
	}

	ns := pod.Namespace
	name := pod.Name

	klog.Infof("AdmissionReview for Kind=%v, Namespace=%v Name=%v UID=%v patchOperation=%v UserInfo=%v",
		req.Kind, req.Namespace, pod.Name, req.UID, req.Operation, req.UserInfo)

	if pod.Annotations[extraVolumeAnnKey] != "true" {
		klog.Info("Skipping mutation")
		return &v1beta1.AdmissionResponse{
			Allowed: true,
		}
	}

	patchBytes, err := createPatch(&pod, getVolCfg(ns, name))
	if err != nil {
		klog.Errorf("Could not create patch: %v", err)
		return toAdmissionResponseErr(err)
	}

	return &v1beta1.AdmissionResponse{
		Allowed: true,
		Patch:   patchBytes,
		PatchType: func() *v1beta1.PatchType {
			pt := v1beta1.PatchTypeJSONPatch
			return &pt
		}(),
	}
}

func toAdmissionResponseErr(err error) *v1beta1.AdmissionResponse {
	return &v1beta1.AdmissionResponse{
		Result: &metav1.Status{
			Message: err.Error(),
		},
	}
}

func createPatch(pod *corev1.Pod, cfg *SidecarConfig) ([]byte, error) {
	var patch []patchOperation

	// add volume mounts
	patch = append(patch, addVolumeMount(pod.Spec.Containers, cfg.VolumeMounts, "/spec/containers")...)
	// add volumes
	patch = append(patch, addVolume(pod.Spec.Volumes, cfg.Volumes, "/spec/volumes")...)

	return json.Marshal(patch)
}

func getVolCfg(ns, name string) *SidecarConfig {
	return &SidecarConfig{
		VolumeMounts: []corev1.VolumeMount{
			{
				Name:      "extra-volume",
				MountPath: "/extra-volume",
			},
		},
		Volumes: []corev1.Volume{
			{
				Name: "extra-volume",
				VolumeSource: corev1.VolumeSource{
					HostPath: &corev1.HostPathVolumeSource{
						Path: filepath.Join(ExtraVolumeHostPath, "extra-volume", ns, name),
					},
				},
			},
		},
	}
}

func addVolumeMount(target []corev1.Container, added []corev1.VolumeMount, basePath string) (patch []patchOperation) {
	for i, c := range target {
		target[i].VolumeMounts = append(c.VolumeMounts, added...)
	}

	return append(patch, patchOperation{
		Op:    "replace",
		Path:  basePath,
		Value: target,
	})
}

func addVolume(target, added []corev1.Volume, basePath string) (patch []patchOperation) {
	first := len(target) == 0
	var value interface{}
	for _, add := range added {
		value = add
		path := basePath
		if first {
			first = false
			value = []corev1.Volume{add}
		} else {
			path += "/-"
		}
		patch = append(patch, patchOperation{
			Op:    "add",
			Path:  path,
			Value: value,
		})
	}
	return patch
}
