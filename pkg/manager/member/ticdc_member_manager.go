// Copyright 2020 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package member

import (
	"fmt"

	"github.com/pingcap/tidb-operator/pkg/apis/pingcap/v1alpha1"
	"github.com/pingcap/tidb-operator/pkg/controller"
	"github.com/pingcap/tidb-operator/pkg/label"
	"github.com/pingcap/tidb-operator/pkg/manager"
	"github.com/pingcap/tidb-operator/pkg/pdapi"
	apps "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	appslisters "k8s.io/client-go/listers/apps/v1"
	"k8s.io/klog"
)

// ticdcMemberManager implements manager.Manager.
type ticdcMemberManager struct {
	pdControl     pdapi.PDControlInterface
	typedControl  controller.TypedControlInterface
	deployLister  appslisters.DeploymentLister
	deployControl controller.DeploymentControlInterface
}

// NewTiCdcMemberManager returns a *ticdcMemberManager
func NewTiCdcMemberManager(
	pdControl pdapi.PDControlInterface,
	typedControl controller.TypedControlInterface,
	deployLister appslisters.DeploymentLister,
	deployControl controller.DeploymentControlInterface) manager.Manager {
	return &ticdcMemberManager{
		pdControl:     pdControl,
		typedControl:  typedControl,
		deployLister:  deployLister,
		deployControl: deployControl,
	}
}

// Sync fulfills the manager.Manager interface
func (tcmm *ticdcMemberManager) Sync(tc *v1alpha1.TidbCluster) error {
	ns := tc.GetNamespace()
	tcName := tc.GetName()

	if tc.Spec.TiCdc == nil {
		return nil
	}
	if tc.Spec.Paused {
		klog.Infof("cluster %s/%s is paused, skip syncing for ticdc deployment", ns, tcName)
		return nil
	}
	if !tc.PDIsAvailable() {
		return controller.RequeueErrorf("TidbCluster: [%s/%s], waiting for PD cluster running", ns, tcName)
	}

	return tcmm.syncDeployment(tc)
}

func (tcmm *ticdcMemberManager) syncDeployment(tc *v1alpha1.TidbCluster) error {
	ns := tc.GetNamespace()
	tcName := tc.GetName()

	oldDeployTmp, err := tcmm.deployLister.Deployments(ns).Get(controller.TiCdcMemberName(tcName))
	if err != nil && !errors.IsNotFound(err) {
		return err
	}

	deployNotExist := errors.IsNotFound(err)
	oldDeploy := oldDeployTmp.DeepCopy()

	if err := tcmm.syncTiCdcStatus(tc, oldDeploy); err != nil {
		return err
	}

	newDeploy, err := getNewDeployment(tc)
	if err != nil {
		return err
	}

	if deployNotExist {
		err = SetDeploymentLastAppliedConfigAnnotation(newDeploy)
		if err != nil {
			return err
		}
		err = tcmm.deployControl.CreateDeployment(tc, newDeploy)
		if err != nil {
			return err
		}
		return nil
	}

	// TODO: skip upgrade if pd or tikv is upgrading

	return updateDeployment(tcmm.deployControl, tc, newDeploy, oldDeploy)
}

// TODO add syncTiCdcStatus
func (tcmm *ticdcMemberManager) syncTiCdcStatus(tc *v1alpha1.TidbCluster, deploy *apps.Deployment) error {
	return nil
}

func getNewDeployment(tc *v1alpha1.TidbCluster) (*apps.Deployment, error) {
	ns := tc.GetNamespace()
	tcName := tc.GetName()

	baseTiCdcSpec := tc.BaseTiCdcSpec()
	ticdcLabel := labelTiCdc(tc)
	deployName := controller.TiCdcMemberName(tcName)
	podAnnotations := CombineAnnotations(controller.AnnProm(8301), baseTiCdcSpec.Annotations())
	deployAnnotations := getStsAnnotations(tc, label.TiCdcLabelVal)

	cmd := fmt.Sprintf("/cdc server --pd=%s-pd:2379 --log-file=\"\" --status-addr=0.0.0.0:8301", tcName)

	ticdcContainer := corev1.Container{
		Name:            v1alpha1.TiCdcMemberType.String(),
		Image:           tc.TiCdcImage(),
		ImagePullPolicy: baseTiCdcSpec.ImagePullPolicy(),
		Command:         []string{"/bin/sh", "-c", cmd},
		Ports: []corev1.ContainerPort{
			{
				Name:          "status",
				ContainerPort: int32(8301),
				Protocol:      corev1.ProtocolTCP,
			},
		},
		Resources: controller.ContainerResource(tc.Spec.TiCdc.ResourceRequirements),
	}
	podSpec := baseTiCdcSpec.BuildPodSpec()
	podSpec.Containers = []corev1.Container{ticdcContainer}
	podSpec.ServiceAccountName = tc.Spec.TiCdc.ServiceAccount

	ticdcDeploy := &apps.Deployment{
		ObjectMeta: metav1.ObjectMeta{
			Name:            deployName,
			Namespace:       ns,
			Labels:          ticdcLabel.Labels(),
			Annotations:     deployAnnotations,
			OwnerReferences: []metav1.OwnerReference{controller.GetOwnerRef(tc)},
		},
		Spec: apps.DeploymentSpec{
			Replicas: controller.Int32Ptr(tc.TiCdcDeployDesiredReplicas()),
			Selector: ticdcLabel.LabelSelector(),
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels:      ticdcLabel.Labels(),
					Annotations: podAnnotations,
				},
				Spec: podSpec,
			},
		},
	}
	return ticdcDeploy, nil
}

func labelTiCdc(tc *v1alpha1.TidbCluster) label.Label {
	instanceName := tc.GetInstanceName()
	return label.New().Instance(instanceName).TiCdc()
}

type FakeTiCdcMemberManager struct {
	err error
}

func NewFakeTiCdcMemberManager() *FakeTiCdcMemberManager {
	return &FakeTiCdcMemberManager{}
}

func (ftmm *FakeTiCdcMemberManager) SetSyncError(err error) {
	ftmm.err = err
}

func (ftmm *FakeTiCdcMemberManager) Sync(tc *v1alpha1.TidbCluster) error {
	if ftmm.err != nil {
		return ftmm.err
	}
	return nil
}
