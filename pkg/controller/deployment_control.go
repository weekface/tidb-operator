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

package controller

import (
	"fmt"
	"strings"

	"github.com/pingcap/tidb-operator/pkg/apis/pingcap/v1alpha1"
	tcinformers "github.com/pingcap/tidb-operator/pkg/client/informers/externalversions/pingcap/v1alpha1"
	v1listers "github.com/pingcap/tidb-operator/pkg/client/listers/pingcap/v1alpha1"
	apps "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	appsinformers "k8s.io/client-go/informers/apps/v1"
	"k8s.io/client-go/kubernetes"
	appslisters "k8s.io/client-go/listers/apps/v1"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/record"
	"k8s.io/client-go/util/retry"
	"k8s.io/klog"
)

// DeploymentControlInterface defines the interface that uses to create, update, and delete Deployments,
type DeploymentControlInterface interface {
	// CreateDeployment creates a Deployment in a TidbCluster.
	CreateDeployment(*v1alpha1.TidbCluster, *apps.Deployment) error
	// UpdateDeployment updates a Deployment in a TidbCluster.
	UpdateDeployment(*v1alpha1.TidbCluster, *apps.Deployment) (*apps.Deployment, error)
	// DeleteDeployment deletes a Deployment in a TidbCluster.
	DeleteDeployment(*v1alpha1.TidbCluster, *apps.Deployment) error
}

type realDeploymentControl struct {
	kubeCli      kubernetes.Interface
	deployLister appslisters.DeploymentLister
	recorder     record.EventRecorder
}

// NewRealDeploymentControl returns a DeploymentControlInterface
func NewRealDeploymentControl(kubeCli kubernetes.Interface, deployLister appslisters.DeploymentLister, recorder record.EventRecorder) DeploymentControlInterface {
	return &realDeploymentControl{kubeCli, deployLister, recorder}
}

// CreateDeployment create a Deployment in a TidbCluster.
func (sc *realDeploymentControl) CreateDeployment(tc *v1alpha1.TidbCluster, deploy *apps.Deployment) error {
	_, err := sc.kubeCli.AppsV1().Deployments(tc.Namespace).Create(deploy)
	// sink already exists errors
	if apierrors.IsAlreadyExists(err) {
		return err
	}
	sc.recordDeploymentEvent("create", tc, deploy, err)
	return err
}

// UpdateDeployment update a Deployment in a TidbCluster.
func (sc *realDeploymentControl) UpdateDeployment(tc *v1alpha1.TidbCluster, deploy *apps.Deployment) (*apps.Deployment, error) {
	ns := tc.GetNamespace()
	tcName := tc.GetName()
	deployName := deploy.GetName()
	deploySpec := deploy.Spec.DeepCopy()
	var updatedDeploy *apps.Deployment

	err := retry.RetryOnConflict(retry.DefaultBackoff, func() error {
		// TODO: verify if Deployment identity(name, namespace, labels) matches TidbCluster
		var updateErr error
		updatedDeploy, updateErr = sc.kubeCli.AppsV1().Deployments(ns).Update(deploy)
		if updateErr == nil {
			klog.Infof("TidbCluster: [%s/%s]'s Deployment: [%s/%s] updated successfully", ns, tcName, ns, deployName)
			return nil
		}
		klog.Errorf("failed to update TidbCluster: [%s/%s]'s Deployment: [%s/%s], error: %v", ns, tcName, ns, deployName, updateErr)

		if updated, err := sc.deployLister.Deployments(ns).Get(deployName); err == nil {
			// make a copy so we don't mutate the shared cache
			deploy = updated.DeepCopy()
			deploy.Spec = *deploySpec
		} else {
			utilruntime.HandleError(fmt.Errorf("error getting updated Deployment %s/%s from lister: %v", ns, deployName, err))
		}
		return updateErr
	})

	return updatedDeploy, err
}

// DeleteDeployment delete a Deployment in a TidbCluster.
func (sc *realDeploymentControl) DeleteDeployment(tc *v1alpha1.TidbCluster, deploy *apps.Deployment) error {
	err := sc.kubeCli.AppsV1().Deployments(tc.Namespace).Delete(deploy.Name, nil)
	sc.recordDeploymentEvent("delete", tc, deploy, err)
	return err
}

func (sc *realDeploymentControl) recordDeploymentEvent(verb string, tc *v1alpha1.TidbCluster, deploy *apps.Deployment, err error) {
	tcName := tc.Name
	deployName := deploy.Name
	if err == nil {
		reason := fmt.Sprintf("Successful%s", strings.Title(verb))
		message := fmt.Sprintf("%s Deployment %s in TidbCluster %s successful",
			strings.ToLower(verb), deployName, tcName)
		sc.recorder.Event(tc, corev1.EventTypeNormal, reason, message)
	} else {
		reason := fmt.Sprintf("Failed%s", strings.Title(verb))
		message := fmt.Sprintf("%s Deployment %s in TidbCluster %s failed error: %s",
			strings.ToLower(verb), deployName, tcName, err)
		sc.recorder.Event(tc, corev1.EventTypeWarning, reason, message)
	}
}

var _ DeploymentControlInterface = &realDeploymentControl{}

// FakeDeploymentControl is a fake DeploymentControlInterface
type FakeDeploymentControl struct {
	DeployLister            appslisters.DeploymentLister
	DeployIndexer           cache.Indexer
	TcLister                v1listers.TidbClusterLister
	TcIndexer               cache.Indexer
	createDeploymentTracker RequestTracker
	updateDeploymentTracker RequestTracker
	deleteDeploymentTracker RequestTracker
	statusChange            func(deploy *apps.Deployment)
}

// NewFakeDeploymentControl returns a FakeDeploymentControl
func NewFakeDeploymentControl(deployInformer appsinformers.DeploymentInformer, tcInformer tcinformers.TidbClusterInformer) *FakeDeploymentControl {
	return &FakeDeploymentControl{
		deployInformer.Lister(),
		deployInformer.Informer().GetIndexer(),
		tcInformer.Lister(),
		tcInformer.Informer().GetIndexer(),
		RequestTracker{},
		RequestTracker{},
		RequestTracker{},
		nil,
	}
}

// SetCreateDeploymentError sets the error attributes of createDeploymentTracker
func (ssc *FakeDeploymentControl) SetCreateDeploymentError(err error, after int) {
	ssc.createDeploymentTracker.SetError(err).SetAfter(after)
}

// SetUpdateDeploymentError sets the error attributes of updateDeploymentTracker
func (ssc *FakeDeploymentControl) SetUpdateDeploymentError(err error, after int) {
	ssc.updateDeploymentTracker.SetError(err).SetAfter(after)
}

// SetDeleteDeploymentError sets the error attributes of deleteDeploymentTracker
func (ssc *FakeDeploymentControl) SetDeleteDeploymentError(err error, after int) {
	ssc.deleteDeploymentTracker.SetError(err).SetAfter(after)
}

func (ssc *FakeDeploymentControl) SetStatusChange(fn func(*apps.Deployment)) {
	ssc.statusChange = fn
}

// CreateDeployment adds the deployment to DeployIndexer
func (ssc *FakeDeploymentControl) CreateDeployment(_ *v1alpha1.TidbCluster, deploy *apps.Deployment) error {
	defer func() {
		ssc.createDeploymentTracker.Inc()
		ssc.statusChange = nil
	}()

	if ssc.createDeploymentTracker.ErrorReady() {
		defer ssc.createDeploymentTracker.Reset()
		return ssc.createDeploymentTracker.GetError()
	}

	if ssc.statusChange != nil {
		ssc.statusChange(deploy)
	}

	return ssc.DeployIndexer.Add(deploy)
}

// UpdateDeployment updates the deployment of DeployIndexer
func (ssc *FakeDeploymentControl) UpdateDeployment(_ *v1alpha1.TidbCluster, deploy *apps.Deployment) (*apps.Deployment, error) {
	defer func() {
		ssc.updateDeploymentTracker.Inc()
		ssc.statusChange = nil
	}()

	if ssc.updateDeploymentTracker.ErrorReady() {
		defer ssc.updateDeploymentTracker.Reset()
		return nil, ssc.updateDeploymentTracker.GetError()
	}

	if ssc.statusChange != nil {
		ssc.statusChange(deploy)
	}
	return deploy, ssc.DeployIndexer.Update(deploy)
}

// DeleteDeployment deletes the deployment of DeployIndexer
func (ssc *FakeDeploymentControl) DeleteDeployment(_ *v1alpha1.TidbCluster, _ *apps.Deployment) error {
	return nil
}

var _ DeploymentControlInterface = &FakeDeploymentControl{}
