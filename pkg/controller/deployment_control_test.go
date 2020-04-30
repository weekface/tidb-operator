// Copyright 2018 PingCAP, Inc.
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
	"errors"
	"testing"

	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/fake"
	appslisters "k8s.io/client-go/listers/apps/v1"
	core "k8s.io/client-go/testing"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/record"
)

func TestDeploymentControlCreatesDeployments(t *testing.T) {
	g := NewGomegaWithT(t)
	recorder := record.NewFakeRecorder(10)
	tc := newTidbCluster()
	set := newDeployment(tc, "pd")
	fakeClient := &fake.Clientset{}
	control := NewRealDeploymentControl(fakeClient, nil, recorder)
	fakeClient.AddReactor("create", "statefulsets", func(action core.Action) (bool, runtime.Object, error) {
		create := action.(core.CreateAction)
		return true, create.GetObject(), nil
	})
	err := control.CreateDeployment(tc, set)
	g.Expect(err).To(Succeed())

	events := collectEvents(recorder.Events)
	g.Expect(events).To(HaveLen(1))
	g.Expect(events[0]).To(ContainSubstring(corev1.EventTypeNormal))
}

func TestDeploymentControlCreatesDeploymentExists(t *testing.T) {
	g := NewGomegaWithT(t)
	recorder := record.NewFakeRecorder(10)
	tc := newTidbCluster()
	set := newDeployment(tc, "pd")
	fakeClient := &fake.Clientset{}
	control := NewRealDeploymentControl(fakeClient, nil, recorder)
	fakeClient.AddReactor("create", "statefulsets", func(action core.Action) (bool, runtime.Object, error) {
		return true, set, apierrors.NewAlreadyExists(action.GetResource().GroupResource(), set.Name)
	})
	err := control.CreateDeployment(tc, set)
	g.Expect(apierrors.IsAlreadyExists(err)).To(Equal(true))

	events := collectEvents(recorder.Events)
	g.Expect(events).To(HaveLen(0))
}

func TestDeploymentControlCreatesDeploymentFailed(t *testing.T) {
	g := NewGomegaWithT(t)
	recorder := record.NewFakeRecorder(10)
	tc := newTidbCluster()
	set := newDeployment(tc, "pd")
	fakeClient := &fake.Clientset{}
	control := NewRealDeploymentControl(fakeClient, nil, recorder)
	fakeClient.AddReactor("create", "statefulsets", func(action core.Action) (bool, runtime.Object, error) {
		return true, nil, apierrors.NewInternalError(errors.New("API server down"))
	})
	err := control.CreateDeployment(tc, set)
	g.Expect(err).To(HaveOccurred())

	events := collectEvents(recorder.Events)
	g.Expect(events).To(HaveLen(1))
	g.Expect(events[0]).To(ContainSubstring(corev1.EventTypeWarning))
}

func TestDeploymentControlUpdateDeployment(t *testing.T) {
	g := NewGomegaWithT(t)
	recorder := record.NewFakeRecorder(10)
	tc := newTidbCluster()
	set := newDeployment(tc, "pd")
	set.Spec.Replicas = func() *int32 { var i int32 = 100; return &i }()
	fakeClient := &fake.Clientset{}
	control := NewRealDeploymentControl(fakeClient, nil, recorder)
	fakeClient.AddReactor("update", "statefulsets", func(action core.Action) (bool, runtime.Object, error) {
		update := action.(core.UpdateAction)
		return true, update.GetObject(), nil
	})
	updateSS, err := control.UpdateDeployment(tc, set)
	g.Expect(err).To(Succeed())
	g.Expect(int(*updateSS.Spec.Replicas)).To(Equal(100))
}

func TestDeploymentControlUpdateDeploymentConflictSuccess(t *testing.T) {
	g := NewGomegaWithT(t)
	recorder := record.NewFakeRecorder(10)
	tc := newTidbCluster()
	set := newDeployment(tc, "pd")
	set.Spec.Replicas = func() *int32 { var i int32 = 100; return &i }()
	fakeClient := &fake.Clientset{}
	indexer := cache.NewIndexer(cache.MetaNamespaceKeyFunc, cache.Indexers{cache.NamespaceIndex: cache.MetaNamespaceIndexFunc})
	oldSet := newDeployment(tc, "pd")
	oldSet.Spec.Replicas = func() *int32 { var i int32 = 200; return &i }()
	err := indexer.Add(oldSet)
	g.Expect(err).To(Succeed())
	setLister := appslisters.NewDeploymentLister(indexer)
	control := NewRealDeploymentControl(fakeClient, setLister, recorder)
	conflict := false
	fakeClient.AddReactor("update", "statefulsets", func(action core.Action) (bool, runtime.Object, error) {
		update := action.(core.UpdateAction)
		if !conflict {
			conflict = true
			return true, oldSet, apierrors.NewConflict(action.GetResource().GroupResource(), set.Name, errors.New("conflict"))
		}
		return true, update.GetObject(), nil
	})
	updateSS, err := control.UpdateDeployment(tc, set)
	g.Expect(err).To(Succeed())
	g.Expect(int(*updateSS.Spec.Replicas)).To(Equal(100))
}

func TestDeploymentControlDeleteDeployment(t *testing.T) {
	g := NewGomegaWithT(t)
	recorder := record.NewFakeRecorder(10)
	tc := newTidbCluster()
	set := newDeployment(tc, "pd")
	fakeClient := &fake.Clientset{}
	control := NewRealDeploymentControl(fakeClient, nil, recorder)
	fakeClient.AddReactor("delete", "statefulsets", func(action core.Action) (bool, runtime.Object, error) {
		return true, nil, nil
	})
	err := control.DeleteDeployment(tc, set)
	g.Expect(err).To(Succeed())
	events := collectEvents(recorder.Events)
	g.Expect(events).To(HaveLen(1))
	g.Expect(events[0]).To(ContainSubstring(corev1.EventTypeNormal))
}

func TestDeploymentControlDeleteDeploymentFailed(t *testing.T) {
	g := NewGomegaWithT(t)
	recorder := record.NewFakeRecorder(10)
	tc := newTidbCluster()
	set := newDeployment(tc, "pd")
	fakeClient := &fake.Clientset{}
	control := NewRealDeploymentControl(fakeClient, nil, recorder)
	fakeClient.AddReactor("delete", "statefulsets", func(action core.Action) (bool, runtime.Object, error) {
		return true, nil, apierrors.NewInternalError(errors.New("API server down"))
	})
	err := control.DeleteDeployment(tc, set)
	g.Expect(err).To(HaveOccurred())

	events := collectEvents(recorder.Events)
	g.Expect(events).To(HaveLen(1))
	g.Expect(events[0]).To(ContainSubstring(corev1.EventTypeWarning))
}
