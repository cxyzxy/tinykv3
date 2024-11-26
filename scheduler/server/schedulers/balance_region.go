// Copyright 2017 PingCAP, Inc.
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

package schedulers

import (
	"fmt"
	"sort"

	"github.com/pingcap-incubator/tinykv/scheduler/server/core"
	"github.com/pingcap-incubator/tinykv/scheduler/server/schedule"
	"github.com/pingcap-incubator/tinykv/scheduler/server/schedule/operator"
	"github.com/pingcap-incubator/tinykv/scheduler/server/schedule/opt"
)

func init() {
	schedule.RegisterSliceDecoderBuilder("balance-region", func(args []string) schedule.ConfigDecoder {
		return func(v interface{}) error {
			return nil
		}
	})
	schedule.RegisterScheduler("balance-region", func(opController *schedule.OperatorController, storage *core.Storage, decoder schedule.ConfigDecoder) (schedule.Scheduler, error) {
		return newBalanceRegionScheduler(opController), nil
	})
}

const (
	// balanceRegionRetryLimit is the limit to retry schedule for selected store.
	balanceRegionRetryLimit = 10
	balanceRegionName       = "balance-region-scheduler"
)

type balanceRegionScheduler struct {
	*baseScheduler
	name         string
	opController *schedule.OperatorController
}

// newBalanceRegionScheduler creates a scheduler that tends to keep regions on
// each store balanced.
func newBalanceRegionScheduler(opController *schedule.OperatorController, opts ...BalanceRegionCreateOption) schedule.Scheduler {
	base := newBaseScheduler(opController)
	s := &balanceRegionScheduler{
		baseScheduler: base,
		opController:  opController,
	}
	for _, opt := range opts {
		opt(s)
	}
	return s
}

// BalanceRegionCreateOption is used to create a scheduler with an option.
type BalanceRegionCreateOption func(s *balanceRegionScheduler)

func (s *balanceRegionScheduler) GetName() string {
	if s.name != "" {
		return s.name
	}
	return balanceRegionName
}

func (s *balanceRegionScheduler) GetType() string {
	return "balance-region"
}

func (s *balanceRegionScheduler) IsScheduleAllowed(cluster opt.Cluster) bool {
	return s.opController.OperatorCount(operator.OpRegion) < cluster.GetRegionScheduleLimit()
}

type sortStore []*core.StoreInfo

func (s sortStore) Len() int {
	return len(s)
}

func (s sortStore) Less(i int, j int) bool {
	return s[i].GetRegionSize() < s[j].GetRegionSize()
}

func (s sortStore) Swap(i int, j int) {
	s[i], s[j] = s[j], s[i]
}

func (s *balanceRegionScheduler) Schedule(cluster opt.Cluster) *operator.Operator {
	// Your Code Here (3C).
	// todo need to refactor
	maxDownTime := cluster.GetMaxStoreDownTime()
	suitableStores := make(sortStore, 0)
	// get all suitable stores
	for _, store := range cluster.GetStores() {
		if store.IsUp() && store.DownTime() <= maxDownTime {
			suitableStores = append(suitableStores, store)
		}
	}

	// sort it by region size
	sort.Sort(suitableStores)
	var suitableStore *core.StoreInfo
	var suitableRegion *core.RegionInfo
	for i := len(suitableStores) - 1; i >= 0; i-- {
		suitableStore = suitableStores[i]
		cluster.GetPendingRegionsWithLock(suitableStore.GetID(), func(container core.RegionsContainer) {
			suitableRegion = container.RandomRegion(nil, nil)
		})
		if suitableRegion != nil {
			break
		}

		cluster.GetFollowersWithLock(suitableStore.GetID(), func(container core.RegionsContainer) {
			suitableRegion = container.RandomRegion(nil, nil)
		})
		if suitableRegion != nil {
			break
		}

		cluster.GetLeadersWithLock(suitableStore.GetID(), func(container core.RegionsContainer) {
			suitableRegion = container.RandomRegion(nil, nil)
		})
		if suitableRegion != nil {
			break
		}
	}

	if suitableRegion == nil {
		return nil
	}

	originalStoreIds := suitableRegion.GetStoreIds()

	if len(originalStoreIds) < cluster.GetMaxReplicas() {
		return nil
	}

	// find targetStore that not in originStore
	var targetStore *core.StoreInfo
	for i := 0; i < len(suitableStores); i++ {
		curStoreId := suitableStores[i].GetID()
		if _, ok := originalStoreIds[curStoreId]; !ok {
			targetStore = suitableStores[i]
			break
		}
	}

	if targetStore == nil {
		return nil
	}

	if suitableStore.GetRegionSize()-targetStore.GetRegionSize() < 2*suitableRegion.GetApproximateSize() {
		return nil
	}

	// the Scheduler should allocate a new peer on the target store and create a move peer operator.
	newPeer, err := cluster.AllocPeer(targetStore.GetID())
	if err != nil {
		return nil
	}
	op, err := operator.CreateMovePeerOperator(
		fmt.Sprintf(
			"<region %d> move from <store %d> to <store %d>",
			suitableRegion.GetID(),
			suitableStore.GetID(),
			targetStore.GetID(),
		),
		cluster,
		suitableRegion,
		operator.OpBalance,
		suitableStore.GetID(),
		targetStore.GetID(),
		newPeer.GetId(),
	)
	if err != nil {
		return nil
	}
	return op

}
