// Copyright 2021-present StarRocks, Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.starrocks.lake.resource;

import com.google.common.collect.ImmutableMap;
import com.starrocks.common.Config;
import com.starrocks.common.util.DnsCache;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.ast.UserIdentity;
import com.starrocks.system.ComputeNode;
import org.apache.commons.lang.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Created by liujing on 2024/5/9.
 */
public class ComputeNodeResourceIsolationMgr {

    private static final Logger LOG = LogManager.getLogger(ComputeNodeResourceIsolationMgr.class);

    private final boolean enabled;
    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
    //userName -> cn ids
    private final Map<String, Set<Long>> userAvailableComputeNodeIds = new ConcurrentHashMap<>();

    public ComputeNodeResourceIsolationMgr(boolean enabled) {
        this.enabled = enabled;
    }

    public void setUserComputeNodeResource(UserIdentity user, List<String> hosts) {
        if (!enabled) {
            LOG.warn("Compute node resource isolation manager, not enabled.");
            return;
        }
        writeLock();
        try {
            Map<String, ComputeNode> computeNodes;
            ImmutableMap<Long, ComputeNode> idToComputeNode =
                    GlobalStateMgr.getCurrentWarehouseMgr().getComputeNodesFromWarehouse();
            if (Config.compute_node_resource_group_isolation_by_ip) {
                hosts.stream().map(DnsCache::tryLookup).collect(Collectors.toList());
                computeNodes = idToComputeNode.values().stream().collect(Collectors.toMap(ComputeNode::getIP, cn -> cn));
            } else {
                computeNodes = idToComputeNode.values().stream().collect(Collectors.toMap(ComputeNode::getHost, cn -> cn));
            }
            List<Long> cnIds = hosts.stream()
                    .filter(host -> StringUtils.isNotBlank(host) && computeNodes.containsKey(host))
                    .map(host -> computeNodes.get(host).getId())
                    .collect(Collectors.toList());
            if (cnIds.isEmpty()) {
                LOG.warn("Compute node resource isolation manager, user: {}, " +
                        "availableComputeNodes are empty.", user.getUser());
                return;
            }
            this.userAvailableComputeNodeIds.put(user.getUser(), new HashSet<>(cnIds));
            GlobalStateMgr.getCurrentState().getEditLog().logSetUserComputeNodeResource(user, cnIds);
            LOG.info("Debug -> Compute node resource isolation manager, " +
                    "set user compute node resource, {},", cnIds);
        } finally {
            writeUnlock();
        }
    }

    public void replaySetUserComputeNodeResource(UserComputeNodeResourceInfo userComputeNodeResourceInfo) {
        writeLock();
        try {
            this.userAvailableComputeNodeIds.put(
                    userComputeNodeResourceInfo.getResourceUser(),
                    userComputeNodeResourceInfo.getComputeNodeIds());
            LOG.info("Debug -> Compute node resource isolation manager," +
                    " replay set user compute node resource, {},", userComputeNodeResourceInfo);
        } finally {
            writeUnlock();
        }
    }

    public Set<Long> getUserAvailableComputeNodeIds(String u) {
        readLock();
        try {
            return this.userAvailableComputeNodeIds.containsKey(u) ?
                    Collections.emptySet() : this.userAvailableComputeNodeIds.get(u);
        } finally {
            readUnlock();
        }
    }

    public Function<Long, Boolean> getUserAvailableFilterFunc(String u) {
        readLock();
        try {
            return new Function<Long, Boolean>() {

                final Set<Long> nodeIds = getUserAvailableComputeNodeIds(u);

                @Override
                public Boolean apply(Long id) {
                    return nodeIds.isEmpty() ? true : nodeIds.contains(id);
                }
            };
        } finally {
            readUnlock();
        }
    }

    private void readLock() {
        lock.readLock().lock();
    }

    private void readUnlock() {
        lock.readLock().unlock();
    }

    private void writeLock() {
        lock.writeLock().lock();
    }

    private void writeUnlock() {
        lock.writeLock().unlock();
    }

}
