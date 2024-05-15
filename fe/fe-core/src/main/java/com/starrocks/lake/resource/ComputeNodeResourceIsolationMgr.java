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
import com.starrocks.persist.metablock.SRMetaBlockEOFException;
import com.starrocks.persist.metablock.SRMetaBlockException;
import com.starrocks.persist.metablock.SRMetaBlockID;
import com.starrocks.persist.metablock.SRMetaBlockReader;
import com.starrocks.persist.metablock.SRMetaBlockWriter;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.ast.UserIdentity;
import com.starrocks.system.ComputeNode;
import org.apache.commons.lang.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.DataOutputStream;
import java.io.IOException;
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

    private static final int MAGIC_HEADER = 81899536;

    private final transient boolean enabled;

    private final transient ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
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
                    this.userAvailableComputeNodeIds.get(u) : Collections.emptySet();
        } finally {
            readUnlock();
        }
    }

    public Function<Long, Boolean> getUserAvailableFilterFunc(UserIdentity u) {
        readLock();
        try {
            return new Function<Long, Boolean>() {
                final Set<Long> nodeIds = u == null ?
                        Collections.emptySet() :
                        getUserAvailableComputeNodeIds(u.getUser());

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

    public void load(SRMetaBlockReader reader) throws IOException, SRMetaBlockException, SRMetaBlockEOFException {
        int magicHeader = reader.readJson(int.class);
        if (magicHeader == MAGIC_HEADER) {
            int len = reader.readJson(int.class);
            for (int i = 0; i < len; i++) {
                UserComputeNodeResourceInfo userComputeNodeResourceInfo = reader.readJson(UserComputeNodeResourceInfo.class);
                this.userAvailableComputeNodeIds.put(
                        userComputeNodeResourceInfo.getResourceUser(),
                        userComputeNodeResourceInfo.getComputeNodeIds());
            }
            LOG.info("loaded {} userAvailableComputeNodeIds", len);
        }
    }

    public void save(DataOutputStream dos) throws IOException {
        final int len = userAvailableComputeNodeIds.size();
        if (len > 0) {
            try {
                SRMetaBlockWriter writer = new SRMetaBlockWriter(dos, SRMetaBlockID.COMPUTE_NODE_RESOURCE_ISOLATION_MGR, len + 2);
                writer.writeJson(MAGIC_HEADER);
                writer.writeJson(len);
                for (Map.Entry<String, Set<Long>> entry : userAvailableComputeNodeIds.entrySet()) {
                    UserComputeNodeResourceInfo userComputeNodeResourceInfo =
                            new UserComputeNodeResourceInfo(entry.getKey(), entry.getValue());
                    writer.writeJson(userComputeNodeResourceInfo);
                }
                LOG.info("saved {} userAvailableComputeNodeIds", len);
                writer.close();
            } catch (SRMetaBlockException e) {
                IOException exception = new IOException("failed to save ComputeNodeResourceIsolationMgr!");
                exception.initCause(e);
                throw exception;
            }
        }
    }
}
