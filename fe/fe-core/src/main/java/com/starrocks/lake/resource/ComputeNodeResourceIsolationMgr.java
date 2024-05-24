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
import com.starrocks.authentication.AuthenticationMgr;
import com.starrocks.common.Config;
import com.starrocks.common.util.DnsCache;
import com.starrocks.persist.metablock.SRMetaBlockEOFException;
import com.starrocks.persist.metablock.SRMetaBlockException;
import com.starrocks.persist.metablock.SRMetaBlockID;
import com.starrocks.persist.metablock.SRMetaBlockReader;
import com.starrocks.persist.metablock.SRMetaBlockWriter;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.RunMode;
import com.starrocks.sql.ast.SetResourceIsolationVar;
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
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Created by liujing on 2024/5/9.
 */
public class ComputeNodeResourceIsolationMgr {

    private static final Logger LOG = LogManager.getLogger(ComputeNodeResourceIsolationMgr.class);

    private static final int MAGIC_HEADER = 81899536;

    private final boolean enabled;
    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
    // userName -> cn ids read resource
    private final Map<String, Set<Long>> userReadAvailableComputeNodeIds = new ConcurrentHashMap<>();
    // root -> cn worker ids write resource
    private final Set<Long> rootWriteAvailableComputeNodeIds = new CopyOnWriteArraySet<>();

    public ComputeNodeResourceIsolationMgr() {
        this.enabled = RunMode.isSharedDataMode() && Config.enable_compute_node_resource_isolation;
    }

    public void setRootComputeNodeWriteResource(List<String> hosts) {
        if (!enabled) {
            LOG.warn("Compute node resource isolation manager, not enabled.");
            return;
        }
        writeLock();
        try {
            Set<Long> uniqueCnIds = findComputeNodeIds(hosts);
            if (uniqueCnIds.isEmpty()) {
                LOG.warn("Compute node resource isolation manager, user: {}, " +
                        "read available computeNodes are empty.", AuthenticationMgr.ROOT_USER);
                return;
            }
            this.rootWriteAvailableComputeNodeIds.addAll(uniqueCnIds);
            GlobalStateMgr.getCurrentState()
                    .getEditLog()
                    .logSetUserComputeNodeResource(
                            UserComputeNodeResourceInfo.create(SetResourceIsolationVar.WRITE, AuthenticationMgr.ROOT_USER,
                                    uniqueCnIds));
            LOG.info("Compute node resource isolation manager, " +
                    "set user compute node write resource, {},", uniqueCnIds);
        } finally {
            writeUnlock();
        }
    }

    public void setUserComputeNodeReadResource(UserIdentity user, List<String> hosts) {
        if (!enabled) {
            LOG.warn("Compute node resource isolation manager, not enabled.");
            return;
        }
        writeLock();
        try {
            final Set<Long> uniqueCnIds = findComputeNodeIds(hosts);
            if (uniqueCnIds.isEmpty()) {
                LOG.warn("Compute node resource isolation manager, user: {}, " +
                        "read available computeNodes are empty.", user.getUser());
                return;
            }
            this.userReadAvailableComputeNodeIds.put(user.getUser(), uniqueCnIds);
            GlobalStateMgr.getCurrentState()
                    .getEditLog()
                    .logSetUserComputeNodeResource(
                            UserComputeNodeResourceInfo.create(SetResourceIsolationVar.READ, user.getUser(),
                                    uniqueCnIds));
            LOG.info("Compute node resource isolation manager, " +
                    "set user compute node read resource, {},", uniqueCnIds);
        } finally {
            writeUnlock();
        }
    }

    public void replaySetUserComputeNodeResource(UserComputeNodeResourceInfo info) {
        if (!enabled) {
            LOG.warn("Compute node resource isolation manager, not enabled.");
            return;
        }
        writeLock();
        try {
            if (SetResourceIsolationVar.READ == info.getResourceOperate()) {
                this.userReadAvailableComputeNodeIds.put(
                        info.getResourceUser(),
                        info.getComputeNodeIds());
            } else if (SetResourceIsolationVar.WRITE == info.getResourceOperate()
                    && AuthenticationMgr.ROOT_USER.equals(info.getResourceUser())) {
                this.rootWriteAvailableComputeNodeIds.addAll(info.getComputeNodeIds());
            } else {
                return;
            }
            LOG.info("Compute node resource isolation manager," +
                            " replay set user[{}] compute node resource: {}, operate: {}.",
                    info.getResourceUser(),
                    info.getComputeNodeIds(),
                    info.getResourceOperate());

        } finally {
            writeUnlock();
        }
    }

    public Set<Long> getUserReadAvailableComputeNodeIds(String u) {
        readLock();
        try {
            return this.userReadAvailableComputeNodeIds.containsKey(u) ?
                    this.userReadAvailableComputeNodeIds.get(u) : Collections.emptySet();
        } finally {
            readUnlock();
        }
    }

    public Function<Long, Boolean> getUserReadAvailableFilterFunc(UserIdentity u) {
        readLock();
        try {
            return new Function<Long, Boolean>() {
                final Set<Long> nodeIds = u == null ?
                        Collections.emptySet() :
                        getUserReadAvailableComputeNodeIds(u.getUser());

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

    private Set<Long> findComputeNodeIds(List<String> hosts) {
        Map<String, ComputeNode> computeNodes;
        ImmutableMap<Long, ComputeNode> idToComputeNode =
                GlobalStateMgr.getCurrentWarehouseMgr().getComputeNodesFromWarehouse();
        if (Config.compute_node_resource_isolation_by_ip) {
            computeNodes = idToComputeNode.values().stream().collect(Collectors.toMap(ComputeNode::getIP, cn -> cn));
            hosts.stream().map(DnsCache::tryLookup).collect(Collectors.toList());
        } else {
            computeNodes = idToComputeNode.values().stream().collect(Collectors.toMap(ComputeNode::getHost, cn -> cn));
        }
        List<Long> cnIds = hosts.stream()
                .filter(host -> StringUtils.isNotBlank(host) && computeNodes.containsKey(host))
                .map(host -> computeNodes.get(host).getId())
                .collect(Collectors.toList());
        return new HashSet<>(cnIds);
    }

    public void load(SRMetaBlockReader reader) throws IOException, SRMetaBlockException, SRMetaBlockEOFException {
        int magicHeader = reader.readJson(int.class);
        if (magicHeader == MAGIC_HEADER) {
            int len = reader.readJson(int.class);
            for (int i = 0; i < len; i++) {
                UserComputeNodeResourceInfo userComputeNodeResourceInfo = reader.readJson(UserComputeNodeResourceInfo.class);
                if (SetResourceIsolationVar.READ == userComputeNodeResourceInfo.getResourceOperate()) {
                    this.userReadAvailableComputeNodeIds.put(
                            userComputeNodeResourceInfo.getResourceUser(),
                            userComputeNodeResourceInfo.getComputeNodeIds());
                } else if (SetResourceIsolationVar.WRITE == userComputeNodeResourceInfo.getResourceOperate()) {
                    this.rootWriteAvailableComputeNodeIds.addAll(userComputeNodeResourceInfo.getComputeNodeIds());
                }
            }
            LOG.info("loaded {} userReadAvailableComputeNodeIds", len);
        }
    }

    public void save(DataOutputStream dos) throws IOException {
        final int len = userReadAvailableComputeNodeIds.size() + (rootWriteAvailableComputeNodeIds.isEmpty() ? 0 : 1);
        if (len > 0) {
            try {
                SRMetaBlockWriter writer = new SRMetaBlockWriter(dos, SRMetaBlockID.COMPUTE_NODE_RESOURCE_ISOLATION_MGR, len + 2);
                writer.writeJson(MAGIC_HEADER);
                writer.writeJson(len);
                for (Map.Entry<String, Set<Long>> entry : userReadAvailableComputeNodeIds.entrySet()) {
                    UserComputeNodeResourceInfo userComputeNodeReadResourceInfo =
                            UserComputeNodeResourceInfo.create(
                                    SetResourceIsolationVar.READ,
                                    entry.getKey(),
                                    entry.getValue());
                    writer.writeJson(userComputeNodeReadResourceInfo);
                }
                if (!rootWriteAvailableComputeNodeIds.isEmpty()) {
                    UserComputeNodeResourceInfo userComputeNodeWriteResourceInfo =
                            UserComputeNodeResourceInfo.create(
                                    SetResourceIsolationVar.WRITE,
                                    AuthenticationMgr.ROOT_USER,
                                    this.rootWriteAvailableComputeNodeIds);
                    writer.writeJson(userComputeNodeWriteResourceInfo);
                }
                LOG.info("saved {} userReadAvailableComputeNodeIds", len);
                writer.close();
            } catch (SRMetaBlockException e) {
                IOException exception = new IOException("failed to save ComputeNodeResourceIsolationMgr!");
                exception.initCause(e);
                throw exception;
            }
        }
    }
}
