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

// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/task/AgentBatchTask.java

// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package com.staros.schedule;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.staros.exception.ExceptionCode;
import com.staros.exception.NoAliveWorkersException;
import com.staros.exception.ScheduleConflictStarException;
import com.staros.exception.StarException;
import com.staros.metrics.MetricsSystem;
import com.staros.proto.AddShardInfo;
import com.staros.proto.AddShardRequest;
import com.staros.proto.PlacementPolicy;
import com.staros.proto.RemoveShardRequest;
import com.staros.schedule.select.FirstNSelector;
import com.staros.schedule.select.Selector;
import com.staros.service.ServiceManager;
import com.staros.shard.Shard;
import com.staros.shard.ShardGroup;
import com.staros.shard.ShardManager;
import com.staros.shard.ShardPolicyFilter;
import com.staros.util.AbstractServer;
import com.staros.util.Config;
import com.staros.util.LockCloseable;
import com.staros.util.Utils;
import com.staros.worker.Worker;
import com.staros.worker.WorkerGroup;
import com.staros.worker.WorkerManager;
import io.prometheus.metrics.core.datapoints.CounterDataPoint;
import io.prometheus.metrics.core.metrics.Counter;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.Closeable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.FutureTask;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Created by liujing on 2024/5/15.
 */
public class ShardSchedulerV3 extends AbstractServer implements Scheduler {

    private static final Logger LOG = LogManager.getLogger(ShardSchedulerV3.class);
    private static final int PRIORITY_LOW = 0;
    private static final int PRIORITY_MEDIUM = 10;
    private static final int PRIORITY_HIGH = 20;
    private static final int ADJUST_THREAD_INTERVAL_SECS = 300;
    private static final int INIT_CHECK_THREAD_DELAY_SECS = 30;
    private static final int SHORT_NAP = 100;
    private final ShardSchedulerV3.ExclusiveLocker requestLocker = new ShardSchedulerV3.ExclusiveLocker();
    private final Selector scoreSelector = new FirstNSelector();
    private static final List<PlacementPolicy> CONFLICT_POLICIES;
    private ScheduledThreadPoolExecutor calculateExecutors;
    private ThreadPoolExecutor dispatchExecutors;
    private ScheduledThreadPoolExecutor adjustPoolExecutors;
    private final ServiceManager serviceManager;
    private final WorkerManager workerManager;
    private static final Counter METRIC_WORKER_SHARD_COUNT;
    private final CounterDataPoint addShardOpCounter;
    private final CounterDataPoint removeShardOpCounter;

    private ShardSchedulerV3.DispatchTask<StarException> dispatchTaskForAddToWorker(
            String serviceId,
            List<Long> shardIds,
            long workerId,
            int priority) {
        Callable<StarException> callable = () -> {
            try {
                this.executeAddToWorker(serviceId, shardIds, workerId);
                return null;
            } catch (StarException starException) {
                return starException;
            } catch (Exception exception) {
                return new StarException(ExceptionCode.SCHEDULE, exception.getMessage());
            }
        };
        String description = String.format("[AddToWorker Task] serviceId: %s," +
                " workerId: %s, priority: %d", serviceId, workerId, priority);
        return new ShardSchedulerV3.DispatchTask(callable, priority, description);
    }

    private ShardSchedulerV3.DispatchTask<Boolean> dispatchTaskForAddToGroup(ScheduleRequestContext ctx, int priority) {
        String description = String.format("[AddToGroup Task] %s, " +
                "priority: %d", ctx, priority);
        return new ShardSchedulerV3.DispatchTask(() -> {
            this.executeAddToGroupPhase2(ctx);
        }, true, priority, description);
    }

    private ShardSchedulerV3.DispatchTask<Boolean> dispatchTaskForRemoveFromGroup(ScheduleRequestContext ctx, int priority) {
        String description = String.format("[RemoveFromGroup Task] %s," +
                " priority: %d", ctx, priority);
        return new ShardSchedulerV3.DispatchTask(() -> {
            this.executeRemoveFromGroupPhase2(ctx);
        }, true, priority, description);
    }

    private ShardSchedulerV3.DispatchTask<StarException> dispatchTaskForRemoveFromWorker(
            String serviceId,
            List<Long> shardIds,
            long workerId,
            int priority) {
        Callable<StarException> callable = () -> {
            try {
                this.executeRemoveFromWorker(serviceId, shardIds, workerId);
                return null;
            } catch (StarException starException) {
                return starException;
            } catch (Exception exception) {
                return new StarException(ExceptionCode.SCHEDULE, exception.getMessage());
            }
        };
        String description = String.format("[RemoveFromWorker Task] serviceId: %s," +
                " workerId: %s, priority: %d", serviceId, workerId, priority);
        return new ShardSchedulerV3.DispatchTask(callable, priority, description);
    }

    public ShardSchedulerV3(ServiceManager serviceManager, WorkerManager workerManager) {
        METRIC_WORKER_SHARD_COUNT.remove("add");
        METRIC_WORKER_SHARD_COUNT.remove("remove");
        this.addShardOpCounter = METRIC_WORKER_SHARD_COUNT.labelValues("add");
        this.removeShardOpCounter = METRIC_WORKER_SHARD_COUNT.labelValues("remove");
        this.serviceManager = serviceManager;
        this.workerManager = workerManager;
    }

    public void scheduleAddToGroup(String serviceId, long shardId, long wgId) throws StarException {
        this.scheduleAddToGroup(serviceId, Collections.nCopies(1, shardId), wgId);
    }

    public void scheduleAddToGroup(String serviceId, List<Long> shardIds, long wgId) throws StarException {
        CountDownLatch latch = new CountDownLatch(shardIds.size());
        List<ScheduleRequestContext> ctxs = new ArrayList();
        Iterator iterator = shardIds.iterator();

        while (iterator.hasNext()) {
            Long id = (Long) iterator.next();
            ScheduleRequestContext ctx = new ScheduleRequestContext(serviceId, id, wgId, latch);
            ctxs.add(ctx);
            this.submitCalcTaskInternal(() -> {
                this.executeAddToGroupPhase1(ctx);
            }, 0L);
        }

        try {
            latch.await();
        } catch (InterruptedException interruptedException) {
            throw new StarException(ExceptionCode.SCHEDULE,
                    interruptedException.getMessage());
        }

        iterator = ctxs.iterator();

        ScheduleRequestContext ctx;
        do {
            if (!iterator.hasNext()) {
                return;
            }

            ctx = (ScheduleRequestContext) iterator.next();
        } while (ctx.getException() == null);

        throw ctx.getException();
    }

    public void scheduleAsyncAddToGroup(String serviceId, long shardId, long wgId) throws StarException {
        ScheduleRequestContext ctx = new ScheduleRequestContext(serviceId, shardId, wgId, (CountDownLatch) null);
        this.submitCalcTaskInternal(() -> {
            this.executeAddToGroupPhase1(ctx);
        }, 0L);
    }

    public void scheduleAddToDefaultGroup(String serviceId, List<Long> shardIds) throws StarException {
        WorkerGroup group = this.workerManager.getDefaultWorkerGroup(serviceId);
        if (group == null) {
            throw new StarException(ExceptionCode.NOT_EXIST,
                    String.format("DefaultWorkerGroup not exist for service %s", serviceId));
        } else {
            this.scheduleAddToGroup(serviceId, shardIds, group.getGroupId());
        }
    }

    public void scheduleAsyncRemoveFromGroup(String serviceId, long shardId, long workerGroupId) throws StarException {
        ScheduleRequestContext ctx = new ScheduleRequestContext(serviceId, shardId, workerGroupId, null);
        this.submitCalcTaskInternal(() -> {
            this.executeRemoveFromGroupPhase1(ctx);
        }, 0L);
    }

    private void executeAddToGroupPhase1(ScheduleRequestContext ctx) {
        try {
            this.executeAddToGroupPhase1Detail(ctx);
        } catch (ScheduleConflictStarException scheduleConflictStarException) {
            this.submitCalcTaskInternal(() -> {
                this.executeAddToGroupPhase1(ctx);
            }, 100L);
        } catch (StarException starException) {
            ctx.done(starException);
        } catch (Throwable throwable) {
            ctx.done(new StarException(ExceptionCode.SCHEDULE, throwable.getMessage()));
        }

    }

    private void submitCalcTaskInternal(Runnable run, long delay) throws StarException {
        try {
            if (delay == 0L) {
                this.calculateExecutors.execute(run);
            } else {
                this.calculateExecutors.schedule(run, delay, TimeUnit.MICROSECONDS);
            }

        } catch (RejectedExecutionException rejectedExecutionException) {
            if (!this.isRunning()) {
                throw new StarException(ExceptionCode.SCHEDULE, "Scheduling shutdown!");
            } else {
                throw new StarException(ExceptionCode.SCHEDULE, rejectedExecutionException.getMessage());
            }
        }
    }

    public void scheduleAddToWorker(String serviceId, long shardId, long workerId) throws StarException {
        this.scheduleAddToWorker(serviceId, Collections.nCopies(1, shardId), workerId);
    }

    public void scheduleAddToWorker(String serviceId, List<Long> shardIds, long workerId) throws StarException {
        ShardSchedulerV3.DispatchTask<StarException> task = this.dispatchTaskForAddToWorker(
                serviceId,
                shardIds,
                workerId,
                20);
        this.submitDispatchTask(task, true);
    }

    public void scheduleAsyncAddToWorker(String serviceId, List<Long> shardIds, long workerId) throws StarException {
        ShardSchedulerV3.DispatchTask<StarException> task = this.dispatchTaskForAddToWorker(
                serviceId,
                shardIds,
                workerId,
                10);
        this.submitDispatchTask(task, false);
    }

    public void scheduleRemoveFromWorker(String serviceId, long shardId, long workerId) throws StarException {
        this.scheduleRemoveFromWorker(serviceId, Collections.nCopies(1, shardId), workerId);
    }

    public void scheduleRemoveFromWorker(String serviceId, List<Long> shardIds, long workerId) throws StarException {
        ShardSchedulerV3.DispatchTask<StarException> task = this.dispatchTaskForRemoveFromWorker(
                serviceId,
                shardIds,
                workerId,
                10);
        this.submitDispatchTask(task, true);
    }

    public void scheduleAsyncRemoveFromWorker(String serviceId, List<Long> shardIds, long workerId) throws StarException {
        ShardSchedulerV3.DispatchTask<StarException> task = this.dispatchTaskForRemoveFromWorker(
                serviceId,
                shardIds,
                workerId,
                0);
        this.submitDispatchTask(task, false);
    }

    private <T extends Exception> void submitDispatchTask(
            ShardSchedulerV3.DispatchTask<T> task,
            boolean wait) throws StarException {
        try {
            this.dispatchExecutors.execute(task);
        } catch (Exception exception) {
            LOG.error("Fail to submit schedule task {}", task, exception);
            throw new StarException(ExceptionCode.SCHEDULE, exception.getMessage());
        }

        if (wait) {
            Exception exception = null;

            try {
                if (task.get() != null) {
                    exception = (Exception) task.get();
                }
            } catch (Throwable throwable) {
                LOG.error("Fail to get task result. task: {}", task, throwable);
                throw new StarException(ExceptionCode.SCHEDULE, throwable.getMessage());
            }

            if (exception != null) {
                if (exception instanceof StarException) {
                    throw (StarException) exception;
                }
                throw new StarException(ExceptionCode.SCHEDULE, exception.getMessage());
            }
        }
    }

    private void executeAddToGroupPhase1Detail(ScheduleRequestContext ctx) {
        ShardManager shardManager = this.serviceManager.getShardManager(ctx.getServiceId());
        if (shardManager == null) {
            ctx.done(new StarException(ExceptionCode.NOT_EXIST,
                    String.format("Service %s Not Exist", ctx.getServiceId())));
        } else {
            Shard shard = shardManager.getShard(ctx.getShardId());
            if (shard == null) {
                ctx.done(new StarException(ExceptionCode.NOT_EXIST,
                        String.format("Shard %d Not Exist", ctx.getShardId())));
            } else {
                int replicaNum = shard.getExpectedReplicaNum();
                Stream<Long> longStream = shard.getReplicaWorkerIds().stream();
                WorkerManager wm = this.workerManager;
                wm.getClass();
                List<Long> existReplicas = longStream.map(wm::getWorker).filter((y) -> {
                    return y != null && y.isAlive() && y.getGroupId() == ctx.getWorkerGroupId();
                }).map(Worker::getWorkerId).collect(Collectors.toList());
                if (existReplicas.size() >= replicaNum) {
                    ctx.done();
                } else {
                    int desired = replicaNum - existReplicas.size();
                    int priority = 10;
                    if (ctx.isWaited()) {
                        priority = 20;
                    } else if (desired <= replicaNum / 2) {
                        priority = 0;
                    }

                    WorkerGroup wg = this.workerManager.getWorkerGroupNoException(shard.getServiceId(), ctx.getWorkerGroupId());
                    if (wg == null) {
                        ctx.done(new StarException(ExceptionCode.NOT_EXIST,
                                String.format("WorkerGroup %d doesn't exist!", ctx.getWorkerGroupId())));
                    } else {
                        Set<Long> wIds = new HashSet(wg.getAllWorkerIds(true));
                        LOG.info("WorkerGroup get All worker ids: {}.", wIds);
                        if (wIds.isEmpty()) {
                            ctx.done(new NoAliveWorkersException("WorkerGroup {} doesn't have alive workers",
                                    ctx.getWorkerGroupId()));
                        } else {
                            existReplicas.forEach(wIds::remove);
                            if (!this.requestLocker.tryLock(ctx, shardManager)) {
                                ctx.reset();
                                throw new ScheduleConflictStarException();
                            } else {
                                ShardSchedulerV3.DeferOp cleanOp = new ShardSchedulerV3.DeferOp(ctx.getRunnable());
                                Throwable t = null;

                                try {
                                    Map<PlacementPolicy, Collection<Long>> ppMap = new HashMap();
                                    Iterator iterator = shard.getGroupIds().iterator();

                                    while (iterator.hasNext()) {
                                        Long gid = (Long) iterator.next();
                                        ShardGroup g = shardManager.getShardGroup(gid);
                                        PlacementPolicy policy = g.getPlacementPolicy();
                                        List<Long> workerIds = new ArrayList();
                                        Iterator shardIdsIt = g.getShardIds().iterator();

                                        while (shardIdsIt.hasNext()) {
                                            Long id = (Long) shardIdsIt.next();
                                            if (id != shard.getShardId()) {
                                                Shard firstDegreeShard = shardManager.getShard(id);
                                                if (firstDegreeShard != null) {
                                                    Stream stream = firstDegreeShard.getReplicaWorkerIds().stream();
                                                    wIds.getClass();
                                                    workerIds.addAll((Collection) stream.
                                                            filter(wIds::contains)
                                                            .collect(Collectors.toList()));
                                                }
                                            }
                                        }

                                        if (!workerIds.isEmpty()) {
                                            if (ppMap.containsKey(policy)) {
                                                ((Collection) ppMap.get(policy)).addAll(workerIds);
                                            } else {
                                                ppMap.put(policy, workerIds);
                                            }
                                        }
                                    }

                                    ShardPolicyFilter.filter(ppMap, wIds);
                                    if (wIds.isEmpty()) {
                                        ctx.done(new StarException(ExceptionCode.SCHEDULE,
                                                String.format("Can't find worker for request: %s", ctx)));
                                        return;
                                    }

                                    Object workerIds;
                                    if (wIds.size() < desired) {
                                        LOG.debug("Schedule requests {} workers, but only {} available. {}",
                                                desired, wIds.size(), ctx);
                                        workerIds = new ArrayList(wIds);
                                    } else {
                                        ScheduleScorer scorer = new ScheduleScorer(wIds);
                                        ppMap.forEach(scorer::apply);
                                        scorer.apply(this.workerManager);
                                        LOG.debug("final scores for selection: {}, for request {}",
                                                scorer.getScores(), ctx);
                                        workerIds = scorer.selectHighEnd(this.scoreSelector, desired);
                                    }

                                    ctx.setWorkerIds((Collection) workerIds);
                                    LOG.debug("Schedule request {}, pending schedule to workerList: {}",
                                            ctx, ctx.getWorkerIds());
                                    ShardSchedulerV3.DispatchTask task = this.dispatchTaskForAddToGroup(ctx, priority);

                                    try {
                                        this.dispatchExecutors.execute(task);
                                        cleanOp.cancel();
                                    } catch (Throwable throwable) {
                                        LOG.error("Fail to add task {} into dispatchWorkerExecutors",
                                                task, throwable);
                                        ctx.done(new StarException(ExceptionCode.SCHEDULE, throwable.getMessage()));
                                    }
                                } catch (Throwable throwable) {
                                    t = throwable;
                                    throw throwable;
                                } finally {
                                    if (cleanOp != null) {
                                        if (t != null) {
                                            try {
                                                cleanOp.close();
                                            } catch (Throwable throwable) {
                                                t.addSuppressed(throwable);
                                            }
                                        } else {
                                            cleanOp.close();
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    private void executeAddToGroupPhase2(ScheduleRequestContext ctx) {
        ShardSchedulerV3.DeferOp ignored = new ShardSchedulerV3.DeferOp(ctx.getRunnable());
        Throwable t = null;

        try {
            if (this.isRunning()) {
                this.executeAddToWorker(ctx);
            } else {
                ctx.done(new StarException(ExceptionCode.SCHEDULE, "Schedule shutdown in progress"));
            }
        } catch (Throwable throwable) {
            t = throwable;
            throw throwable;
        } finally {
            if (ignored != null) {
                if (t != null) {
                    try {
                        ignored.close();
                    } catch (Throwable throwable) {
                        t.addSuppressed(throwable);
                    }
                } else {
                    ignored.close();
                }
            }
        }
    }

    private void executeAddToWorker(ScheduleRequestContext ctx) {
        StarException exception = null;
        Iterator iterator = ctx.getWorkerIds().iterator();

        while (iterator.hasNext()) {
            long workerId = (Long) iterator.next();

            try {
                this.executeAddToWorker(ctx.getServiceId(), Collections.nCopies(1, ctx.getShardId()), workerId);
            } catch (StarException starException) {
                exception = starException;
            } catch (Exception e) {
                exception = new StarException(ExceptionCode.SCHEDULE, e.getMessage());
            }
        }

        ctx.done(exception);
    }

    private void executeAddToWorker(String serviceId, List<Long> shardIds, long workerId) {
        ShardManager shardManager = this.serviceManager.getShardManager(serviceId);
        if (shardManager == null) {
            throw new StarException(ExceptionCode.NOT_EXIST,
                    String.format("service %s not exists", serviceId));
        } else {
            Worker worker = this.workerManager.getWorker(workerId);
            if (worker == null) {
                throw new StarException(ExceptionCode.NOT_EXIST,
                        String.format("worker %d not exists", workerId));
            } else if (!worker.getServiceId().equals(serviceId)) {
                throw new StarException(ExceptionCode.INVALID_ARGUMENT,
                        String.format("worker %d doesn't belong to service: %s", workerId, serviceId));
            } else {
                List<List<AddShardInfo>> batches = new ArrayList();
                int remainShardSize = shardIds.size();
                List<AddShardInfo> miniBatch = new ArrayList(
                        Integer.min(remainShardSize,
                                Config.SCHEDULER_MAX_BATCH_ADD_SHARD_SIZE));
                Iterator shardIdsIt = shardIds.iterator();

                while (shardIdsIt.hasNext()) {
                    Long id = (Long) shardIdsIt.next();
                    Shard shard = shardManager.getShard(id);
                    --remainShardSize;
                    if (shard == null) {
                        if (shardIds.size() == 1) {
                            throw new StarException(ExceptionCode.NOT_EXIST,
                                    String.format("shard %d not exists", id));
                        }
                    } else {
                        miniBatch.add(shard.getAddShardInfo());
                        if (miniBatch.size() >= Config.SCHEDULER_MAX_BATCH_ADD_SHARD_SIZE) {
                            batches.add(miniBatch);
                            miniBatch = new ArrayList(
                                    Integer.min(remainShardSize,
                                            Config.SCHEDULER_MAX_BATCH_ADD_SHARD_SIZE));
                        }
                    }
                }

                if (!miniBatch.isEmpty()) {
                    batches.add(miniBatch);
                }

                StarException exception = null;
                Iterator batchesIt = batches.iterator();

                while (batchesIt.hasNext()) {
                    List<AddShardInfo> info = (List) batchesIt.next();
                    AddShardRequest request = AddShardRequest.newBuilder()
                            .setServiceId(serviceId)
                            .setWorkerId(workerId)
                            .addAllShardInfo(info).build();
                    this.addShardOpCounter.inc();
                    if (worker.addShard(request)) {
                        shardManager.addShardReplicas(shardIds, workerId);
                    } else {
                        exception = new StarException(ExceptionCode.SCHEDULE,
                                String.format("Schedule add shard task execution failed serviceId: %s," +
                                                " workerId: %d," +
                                                " shardIds: %s",
                                        serviceId,
                                        workerId,
                                        shardIds));
                    }
                }

                if (exception != null) {
                    throw exception;
                }
            }
        }
    }

    private void executeRemoveFromWorker(String serviceId, List<Long> shardIds, long workerId) {
        ShardManager shardManager = this.serviceManager.getShardManager(serviceId);
        if (shardManager == null) {
            throw new StarException(ExceptionCode.NOT_EXIST,
                    String.format("service %s not exist!", serviceId));
        } else {
            boolean doClean = true;
            Worker worker = this.workerManager.getWorker(workerId);
            if (worker == null) {
                LOG.debug("worker {} not exist when execute remove shards for service {}!",
                        workerId, serviceId);
            } else if (!worker.isAlive()) {
                LOG.debug("worker {} dead when execute remove shards for service {}.",
                        workerId, serviceId);
            } else if (!worker.getServiceId().equals(serviceId)) {
                LOG.debug("worker {} doesn't belong to service {}",
                        workerId, serviceId);
            } else {
                RemoveShardRequest request = RemoveShardRequest.newBuilder()
                        .setServiceId(serviceId)
                        .setWorkerId(workerId)
                        .addAllShardIds(shardIds).build();
                this.removeShardOpCounter.inc();
                doClean = worker.removeShard(request);
            }

            if (doClean) {
                shardManager.removeShardReplicas(shardIds, workerId);
            }

        }
    }

    private void executeRemoveFromGroupPhase1(ScheduleRequestContext ctx) {
        if (!this.isRunning()) {
            ctx.done(new StarException(ExceptionCode.SCHEDULE,
                    "Schedule in shutdown progress"));
        } else {
            try {
                this.executeRemoveFromGroupPhase1Detail(ctx);
            } catch (ScheduleConflictStarException scheduleConflictStarException) {
                this.submitCalcTaskInternal(() -> {
                    this.executeRemoveFromGroupPhase1(ctx);
                }, 100L);
            } catch (StarException starException) {
                ctx.done(starException);
            } catch (Throwable throwable) {
                ctx.done(new StarException(ExceptionCode.SCHEDULE, throwable.getMessage()));
            }
        }
    }

    private void executeRemoveFromGroupPhase1Detail(ScheduleRequestContext ctx) {
        ShardManager shardManager = this.serviceManager.getShardManager(ctx.getServiceId());
        if (shardManager == null) {
            ctx.done(new StarException(ExceptionCode.NOT_EXIST,
                    String.format("Service %s Not Exist", ctx.getServiceId())));
        } else {
            Shard shard = shardManager.getShard(ctx.getShardId());
            if (shard == null) {
                ctx.done(new StarException(ExceptionCode.NOT_EXIST,
                        String.format("Shard %d Not Exist", ctx.getShardId())));
            } else {
                int replicaNum = shard.getExpectedReplicaNum();
                List<Long> healthyWorkerIds = new ArrayList();
                List<Worker> deadWorkers = new ArrayList();
                shard.getReplicaWorkerIds().forEach((id) -> {
                    Worker worker = this.workerManager.getWorker(id);
                    if (worker == null) {
                        this.scheduleAsyncRemoveFromWorker(ctx.getServiceId(),
                                Collections.nCopies(1, ctx.getShardId()), id);
                    } else if (worker.getGroupId() == ctx.getWorkerGroupId()) {
                        if (!worker.isAlive()) {
                            deadWorkers.add(worker);
                        } else {
                            healthyWorkerIds.add(worker.getWorkerId());
                        }
                    }
                });
                if (healthyWorkerIds.size() + deadWorkers.size() <= replicaNum) {
                    LOG.debug("{}, Number of replicas (include dead ones) are less than expected replica." +
                            " Skip it.", ctx);
                    ctx.done();
                } else if (!this.requestLocker.tryLock(ctx, shardManager)) {
                    throw new ScheduleConflictStarException();
                } else {
                    ShardSchedulerV3.DeferOp cleanOp = new ShardSchedulerV3.DeferOp(ctx.getRunnable());
                    Throwable t = null;

                    try {
                        List workerIds;
                        if (!deadWorkers.isEmpty()) {
                            workerIds = Arrays.asList(this.selectOldestWorkerLastSeen(deadWorkers).getWorkerId());
                        } else {
                            ScheduleScorer scorer = new ScheduleScorer(healthyWorkerIds);
                            Iterator groupIdsIt = shard.getGroupIds().iterator();

                            label185:
                            while (true) {
                                ShardGroup group;
                                do {
                                    if (!groupIdsIt.hasNext()) {
                                        scorer.apply(this.workerManager);
                                        LOG.debug("final scores for selection: {}, for request {}",
                                                scorer.getScores(), ctx);
                                        workerIds = scorer.selectLowEnd(this.scoreSelector, 1);
                                        LOG.debug("Final selection for remove-healthy shard, request:{} selection: {}",
                                                ctx, workerIds);
                                        break label185;
                                    }

                                    long groupId = (Long) groupIdsIt.next();
                                    group = shardManager.getShardGroup(groupId);
                                } while (group == null);

                                List<Long> allReplicaWorkerIds = new ArrayList();
                                Iterator shardIdsIt = group.getShardIds().iterator();

                                while (shardIdsIt.hasNext()) {
                                    long sid = (Long) shardIdsIt.next();
                                    if (sid != shard.getShardId()) {
                                        Shard firstDegreeShard = shardManager.getShard(sid);
                                        if (firstDegreeShard != null) {
                                            allReplicaWorkerIds.addAll(firstDegreeShard.getReplicaWorkerIds());
                                        }
                                    }
                                }
                                scorer.apply(group.getPlacementPolicy(), allReplicaWorkerIds);
                            }
                        }

                        Preconditions.checkState((long) workerIds.size() == 1L, "Should only have one replica to remove!");
                        ctx.setWorkerIds(workerIds);
                        LOG.debug("Schedule request {}, pending schedule to workerList: {}", ctx, ctx.getWorkerIds());
                        ShardSchedulerV3.DispatchTask task = this.dispatchTaskForRemoveFromGroup(ctx, ctx.isWaited() ? 10 : 0);

                        try {
                            this.dispatchExecutors.execute(task);
                            cleanOp.cancel();
                        } catch (Throwable throwable) {
                            LOG.error("Fail to add task {} into dispatchWorkerExecutors", task, throwable);
                            ctx.done(new StarException(ExceptionCode.SCHEDULE, throwable.getMessage()));
                        }
                    } catch (Throwable throwable) {
                        t = throwable;
                        throw throwable;
                    } finally {
                        if (cleanOp != null) {
                            if (t != null) {
                                try {
                                    cleanOp.close();
                                } catch (Throwable throwable) {
                                    t.addSuppressed(throwable);
                                }
                            } else {
                                cleanOp.close();
                            }
                        }
                    }
                }
            }
        }
    }

    private Worker selectOldestWorkerLastSeen(List<Worker> workers) {
        Worker targetWorker = (Worker) workers.get(0);
        Iterator workersIt = workers.iterator();

        while (workersIt.hasNext()) {
            Worker worker = (Worker) workersIt.next();
            if (worker.getLastSeenTime() < targetWorker.getLastSeenTime()) {
                targetWorker = worker;
            }
        }
        return targetWorker;
    }

    private void executeRemoveFromGroupPhase2(ScheduleRequestContext ctx) {
        ShardSchedulerV3.DeferOp ignored = new ShardSchedulerV3.DeferOp(ctx.getRunnable());
        Throwable t = null;

        try {
            if (this.isRunning()) {
                Iterator worksIdsIt = ctx.getWorkerIds().iterator();

                while (worksIdsIt.hasNext()) {
                    long workerId = (Long) worksIdsIt.next();
                    this.executeRemoveFromWorker(ctx.getServiceId(),
                            Collections.nCopies(1, ctx.getShardId()), workerId);
                }
            } else {
                ctx.done(new StarException(ExceptionCode.SCHEDULE,
                        "Schedule shutdown in progress"));
            }
        } catch (Throwable throwable) {
            t = throwable;
            throw throwable;
        } finally {
            if (ignored != null) {
                if (t != null) {
                    try {
                        ignored.close();
                    } catch (Throwable throwable) {
                        t.addSuppressed(throwable);
                    }
                } else {
                    ignored.close();
                }
            }
        }
    }

    public boolean isIdle() {
        if (!this.isRunning()) {
            return true;
        } else {
            return this.calculateExecutors.getCompletedTaskCount() == this.calculateExecutors.getTaskCount()
                    && this.dispatchExecutors.getCompletedTaskCount() == this.dispatchExecutors.getTaskCount();
        }
    }

    public void doStart() {
        this.calculateExecutors = new ScheduledThreadPoolExecutor(
                2,
                Utils.namedThreadFactory("scheduler-calc-pool"));
        this.calculateExecutors.setMaximumPoolSize(2);
        this.dispatchExecutors = new ThreadPoolExecutor(
                4,
                4,
                0L,
                TimeUnit.MILLISECONDS,
                new PriorityBlockingQueue(),
                Utils.namedThreadFactory("scheduler-dispatch-pool"));
        this.adjustPoolExecutors = new ScheduledThreadPoolExecutor(1);
        this.adjustPoolExecutors.setMaximumPoolSize(1);
        this.adjustPoolExecutors.scheduleAtFixedRate(
                this::adjustScheduleThreadPoolSize,
                30L,
                300L,
                TimeUnit.SECONDS);
    }

    void adjustScheduleThreadPoolSize() {
        if (this.isRunning()) {
            long totalShards = 0L;
            Iterator serviceIdSetIt = this.serviceManager.getServiceIdSet().iterator();

            while (serviceIdSetIt.hasNext()) {
                String serviceId = (String) serviceIdSetIt.next();
                ShardManager shardManager = this.serviceManager.getShardManager(serviceId);
                if (shardManager != null) {
                    totalShards += (long) shardManager.getShardCount();
                }
            }

            int numOfThreads = estimateNumOfCoreThreads(totalShards);
            if (this.calculateExecutors.getCorePoolSize() != numOfThreads) {
                LOG.info("Total number of shards in memory: {}, " +
                                "adjust scheduler core thread pool size from {} to {}",
                        totalShards,
                        this.calculateExecutors.getCorePoolSize(),
                        numOfThreads);
                if (this.calculateExecutors.getCorePoolSize() < numOfThreads) {
                    this.calculateExecutors.setMaximumPoolSize(numOfThreads);
                    this.calculateExecutors.setCorePoolSize(numOfThreads);
                    this.dispatchExecutors.setMaximumPoolSize(numOfThreads * 2);
                    this.dispatchExecutors.setCorePoolSize(numOfThreads * 2);
                } else {
                    this.calculateExecutors.setCorePoolSize(numOfThreads);
                    this.calculateExecutors.setMaximumPoolSize(numOfThreads);
                    this.dispatchExecutors.setCorePoolSize(numOfThreads * 2);
                    this.dispatchExecutors.setMaximumPoolSize(numOfThreads * 2);
                }

            }
        }
    }

    static int estimateNumOfCoreThreads(long numOfShards) {
        if (numOfShards < 5000L) {
            return 1;
        } else if (numOfShards < 10000L) {
            return 2;
        } else if (numOfShards < 100000L) {
            return 4;
        } else {
            return numOfShards < 1000000L ? 8 : 16;
        }
    }

    public void doStop() {
        Utils.shutdownExecutorService(this.calculateExecutors);
        Utils.shutdownExecutorService(this.dispatchExecutors);
        Utils.shutdownExecutorService(this.adjustPoolExecutors);
    }

    static {
        CONFLICT_POLICIES = Arrays.asList(PlacementPolicy.EXCLUDE, PlacementPolicy.PACK, PlacementPolicy.SPREAD);
        METRIC_WORKER_SHARD_COUNT = MetricsSystem.registerCounter("starmgr_schedule_shard_ops",
                "count of operations by adding/remove shards to/from worker",
                Lists.newArrayList("op"));
    }

    private static class DispatchTask<T> extends FutureTask<T> implements Comparable<ShardSchedulerV3.DispatchTask<T>> {
        private final int priority;
        private final String description;

        public DispatchTask(Runnable runnable, T result, int priority, String description) {
            super(runnable, result);
            this.priority = priority;
            this.description = description;
        }

        public DispatchTask(Callable<T> callable, int priority, String description) {
            super(callable);
            this.priority = priority;
            this.description = description;
        }

        public int compareTo(ShardSchedulerV3.DispatchTask o) {
            return Integer.compare(o.priority, this.priority);
        }

        public String toString() {
            return this.description;
        }
    }

    private static class ExclusiveLocker {
        protected Set<ScheduleRequestContext> exclusiveContexts;
        protected final ReentrantLock exclusiveMapLock;
        protected final Map<Long, Set<Long>> exclusiveShardGroups;

        private ExclusiveLocker() {
            this.exclusiveContexts = ConcurrentHashMap.newKeySet();
            this.exclusiveMapLock = new ReentrantLock();
            this.exclusiveShardGroups = new HashMap();
        }

        public boolean tryLock(ScheduleRequestContext ctx, ShardManager manager) {
            if (this.exclusiveContexts.add(ctx)) {
                if (this.checkAndUpdateExclusiveShardGroups(ctx, manager)) {
                    ctx.setRunnable(() -> {
                        this.tryUnlock(ctx);
                    });
                    return true;
                }

                this.exclusiveContexts.remove(ctx);
            }

            return false;
        }

        private void tryUnlock(ScheduleRequestContext ctx) {
            this.cleanExclusiveShardGroup(ctx);
            this.exclusiveContexts.remove(ctx);
        }

        private void cleanExclusiveShardGroup(ScheduleRequestContext ctx) {
            Collection<Long> exclusiveGroups = ctx.getExclusiveGroupIds();
            if (exclusiveGroups != null && !exclusiveGroups.isEmpty()) {
                LockCloseable ignored = new LockCloseable(this.exclusiveMapLock);
                Throwable t = null;

                try {
                    Collection<Long> groupMarker = this.exclusiveShardGroups.get(ctx.getWorkerGroupId());
                    if (groupMarker != null) {
                        groupMarker.removeAll(exclusiveGroups);
                        if (groupMarker.isEmpty()) {
                            this.exclusiveShardGroups.remove(ctx.getWorkerGroupId());
                        }
                    }
                } catch (Throwable throwable) {
                    t = throwable;
                    throw throwable;
                } finally {
                    if (ignored != null) {
                        if (t != null) {
                            try {
                                ignored.close();
                            } catch (Throwable throwable) {
                                t.addSuppressed(throwable);
                            }
                        } else {
                            ignored.close();
                        }
                    }
                }
            }
        }

        private boolean checkAndUpdateExclusiveShardGroups(ScheduleRequestContext ctx, ShardManager shardManager) {
            Shard shard = shardManager.getShard(ctx.getShardId());
            if (shard == null) {
                return true;
            } else {
                Stream<Long> longStream = shard.getGroupIds().stream();
                shardManager.getClass();
                Set<Long> exclusiveIds = longStream.map(shardManager::getShardGroup).filter((y) -> {
                    return y != null && ShardSchedulerV3.CONFLICT_POLICIES.contains(y.getPlacementPolicy());
                }).map(ShardGroup::getGroupId).collect(Collectors.toSet());
                if (exclusiveIds.isEmpty()) {
                    return true;
                } else {
                    LockCloseable ignored = new LockCloseable(this.exclusiveMapLock);
                    Throwable t = null;

                    try {
                        Set<Long> groupMarker = this.exclusiveShardGroups.get(ctx.getWorkerGroupId());
                        if (groupMarker == null) {
                            groupMarker = new HashSet();
                            this.exclusiveShardGroups.put(ctx.getWorkerGroupId(), groupMarker);
                        } else {
                            longStream = exclusiveIds.stream();
                            groupMarker.getClass();
                            if (longStream.anyMatch(groupMarker::contains)) {
                                ShardSchedulerV3.LOG.debug("Has conflict shardgroup running, retry later. {}", ctx);
                                return false;
                            }
                        }

                        groupMarker.addAll(exclusiveIds);
                        ctx.setExclusiveGroupIds(exclusiveIds);
                    } catch (Throwable throwable) {
                        t = throwable;
                        throw throwable;
                    } finally {
                        if (ignored != null) {
                            if (t != null) {
                                try {
                                    ignored.close();
                                } catch (Throwable throwable) {
                                    t.addSuppressed(throwable);
                                }
                            } else {
                                ignored.close();
                            }
                        }
                    }
                    return true;
                }
            }
        }
    }

    private static class DeferOp implements Closeable {
        private final Runnable runnable;
        private boolean done;

        public DeferOp(Runnable runnable) {
            this.runnable = runnable;
            this.done = false;
        }

        public void cancel() {
            this.done = true;
        }

        public void close() {
            if (!this.done) {
                this.done = true;
                this.runnable.run();
            }
        }
    }
}
