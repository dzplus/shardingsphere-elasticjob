/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.shardingsphere.elasticjob.kernel.internal.reconcile;

import com.google.common.util.concurrent.AbstractScheduledService;
import lombok.extern.slf4j.Slf4j;
import org.apache.shardingsphere.elasticjob.kernel.internal.config.ConfigurationService;
import org.apache.shardingsphere.elasticjob.kernel.internal.sharding.ShardingService;
import org.apache.shardingsphere.elasticjob.kernel.internal.storage.JobNodePath;
import org.apache.shardingsphere.elasticjob.reg.base.CoordinatorRegistryCenter;

import java.util.concurrent.TimeUnit;

/**
 * Reconcile service.
 * 重协调管理器 每分钟执行一次
 * 在分布式的场景下由于网络、时钟等原因，可能导致 ZooKeeper 的数据与真实运行的作业产生不一致，这种不一致通过正向的校验无法完全避免。 需要另外启动一个线程定时校验注册中心数据与真实作业状态的一致性，即维持 ElasticJob 的最终一致性。
 *
 * 配置为小于 1 的任意值表示不执行修复。
 *
 * PS 解释一下这个服务 这个服务用的是guava框架中的service
 * Service框架可以帮助我们把异步操作封装成一个Service服务。
 * 让这个服务有了运行状态(我们也可以理解成生命周期)，这样我们可以实时了解当前服务的运行状态。同时我们还可以添加监听器来监听服务运行状态之间的变化。
 */
@Slf4j
public final class ReconcileService extends AbstractScheduledService {
    
    private long lastReconcileTime;
    
    private final ConfigurationService configService;
    
    private final ShardingService shardingService;
    
    private final JobNodePath jobNodePath;
    
    private final CoordinatorRegistryCenter regCenter;
    
    public ReconcileService(final CoordinatorRegistryCenter regCenter, final String jobName) {
        this.regCenter = regCenter;
        lastReconcileTime = System.currentTimeMillis();
        configService = new ConfigurationService(regCenter, jobName);
        shardingService = new ShardingService(regCenter, jobName);
        jobNodePath = new JobNodePath(jobName);
    }
    
    @Override
    protected void runOneIteration() {
        log.info("runOneIteration执行,尝试重新协调执行");
        int reconcileIntervalMinutes = configService.load(true).getReconcileIntervalMinutes();
        //如果协调时间间隔大于0，并且当前时间减去上次协调时间大于协调时间间隔，则开始协调
        if (reconcileIntervalMinutes > 0 && System.currentTimeMillis() - lastReconcileTime >= (long) reconcileIntervalMinutes * 60 * 1000) {
            lastReconcileTime = System.currentTimeMillis();
            //这个判断 1、当前状态不需要分片 2、离线服务商存在分片信息 3、不是静态分片或者没有分片信息
            if (!shardingService.isNeedSharding() && shardingService.hasShardingInfoInOfflineServers() && !(isStaticSharding() && hasShardingInfo())) {
                log.warn("Elastic Job: 作业状态节点值不一致，开始协调...");
                shardingService.setReshardingFlag();
            }
        }
    }
    
    private boolean isStaticSharding() {
        return configService.load(true).isStaticSharding();
    }
    
    private boolean hasShardingInfo() {
        return !regCenter.getChildrenKeys(jobNodePath.getShardingNodePath()).isEmpty();
    }
    
    @Override
    protected Scheduler scheduler() {
        return Scheduler.newFixedDelaySchedule(0, 1, TimeUnit.MINUTES);
    }
}
