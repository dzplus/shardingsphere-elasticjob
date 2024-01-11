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

package org.apache.shardingsphere.elasticjob.kernel.internal.election;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.shardingsphere.elasticjob.kernel.internal.schedule.JobRegistry;
import org.apache.shardingsphere.elasticjob.kernel.internal.server.ServerService;
import org.apache.shardingsphere.elasticjob.kernel.internal.storage.JobNodeStorage;
import org.apache.shardingsphere.elasticjob.reg.base.LeaderExecutionCallback;
import org.apache.shardingsphere.elasticjob.reg.base.CoordinatorRegistryCenter;
import org.apache.shardingsphere.elasticjob.kernel.infra.util.BlockUtils;

/**
 * Leader service.
 */
@Slf4j
public final class LeaderService {

    private final String jobName;

    private final ServerService serverService;

    private final JobNodeStorage jobNodeStorage;

    public LeaderService(final CoordinatorRegistryCenter regCenter, final String jobName) {
        this.jobName = jobName;
        jobNodeStorage = new JobNodeStorage(regCenter, jobName);
        serverService = new ServerService(regCenter, jobName);
    }

    /**
     * Elect leader.
     */
    public void electLeader() {
        log.info("Elect a new leader now.");
        jobNodeStorage.executeInLeader(LeaderNode.LATCH, new LeaderElectionExecutionCallback());
        log.info("Leader election completed.");
    }

    /**
     * Judge current server is leader or not.
     *
     * <p>
     * If leader is electing, this method will block until leader elected success.
     * </p>
     *
     * @return current server is leader or not
     */
    public boolean isLeaderUntilBlock() {
        String serverIp = JobRegistry.getInstance().getJobInstance(jobName).getServerIp();
        //如果不存在leader 且存在可用节点 那就触发选举
        while (!hasLeader() && serverService.hasAvailableServers()) {
            log.info("准备选举中，等待 {} ms", 100);
            BlockUtils.waitingShortTime();
            log.info("检查选举状态, jobName: '{}',ServerIP:{}", jobName, serverIp);
            if (!JobRegistry.getInstance().isShutdown(jobName) && serverService.isAvailableServer(serverIp)) {
                log.info("触发选举流程, jobName: '{}',ServerIP:{}", jobName, serverIp);
                electLeader();
            }
        }
        boolean leader = isLeader();
        log.info("当前本机{}是否是leader:{}", serverIp, leader);
        return leader;
    }

    /**
     * Judge current server is leader or not.
     *
     * @return current server is leader or not
     */
    public boolean isLeader() {
        return !JobRegistry.getInstance().isShutdown(jobName) && JobRegistry.getInstance().getJobInstance(jobName).getJobInstanceId().equals(jobNodeStorage.getJobNodeData(LeaderNode.INSTANCE));
    }

    /**
     * Judge has leader or not in current time.
     *
     * @return has leader or not in current time
     */
    public boolean hasLeader() {
        return jobNodeStorage.isJobNodeExisted(LeaderNode.INSTANCE);
    }

    /**
     * Remove leader and trigger leader election.
     */
    public void removeLeader() {
        jobNodeStorage.removeJobNodeIfExisted(LeaderNode.INSTANCE);
    }

    @RequiredArgsConstructor
    class LeaderElectionExecutionCallback implements LeaderExecutionCallback {

        @Override
        public void execute() {
            //如果没有leader
            if (!hasLeader()) {
                //填充临时节点到指定路径上
                jobNodeStorage.fillEphemeralJobNode(LeaderNode.INSTANCE, JobRegistry.getInstance().getJobInstance(jobName).getJobInstanceId());
            }
        }
    }
}
