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

package org.apache.shardingsphere.elasticjob.kernel.internal.storage;

import com.google.gson.Gson;
import lombok.extern.slf4j.Slf4j;
import org.apache.shardingsphere.elasticjob.kernel.internal.listener.ListenerNotifierManager;
import org.apache.shardingsphere.elasticjob.reg.base.CoordinatorRegistryCenter;
import org.apache.shardingsphere.elasticjob.reg.base.LeaderExecutionCallback;
import org.apache.shardingsphere.elasticjob.reg.base.transaction.TransactionOperation;
import org.apache.shardingsphere.elasticjob.reg.exception.RegExceptionHandler;
import org.apache.shardingsphere.elasticjob.reg.listener.ConnectionStateChangedEventListener;
import org.apache.shardingsphere.elasticjob.reg.listener.DataChangedEventListener;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executor;

/**
 * Job node storage.
 * 任务节点存储
 */
@Slf4j
public final class JobNodeStorage {

    private final CoordinatorRegistryCenter regCenter;

    private final String jobName;

    private final JobNodePath jobNodePath;

    public JobNodeStorage(final CoordinatorRegistryCenter regCenter, final String jobName) {
        this.regCenter = regCenter;
        this.jobName = jobName;
        jobNodePath = new JobNodePath(jobName);
    }

    /**
     * Judge is job node existed or not.
     *
     * @param node node
     * @return is job node existed or not
     */
    public boolean isJobNodeExisted(final String node) {
        return regCenter.isExisted(jobNodePath.getFullPath(node));
    }

    /**
     * Judge is job root node existed or not.
     *
     * @return is job root node existed or not
     */
    public boolean isJobRootNodeExisted() {
        return regCenter.isExisted("/" + jobName);
    }

    /**
     * Get job node data.
     *
     * @param node node
     * @return data of job node
     */
    public String getJobNodeData(final String node) {
        String fullPath = jobNodePath.getFullPath(node);
        String s = regCenter.get(fullPath);
        log.info("fullPath:{},value:{},从本地缓存中拉取", fullPath, s);
        return s;
    }

    /**
     * Get job node data from registry center directly.
     *
     * @param node node
     * @return data of job node
     */
    public String getJobNodeDataDirectly(final String node) {
        String directly = regCenter.getDirectly(jobNodePath.getFullPath(node));
        log.info("{},直接从ZK上拉取", directly);
        return directly;
    }

    /**
     * Get job node children keys.
     *
     * @param node node
     * @return children keys
     */
    public List<String> getJobNodeChildrenKeys(final String node) {
        return regCenter.getChildrenKeys(jobNodePath.getFullPath(node));
    }

    /**
     * Get job root node data.
     *
     * @return data of job node
     */
    public String getJobRootNodeData() {
        return regCenter.get("/" + jobName);
    }

    /**
     * Create job node if needed.
     *
     * <p>Do not create node if root node not existed, which means job is shutdown.</p>
     *
     * @param node node
     */
    public void createJobNodeIfNeeded(final String node) {
        if (isJobRootNodeExisted() && !isJobNodeExisted(node)) {
            regCenter.persist(jobNodePath.getFullPath(node), "");
        }
    }

    /**
     * Remove job node if existed.
     *
     * @param node node
     */
    public void removeJobNodeIfExisted(final String node) {
        if (isJobNodeExisted(node)) {
            regCenter.remove(jobNodePath.getFullPath(node));
        }
    }

    /**
     * Fill job node.
     *
     * @param node  node
     * @param value data of job node
     */
    public void fillJobNode(final String node, final Object value) {
        regCenter.persist(jobNodePath.getFullPath(node), value.toString());
    }

    /**
     * Fill ephemeral job node.
     *
     * @param node  node
     * @param value data of job node
     */
    public void fillEphemeralJobNode(final String node, final Object value) {
        log.info("添加临时作业节点到ZK:{},{}", jobNodePath.getFullPath(node), value.toString());
        regCenter.persistEphemeral(jobNodePath.getFullPath(node), value.toString());
    }

    /**
     * Update job node.
     *
     * @param node  node
     * @param value data of job node
     */
    public void updateJobNode(final String node, final Object value) {
        regCenter.update(jobNodePath.getFullPath(node), value.toString());
    }

    /**
     * Replace data.
     *
     * @param node  node
     * @param value to be replaced data
     */
    public void replaceJobNode(final String node, final Object value) {
        String fullPath = jobNodePath.getFullPath(node);
        String string = value.toString();
        log.info("replaceJobNode:{},{}", fullPath, string);
        regCenter.persist(fullPath, string);
    }

    /**
     * Replace data to root node.
     *
     * @param value to be replaced data
     */
    public void replaceJobRootNode(final Object value) {
        String key = "/" + jobName;
        String string = value.toString();
        log.info("replaceJobRootNode:{},{}", key, string);
        regCenter.persist(key, string);
    }

    /**
     * Execute operations in transaction.
     *
     * @param transactionOperations operations to be executed in transaction
     */
    public void executeInTransaction(final List<TransactionOperation> transactionOperations) {
        List<TransactionOperation> result = new ArrayList<>(transactionOperations.size() + 1);
        result.add(TransactionOperation.opCheckExists("/"));
        result.addAll(transactionOperations);
        try {
            log.info("executeInTransactionS:{}", new Gson().toJson(result));
            //这个地方就是去处理ZK的节点信息
            regCenter.executeInTransaction(result);
            // CHECKSTYLE:OFF
        } catch (final Exception ex) {
            // CHECKSTYLE:ON
            RegExceptionHandler.handleException(ex);
        }
    }

    /**
     * Execute in leader server.
     *
     * @param latchNode node for leader latch
     * @param callback  execute callback
     */
    public void executeInLeader(final String latchNode, final LeaderExecutionCallback callback) {
        regCenter.executeInLeader(jobNodePath.getFullPath(latchNode), callback);
    }

    /**
     * Add connection state listener.
     *
     * @param listener connection state listener
     */
    public void addConnectionStateListener(final ConnectionStateChangedEventListener listener) {
        regCenter.addConnectionStateChangedEventListener("/" + jobName, listener);
    }

    /**
     * Add data listener.
     *
     * @param listener data listener
     */
    public void addDataListener(final DataChangedEventListener listener) {
        Executor executor = ListenerNotifierManager.getInstance().getJobNotifyExecutor(jobName);
        regCenter.watch("/" + jobName, listener, executor);
    }

    /**
     * Get registry center time.
     *
     * @return registry center time
     */
    public long getRegistryCenterTime() {
        return regCenter.getRegistryCenterTime(jobNodePath.getFullPath("systemTime/current"));
    }
}
