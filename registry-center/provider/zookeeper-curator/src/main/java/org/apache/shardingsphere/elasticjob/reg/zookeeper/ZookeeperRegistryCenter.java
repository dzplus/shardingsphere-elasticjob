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

package org.apache.shardingsphere.elasticjob.reg.zookeeper;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.api.ACLProvider;
import org.apache.curator.framework.api.transaction.CuratorOp;
import org.apache.curator.framework.api.transaction.TransactionOp;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.framework.recipes.cache.CuratorCache;
import org.apache.curator.framework.recipes.cache.CuratorCacheListener;
import org.apache.curator.framework.recipes.leader.LeaderLatch;
import org.apache.curator.framework.state.ConnectionStateListener;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.utils.CloseableUtils;
import org.apache.shardingsphere.elasticjob.reg.base.CoordinatorRegistryCenter;
import org.apache.shardingsphere.elasticjob.reg.base.LeaderExecutionCallback;
import org.apache.shardingsphere.elasticjob.reg.base.transaction.TransactionOperation;
import org.apache.shardingsphere.elasticjob.reg.exception.RegException;
import org.apache.shardingsphere.elasticjob.reg.exception.RegExceptionHandler;
import org.apache.shardingsphere.elasticjob.reg.listener.ConnectionStateChangedEventListener;
import org.apache.shardingsphere.elasticjob.reg.listener.ConnectionStateChangedEventListener.State;
import org.apache.shardingsphere.elasticjob.reg.listener.DataChangedEvent;
import org.apache.shardingsphere.elasticjob.reg.listener.DataChangedEvent.Type;
import org.apache.shardingsphere.elasticjob.reg.listener.DataChangedEventListener;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Stat;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;

/**
 * Registry center of ZooKeeper.
 */
@Slf4j
public final class ZookeeperRegistryCenter implements CoordinatorRegistryCenter {

    @Getter(AccessLevel.PROTECTED)
    private final ZookeeperConfiguration zkConfig;

    private final Map<String, CuratorCache> caches = new ConcurrentHashMap<>();

    /**
     * Data listener list.
     */
    private final Map<String, List<CuratorCacheListener>> dataListeners = new ConcurrentHashMap<>();

    /**
     * Connections state listener list.
     */
    private final Map<String, List<ConnectionStateListener>> connStateListeners = new ConcurrentHashMap<>();

    /**
     * CuratorFramework是Netflix公司开源的一套Zookeeper客户端框架，它作为一款优秀的ZooKeeper客户端开源工具，
     * 主要提供了对客户端到服务的连接管理和连接重试机制，以及一些扩展功能，它解决了很多ZooKeeper客户端非常底层的细节开发工作
     */
    @Getter
    private CuratorFramework client;

    public ZookeeperRegistryCenter(final ZookeeperConfiguration zkConfig) {
        this.zkConfig = zkConfig;
    }

    @Override
    public void init() {
        log.info("Elastic job: zookeeper registry center init, server lists is: {}.", zkConfig.getServerLists());
        CuratorFrameworkFactory.Builder builder = CuratorFrameworkFactory.builder()
                .connectString(zkConfig.getServerLists())
                .retryPolicy(new ExponentialBackoffRetry(zkConfig.getBaseSleepTimeMilliseconds(), zkConfig.getMaxRetries(), zkConfig.getMaxSleepTimeMilliseconds()))
                .namespace(zkConfig.getNamespace());
        if (0 != zkConfig.getSessionTimeoutMilliseconds()) {
            builder.sessionTimeoutMs(zkConfig.getSessionTimeoutMilliseconds());
        }
        if (0 != zkConfig.getConnectionTimeoutMilliseconds()) {
            builder.connectionTimeoutMs(zkConfig.getConnectionTimeoutMilliseconds());
        }
        if (!Strings.isNullOrEmpty(zkConfig.getDigest())) {
            builder.authorization("digest", zkConfig.getDigest().getBytes(StandardCharsets.UTF_8))
                    .aclProvider(new ACLProvider() {

                        @Override
                        public List<ACL> getDefaultAcl() {
                            return ZooDefs.Ids.CREATOR_ALL_ACL;
                        }

                        @Override
                        public List<ACL> getAclForPath(final String path) {
                            return ZooDefs.Ids.CREATOR_ALL_ACL;
                        }
                    });
        }
        client = builder.build();
        client.start();
        try {
            if (!client.blockUntilConnected(zkConfig.getMaxSleepTimeMilliseconds() * zkConfig.getMaxRetries(), TimeUnit.MILLISECONDS)) {
                client.close();
                throw new KeeperException.OperationTimeoutException();
            }
            // CHECKSTYLE:OFF
        } catch (final Exception ex) {
            // CHECKSTYLE:ON
            RegExceptionHandler.handleException(ex);
        }
    }

    @Override
    public void close() {
        for (Entry<String, CuratorCache> each : caches.entrySet()) {
            each.getValue().close();
        }
        waitForCacheClose();
        CloseableUtils.closeQuietly(client);
    }

    /*
     * // TODO sleep 500ms, let cache client close first and then client, otherwise will throw exception reference：https://issues.apache.org/jira/browse/CURATOR-157
     */
    private void waitForCacheClose() {
        try {
            Thread.sleep(500L);
        } catch (final InterruptedException ex) {
            Thread.currentThread().interrupt();
        }
    }

    @Override
    public String get(final String key) {
        CuratorCache cache = findCuratorCache(key);
        if (null == cache) {
            return getDirectly(key);
        }
        Optional<ChildData> resultInCache = cache.get(key);
        return resultInCache.map(v -> null == v.getData() ? null : new String(v.getData(), StandardCharsets.UTF_8)).orElseGet(() -> getDirectly(key));
    }

    private CuratorCache findCuratorCache(final String key) {
        log.info("从findCuratorCache获取配置:{}",key);
        for (Entry<String, CuratorCache> entry : caches.entrySet()) {
            if (key.startsWith(entry.getKey())) {
                log.info("从findCuratorCache获取配置:{}",entry.getValue());
                return entry.getValue();
            }
        }
        log.info("从findCuratorCache获取配置:{}","未取到");
        return null;
    }

    @Override
    public String getDirectly(final String key) {
        try {
            log.info("从ZK获取配置getDirectly:{}",key);
            return new String(client.getData().forPath(key), StandardCharsets.UTF_8);
            // CHECKSTYLE:OFF
        } catch (final Exception ex) {
            // CHECKSTYLE:ON
            RegExceptionHandler.handleException(ex);
            return null;
        }
    }

    @Override
    public List<String> getChildrenKeys(final String key) {
        try {
            List<String> result = client.getChildren().forPath(key);
            result.sort(Comparator.reverseOrder());
            return result;
            // CHECKSTYLE:OFF
        } catch (final Exception ex) {
            // CHECKSTYLE:ON
            RegExceptionHandler.handleException(ex);
            return Collections.emptyList();
        }
    }

    @Override
    public int getNumChildren(final String key) {
        try {
            Stat stat = client.checkExists().forPath(key);
            if (null != stat) {
                return stat.getNumChildren();
            }
            // CHECKSTYLE:OFF
        } catch (final Exception ex) {
            // CHECKSTYLE:ON
            RegExceptionHandler.handleException(ex);
        }
        return 0;
    }

    @Override
    public boolean isExisted(final String key) {
        try {
            return null != client.checkExists().forPath(key);
            // CHECKSTYLE:OFF
        } catch (final Exception ex) {
            // CHECKSTYLE:ON
            RegExceptionHandler.handleException(ex);
            return false;
        }
    }

    @Override
    public void persist(final String key, final String value) {
        log.info("存储信息到ZK,{},{}",key,value);
        try {
            if (!isExisted(key)) {
                client.create().creatingParentsIfNeeded().withMode(CreateMode.PERSISTENT).forPath(key, value.getBytes(StandardCharsets.UTF_8));
            } else {
                update(key, value);
            }
            // CHECKSTYLE:OFF
        } catch (final Exception ex) {
            // CHECKSTYLE:ON
            RegExceptionHandler.handleException(ex);
        }
    }

    @Override
    public void update(final String key, final String value) {
        log.info("更新信息到ZK,{},{}",key,value);
        try {
            TransactionOp transactionOp = client.transactionOp();
            client.transaction().forOperations(transactionOp.check().forPath(key), transactionOp.setData().forPath(key, value.getBytes(StandardCharsets.UTF_8)));
            // CHECKSTYLE:OFF
        } catch (final Exception ex) {
            // CHECKSTYLE:ON
            RegExceptionHandler.handleException(ex);
        }
    }

    @Override
    public void persistEphemeral(final String key, final String value) {
        log.info("添加一个临时节点到ZK,{},{}",key,value);
        try {
            if (isExisted(key)) {
                client.delete().deletingChildrenIfNeeded().forPath(key);
            }
            client.create().creatingParentsIfNeeded().withMode(CreateMode.EPHEMERAL).forPath(key, value.getBytes(StandardCharsets.UTF_8));
            // CHECKSTYLE:OFF
        } catch (final Exception ex) {
            // CHECKSTYLE:ON
            RegExceptionHandler.handleException(ex);
        }
    }

    @Override
    public String persistSequential(final String key, final String value) {
        log.info("persistSequential到ZK,{},{}",key,value);
        try {
            return client.create().creatingParentsIfNeeded().withMode(CreateMode.PERSISTENT_SEQUENTIAL).forPath(key, value.getBytes(StandardCharsets.UTF_8));
            // CHECKSTYLE:OFF
        } catch (final Exception ex) {
            // CHECKSTYLE:ON
            RegExceptionHandler.handleException(ex);
        }
        return null;
    }

    @Override
    public void persistEphemeralSequential(final String key) {
        log.info("persistEphemeralSequential到ZK,{}",key);
        try {
            client.create().creatingParentsIfNeeded().withMode(CreateMode.EPHEMERAL_SEQUENTIAL).forPath(key);
            // CHECKSTYLE:OFF
        } catch (final Exception ex) {
            // CHECKSTYLE:ON
            RegExceptionHandler.handleException(ex);
        }
    }

    @Override
    public void remove(final String key) {
        log.info("移除一个节点到ZK,{}",key);
        try {
            client.delete().deletingChildrenIfNeeded().forPath(key);
            // CHECKSTYLE:OFF
        } catch (final Exception ex) {
            // CHECKSTYLE:ON
            RegExceptionHandler.handleException(ex);
        }
    }

    @Override
    public long getRegistryCenterTime(final String key) {
        log.info("getRegistryCenterTime到ZK,{}",key);
        long result = 0L;
        try {
            persist(key, "");
            result = client.checkExists().forPath(key).getMtime();
            // CHECKSTYLE:OFF
        } catch (final Exception ex) {
            // CHECKSTYLE:ON
            RegExceptionHandler.handleException(ex);
        }
        Preconditions.checkState(0L != result, "Cannot get registry center time.");
        return result;
    }

    @Override
    public Object getRawClient() {
        return client;
    }

    @Override
    public void addConnectionStateChangedEventListener(final String key,
                                                       final ConnectionStateChangedEventListener listener) {
        log.info("addConnectionStateChangedEventListener到ZK,{}",key);
        CoordinatorRegistryCenter coordinatorRegistryCenter = this;
        ConnectionStateListener connStateListener = (client, newState) -> {
            State state;
            switch (newState) {
                case CONNECTED:
                    state = State.CONNECTED;
                    break;
                case LOST:
                case SUSPENDED:
                    state = State.UNAVAILABLE;
                    break;
                case RECONNECTED:
                    state = State.RECONNECTED;
                    break;
                case READ_ONLY:
                default:
                    throw new IllegalStateException("Illegal registry center connection state: " + newState);
            }
            listener.onStateChanged(coordinatorRegistryCenter, state);
        };
        client.getConnectionStateListenable().addListener(connStateListener);
        //比较后缺少的添加进list
        connStateListeners.computeIfAbsent(key, k -> new LinkedList<>()).add(connStateListener);
    }

    @Override
    public void executeInTransaction(final List<TransactionOperation> transactionOperations) throws Exception {
        client.transaction().forOperations(toCuratorOps(transactionOperations));
    }

    private List<CuratorOp> toCuratorOps(final List<TransactionOperation> transactionOperations) {
        List<CuratorOp> result = new ArrayList<>(transactionOperations.size());
        TransactionOp transactionOp = client.transactionOp();
        for (TransactionOperation each : transactionOperations) {
            result.add(toCuratorOp(each, transactionOp));
        }
        return result;
    }

    private CuratorOp toCuratorOp(final TransactionOperation each, final TransactionOp transactionOp) {
        try {
            switch (each.getType()) {
                case CHECK_EXISTS:
                    return transactionOp.check().forPath(each.getKey());
                case ADD:
                    return transactionOp.create().forPath(each.getKey(), each.getValue().getBytes(StandardCharsets.UTF_8));
                case UPDATE:
                    return transactionOp.setData().forPath(each.getKey(), each.getValue().getBytes(StandardCharsets.UTF_8));
                case DELETE:
                    return transactionOp.delete().forPath(each.getKey());
                default:
                    throw new UnsupportedOperationException(each.toString());
            }
            // CHECKSTYLE:OFF
        } catch (final Exception ex) {
            // CHECKSTYLE:ON
            throw new RegException(ex);
        }
    }

    @Override
    public void addCacheData(final String cachePath) {
        CuratorCache cache = CuratorCache.build(client, cachePath);
        try {
            cache.start();
            // CHECKSTYLE:OFF
        } catch (final Exception ex) {
            // CHECKSTYLE:ON
            RegExceptionHandler.handleException(ex);
        }
        caches.put(cachePath + "/", cache);
    }

    @Override
    public void evictCacheData(final String cachePath) {
        CuratorCache cache = caches.remove(cachePath + "/");
        if (null != cache) {
            cache.close();
        }
    }

    @Override
    public Object getRawCache(final String cachePath) {
        return caches.get(cachePath + "/");
    }

    @Override
    public void executeInLeader(final String key, final LeaderExecutionCallback callback) {
        try (LeaderLatch latch = new LeaderLatch(client, key)) {
            latch.start();
            latch.await();
            callback.execute();
            // CHECKSTYLE:OFF
        } catch (final Exception ex) {
            // CHECKSTYLE:ON
            handleException(ex);
        }
    }

    @Override
    public void watch(final String key, final DataChangedEventListener listener, final Executor executor) {
        log.info("ZK-watch:{}", key + "/");
        CuratorCache cache = caches.get(key + "/");
        CuratorCacheListener cacheListener = (curatorType, oldData, newData) -> {
            if (null == newData && null == oldData) {
                return;
            }
            Type type = getTypeFromCuratorType(curatorType);
            String path = Type.DELETED == type ? oldData.getPath() : newData.getPath();
            if (path.isEmpty() || Type.IGNORED == type) {
                return;
            }
            byte[] data = Type.DELETED == type ? oldData.getData() : newData.getData();
            listener.onChange(new DataChangedEvent(type, path, null == data ? "" : new String(data, StandardCharsets.UTF_8)));
        };
        if (executor != null) {
            cache.listenable().addListener(cacheListener, executor);
        } else {
            cache.listenable().addListener(cacheListener);
        }
        dataListeners.computeIfAbsent(key, k -> new LinkedList<>()).add(cacheListener);
        log.info("ZK-dataListeners:{}", dataListeners);
    }

    @Override
    public void removeDataListeners(final String key) {
        final CuratorCache cache = caches.get(key + "/");
        if (Objects.isNull(cache)) {
            dataListeners.remove(key);
            return;
        }
        List<CuratorCacheListener> cacheListenerList = dataListeners.remove(key);
        if (Objects.isNull(cacheListenerList)) {
            return;
        }
        cacheListenerList.forEach(listener -> cache.listenable().removeListener(listener));
    }

    @Override
    public void removeConnStateListener(final String key) {
        final List<ConnectionStateListener> listenerList = connStateListeners.remove(key);
        if (Objects.isNull(listenerList)) {
            return;
        }
        listenerList.forEach(listener -> client.getConnectionStateListenable().removeListener(listener));
    }

    private Type getTypeFromCuratorType(final CuratorCacheListener.Type curatorType) {
        switch (curatorType) {
            case NODE_CREATED:
                return Type.ADDED;
            case NODE_DELETED:
                return Type.DELETED;
            case NODE_CHANGED:
                return Type.UPDATED;
            default:
                return Type.IGNORED;
        }
    }

    private void handleException(final Exception ex) {
        if (ex instanceof InterruptedException) {
            Thread.currentThread().interrupt();
        } else {
            throw new RegException(ex);
        }
    }
}
