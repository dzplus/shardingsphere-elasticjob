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

package org.apache.shardingsphere.elasticjob.kernel.executor;

import com.google.gson.Gson;
import lombok.extern.slf4j.Slf4j;
import org.apache.shardingsphere.elasticjob.api.ElasticJob;
import org.apache.shardingsphere.elasticjob.api.JobConfiguration;
import org.apache.shardingsphere.elasticjob.spi.executor.error.handler.JobErrorHandler;
import org.apache.shardingsphere.elasticjob.kernel.executor.error.handler.JobErrorHandlerReloader;
import org.apache.shardingsphere.elasticjob.kernel.executor.facade.JobFacade;
import org.apache.shardingsphere.elasticjob.spi.executor.item.JobItemExecutor;
import org.apache.shardingsphere.elasticjob.kernel.executor.item.JobItemExecutorFactory;
import org.apache.shardingsphere.elasticjob.spi.executor.item.type.TypedJobItemExecutor;
import org.apache.shardingsphere.elasticjob.kernel.executor.threadpool.ExecutorServiceReloader;
import org.apache.shardingsphere.elasticjob.kernel.infra.env.IpUtils;
import org.apache.shardingsphere.elasticjob.kernel.infra.exception.ExceptionUtils;
import org.apache.shardingsphere.elasticjob.kernel.infra.exception.JobExecutionEnvironmentException;
import org.apache.shardingsphere.elasticjob.spi.listener.param.ShardingContexts;
import org.apache.shardingsphere.elasticjob.spi.tracing.event.JobExecutionEvent;
import org.apache.shardingsphere.elasticjob.spi.tracing.event.JobExecutionEvent.ExecutionSource;
import org.apache.shardingsphere.elasticjob.spi.tracing.event.JobStatusTraceEvent.State;
import org.apache.shardingsphere.infra.spi.type.typed.TypedSPILoader;

import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;

/**
 * ElasticJob executor.
 */
@Slf4j
public final class ElasticJobExecutor {

    private final ElasticJob elasticJob;

    private final JobFacade jobFacade;

    @SuppressWarnings("rawtypes")
    private final JobItemExecutor jobItemExecutor;

    private final ExecutorServiceReloader executorServiceReloader;

    private final JobErrorHandlerReloader jobErrorHandlerReloader;

    private final Map<Integer, String> itemErrorMessages;

    public ElasticJobExecutor(final ElasticJob elasticJob, final JobConfiguration jobConfig, final JobFacade jobFacade) {
        this(elasticJob, jobConfig, jobFacade, JobItemExecutorFactory.getExecutor(elasticJob.getClass()));
    }

    public ElasticJobExecutor(final String type, final JobConfiguration jobConfig, final JobFacade jobFacade) {
        this(null, jobConfig, jobFacade, TypedSPILoader.getService(TypedJobItemExecutor.class, type));
    }

    private ElasticJobExecutor(final ElasticJob elasticJob, final JobConfiguration jobConfig, final JobFacade jobFacade, final JobItemExecutor jobItemExecutor) {
        this.elasticJob = elasticJob;
        this.jobFacade = jobFacade;
        this.jobItemExecutor = jobItemExecutor;
        JobConfiguration loadedJobConfig = jobFacade.loadJobConfiguration(true);
        executorServiceReloader = new ExecutorServiceReloader(loadedJobConfig);
        jobErrorHandlerReloader = new JobErrorHandlerReloader(loadedJobConfig);
        itemErrorMessages = new ConcurrentHashMap<>(jobConfig.getShardingTotalCount(), 1);
    }

    /**
     * Execute job.
     */
    public void execute() {
        log.info("执行任务");
        JobConfiguration jobConfig = jobFacade.loadJobConfiguration(true);
        //这是判断是不是要分片
        executorServiceReloader.reloadIfNecessary(jobConfig);
        //这个是看有没有异常处理器
        jobErrorHandlerReloader.reloadIfNecessary(jobConfig);
        JobErrorHandler jobErrorHandler = jobErrorHandlerReloader.getJobErrorHandler();
        //对表
        try {
            jobFacade.checkJobExecutionEnvironment();
        } catch (final JobExecutionEnvironmentException cause) {
            jobErrorHandler.handleException(jobConfig.getJobName(), cause);
        }
        //获取分片上下文的时候才想起来要分片
        ShardingContexts shardingContexts = jobFacade.getShardingContexts();
        //执行前消息
        log.info("当前执行的任务是:{}",new Gson().toJson(shardingContexts));
        jobFacade.postJobStatusTraceEvent(shardingContexts.getTaskId(), State.TASK_STAGING, String.format("Job '%s' execute begin.", jobConfig.getJobName()));
        //检查是否有上一轮任务错过重执行任务正在执行 如果有就将本轮任务添加到错过重执行标记位然后退出
        if (jobFacade.misfireIfRunning(shardingContexts.getShardingItemParameters().keySet())) {
            //如果有就不执行本轮任务
            jobFacade.postJobStatusTraceEvent(shardingContexts.getTaskId(), State.TASK_FINISHED, String.format(
                    "Previous job '%s' - shardingItems '%s' is still running, misfired job will start after previous job completed.", jobConfig.getJobName(),
                    shardingContexts.getShardingItemParameters().keySet()));
            return;
        }
        try {
            jobFacade.beforeJobExecuted(shardingContexts);
            // CHECKSTYLE:OFF
        } catch (final Throwable cause) {
            // CHECKSTYLE:ON
            jobErrorHandler.handleException(jobConfig.getJobName(), cause);
        }
        //执行任务
        execute(jobConfig, shardingContexts, ExecutionSource.NORMAL_TRIGGER);
        //执行完自己的分片后 检查错过任务重执行
        while (jobFacade.isExecuteMisfired(shardingContexts.getShardingItemParameters().keySet())) {
            //清除错过重执行标记
            jobFacade.clearMisfire(shardingContexts.getShardingItemParameters().keySet());
            //执行任务
            execute(jobConfig, shardingContexts, ExecutionSource.MISFIRE);
        }
        //检查有无宕机未执行任务 如果有执行失败转移
        jobFacade.failoverIfNecessary();
        //都执行完了 回调一下
        try {
            //检查
            jobFacade.afterJobExecuted(shardingContexts);
            // CHECKSTYLE:OFF
        } catch (final Throwable cause) {
            // CHECKSTYLE:ON
            jobErrorHandler.handleException(jobConfig.getJobName(), cause);
        }
    }

    private void execute(final JobConfiguration jobConfig, final ShardingContexts shardingContexts, final ExecutionSource executionSource) {
        if (shardingContexts.getShardingItemParameters().isEmpty()) {
            jobFacade.postJobStatusTraceEvent(shardingContexts.getTaskId(), State.TASK_FINISHED, String.format("Sharding item for job '%s' is empty.", jobConfig.getJobName()));
            return;
        }
        //ZK中标记任务开始
        jobFacade.registerJobBegin(shardingContexts);
        String taskId = shardingContexts.getTaskId();
        //推送消息
        jobFacade.postJobStatusTraceEvent(taskId, State.TASK_RUNNING, "");
        try {
            //都执行完了才算执行完了
            process(jobConfig, shardingContexts, executionSource);
        } finally {
            // TODO Consider increasing the status of job failure, and how to handle the overall loop of job failure
            //这个代码就相当于标记任务已经执行完了
            //ZK中标记任务完成
            jobFacade.registerJobCompleted(shardingContexts);
            //根据结果推送错误或者完成事件
            if (itemErrorMessages.isEmpty()) {
                jobFacade.postJobStatusTraceEvent(taskId, State.TASK_FINISHED, "");
            } else {
                jobFacade.postJobStatusTraceEvent(taskId, State.TASK_ERROR, itemErrorMessages.toString());
                itemErrorMessages.clear();
            }
        }
    }

    private void process(final JobConfiguration jobConfig, final ShardingContexts shardingContexts, final ExecutionSource executionSource) {
        //获取分片信息
        Collection<Integer> items = shardingContexts.getShardingItemParameters().keySet();
        //单机单片执行的
        if (1 == items.size()) {
            log.info("以下是单机单片任务执行逻辑");
            int item = shardingContexts.getShardingItemParameters().keySet().iterator().next();
            JobExecutionEvent jobExecutionEvent = new JobExecutionEvent(IpUtils.getHostName(), IpUtils.getIp(), shardingContexts.getTaskId(), jobConfig.getJobName(), executionSource, item);
            process(jobConfig, shardingContexts, item, jobExecutionEvent);
            return;
        }
        //单机多片执行的
        log.info("以下是单机多片任务执行逻辑");
        //CountDownLatch的时候需要传入一个整数n，在这个整数“倒数”到0之前，主线程需要等待在门口
        CountDownLatch latch = new CountDownLatch(items.size());
        for (int each : items) {
            JobExecutionEvent jobExecutionEvent = new JobExecutionEvent(IpUtils.getHostName(), IpUtils.getIp(), shardingContexts.getTaskId(), jobConfig.getJobName(), executionSource, each);
            ExecutorService executorService = executorServiceReloader.getExecutorService();

            if (executorService.isShutdown()) {
                return;
            }
            executorService.submit(() -> {
                try {
                    process(jobConfig, shardingContexts, each, jobExecutionEvent);
                } finally {
                    latch.countDown();
                }
            });
        }
        try {
            latch.await();
        } catch (final InterruptedException ex) {
            Thread.currentThread().interrupt();
        }
    }

    @SuppressWarnings("unchecked")
    private void process(final JobConfiguration jobConfig, final ShardingContexts shardingContexts, final int item, final JobExecutionEvent startEvent) {
        log.info("发送开始处理任务事件JobExecutionEvent:{}", new Gson().toJson(startEvent));
        jobFacade.postJobExecutionEvent(startEvent);
        log.info("Job '{}' executing, item is: '{}'.", jobConfig.getJobName(), item);
        JobExecutionEvent completeEvent;
        try {
            //这个地方是执行你写的业务代码的地方
            jobItemExecutor.process(elasticJob, jobConfig, jobFacade.getJobRuntimeService(), shardingContexts.createShardingContext(item));
            completeEvent = startEvent.executionSuccess();
            log.info("发送开始处理完成任务事件JobExecutionEvent:{}", new Gson().toJson(startEvent));
            jobFacade.postJobExecutionEvent(completeEvent);
            // CHECKSTYLE:OFF
        } catch (final Throwable cause) {
            log.error("FUCK!!!,执行任务出现异常",cause);
            // CHECKSTYLE:ON
            completeEvent = startEvent.executionFailure(ExceptionUtils.transform(cause));
            log.info("发送开始处理异常任务事件JobExecutionEvent:{}", new Gson().toJson(startEvent));
            jobFacade.postJobExecutionEvent(completeEvent);
            //错误信息放进来
            itemErrorMessages.put(item, ExceptionUtils.transform(cause));
            JobErrorHandler jobErrorHandler = jobErrorHandlerReloader.getJobErrorHandler();
            jobErrorHandler.handleException(jobConfig.getJobName(), cause);
        }
    }

    /**
     * Shutdown executor.
     */
    public void shutdown() {
        executorServiceReloader.close();
        jobErrorHandlerReloader.close();
    }
}
