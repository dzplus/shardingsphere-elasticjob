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

package org.apache.shardingsphere.elasticjob.kernel.internal.schedule;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.shardingsphere.elasticjob.api.ElasticJob;
import org.apache.shardingsphere.elasticjob.api.JobConfiguration;
import org.apache.shardingsphere.elasticjob.spi.executor.error.handler.JobErrorHandlerPropertiesValidator;
import org.apache.shardingsphere.elasticjob.kernel.executor.ElasticJobExecutor;
import org.apache.shardingsphere.elasticjob.kernel.infra.exception.JobSystemException;
import org.apache.shardingsphere.elasticjob.kernel.executor.facade.JobFacade;
import org.apache.shardingsphere.elasticjob.kernel.internal.sharding.JobInstance;
import org.apache.shardingsphere.elasticjob.spi.listener.ElasticJobListener;
import org.apache.shardingsphere.elasticjob.kernel.listener.AbstractDistributeOnceElasticJobListener;
import org.apache.shardingsphere.elasticjob.kernel.internal.config.ConfigurationService;
import org.apache.shardingsphere.elasticjob.kernel.internal.guarantee.GuaranteeService;
import org.apache.shardingsphere.elasticjob.kernel.internal.setup.JobClassNameProviderFactory;
import org.apache.shardingsphere.elasticjob.kernel.internal.setup.SetUpFacade;
import org.apache.shardingsphere.elasticjob.reg.base.CoordinatorRegistryCenter;
import org.apache.shardingsphere.elasticjob.kernel.tracing.config.TracingConfiguration;
import org.apache.shardingsphere.infra.spi.type.typed.TypedSPILoader;
import org.quartz.JobBuilder;
import org.quartz.JobDetail;
import org.quartz.Scheduler;
import org.quartz.SchedulerException;
import org.quartz.impl.StdSchedulerFactory;
import org.quartz.simpl.SimpleThreadPool;

import java.util.Collection;
import java.util.Optional;
import java.util.Properties;
import java.util.stream.Collectors;

/**
 * Job scheduler.
 */
@Slf4j
public final class JobScheduler {

    private static final String JOB_EXECUTOR_DATA_MAP_KEY = "jobExecutor";

    @Getter
    private final CoordinatorRegistryCenter regCenter;

    @Getter
    private final JobConfiguration jobConfig;

    private final SetUpFacade setUpFacade;

    private final SchedulerFacade schedulerFacade;

    private final JobFacade jobFacade;

    private final ElasticJobExecutor jobExecutor;

    @Getter
    private final JobScheduleController jobScheduleController;

    //调度器初始化
    public JobScheduler(final CoordinatorRegistryCenter regCenter, final ElasticJob elasticJob, final JobConfiguration jobConfig) {
        log.info("JobScheduler初始化，{}",jobConfig.getJobName());
        Preconditions.checkArgument(null != elasticJob, "Elastic job cannot be null.");
        //注册中心
        this.regCenter = regCenter;
        //生成任务名
        String jobClassName = JobClassNameProviderFactory.getProvider().getJobClassName(elasticJob);
        //配置
        this.jobConfig = setUpJobConfiguration(regCenter, jobClassName, jobConfig);
        //自定义监听器
        Collection<ElasticJobListener> jobListeners = getElasticJobListeners(this.jobConfig);
        //初始化服务
        setUpFacade = new SetUpFacade(regCenter, this.jobConfig.getJobName(), jobListeners);
        //调度器服务加载
        schedulerFacade = new SchedulerFacade(regCenter, this.jobConfig.getJobName());
        //任务服务加载
        jobFacade = new JobFacade(regCenter, this.jobConfig.getJobName(), jobListeners, findTracingConfiguration().orElse(null));
        validateJobProperties();
        //执行器加载
        jobExecutor = new ElasticJobExecutor(elasticJob, this.jobConfig, jobFacade);
        //确认工作
        setGuaranteeServiceForElasticJobListeners(regCenter, jobListeners);
        //创建任务执行控制器
        jobScheduleController = createJobScheduleController();
        log.info("JobScheduler初始化....完成");
    }

    public JobScheduler(final CoordinatorRegistryCenter regCenter, final String elasticJobType, final JobConfiguration jobConfig) {
        log.info("JobScheduler初始化，{}",jobConfig.getJobName());
        Preconditions.checkArgument(!Strings.isNullOrEmpty(elasticJobType), "Elastic job type cannot be null or empty.");
        this.regCenter = regCenter;
        //获取配置信息
        this.jobConfig = setUpJobConfiguration(regCenter, elasticJobType, jobConfig);
        //获取监听器
        Collection<ElasticJobListener> jobListeners = getElasticJobListeners(this.jobConfig);
        setUpFacade = new SetUpFacade(regCenter, this.jobConfig.getJobName(), jobListeners);
        schedulerFacade = new SchedulerFacade(regCenter, this.jobConfig.getJobName());
        jobFacade = new JobFacade(regCenter, this.jobConfig.getJobName(), jobListeners, findTracingConfiguration().orElse(null));
        validateJobProperties();
        jobExecutor = new ElasticJobExecutor(elasticJobType, this.jobConfig, jobFacade);
        setGuaranteeServiceForElasticJobListeners(regCenter, jobListeners);
        jobScheduleController = createJobScheduleController();
        log.info("JobScheduler初始化....完成");
    }

    private JobConfiguration setUpJobConfiguration(final CoordinatorRegistryCenter regCenter, final String jobClassName, final JobConfiguration jobConfig) {
        log.info("初始化配置服务：{},{}", jobConfig.getJobName(),jobClassName);
        ConfigurationService configService = new ConfigurationService(regCenter, jobConfig.getJobName());
        log.info("任务：{}的配置服务已初始化，准备填充配置信息", jobConfig.getJobName());
        return configService.setUpJobConfiguration(jobClassName, jobConfig);
    }

    private Collection<ElasticJobListener> getElasticJobListeners(final JobConfiguration jobConfig) {
        return jobConfig.getJobListenerTypes().stream().map(each -> TypedSPILoader.getService(ElasticJobListener.class, each)).collect(Collectors.toList());
    }

    private Optional<TracingConfiguration<?>> findTracingConfiguration() {
        return jobConfig.getExtraConfigurations().stream().filter(each -> each instanceof TracingConfiguration).findFirst().map(extraConfig -> (TracingConfiguration<?>) extraConfig);
    }

    private void validateJobProperties() {
        validateJobErrorHandlerProperties();
    }

    private void validateJobErrorHandlerProperties() {
        if (null != jobConfig.getJobErrorHandlerType()) {
            TypedSPILoader.findService(JobErrorHandlerPropertiesValidator.class, jobConfig.getJobErrorHandlerType(), jobConfig.getProps())
                    .ifPresent(validator -> validator.validate(jobConfig.getProps()));
        }
    }

    private void setGuaranteeServiceForElasticJobListeners(final CoordinatorRegistryCenter regCenter, final Collection<ElasticJobListener> elasticJobListeners) {
        GuaranteeService guaranteeService = new GuaranteeService(regCenter, jobConfig.getJobName());
        for (ElasticJobListener each : elasticJobListeners) {
            if (each instanceof AbstractDistributeOnceElasticJobListener) {
                ((AbstractDistributeOnceElasticJobListener) each).setGuaranteeService(guaranteeService);
            }
        }
    }

    /**
     * 创建任务调度器控制器
     * @return
     */
    private JobScheduleController createJobScheduleController() {
        //初始化一个任务调度器控制器
        log.info("开始创建Job:{}", getJobConfig().getJobName());
        JobScheduleController result = new JobScheduleController(createScheduler(), createJobDetail(), getJobConfig().getJobName());
        //在本地map注册调度器
        JobRegistry.getInstance().registerJobScheduleController(getJobConfig().getJobName(), result);
        //然后注册到ZK上并启动
        registerStartUpInfo();
        log.info("开始创建Job....完成:{}", getJobConfig().getJobName());
        return result;
    }

    /**
     * 创建任务调度器
     * @return
     */

    private Scheduler createScheduler() {
        log.info("创建Job对应的Scheduler:{}", getJobConfig().getJobName());
        Scheduler result;
        try {
            //quartz工厂
            StdSchedulerFactory factory = new StdSchedulerFactory();
            factory.initialize(getQuartzProps());
            //初始化后取quartz的调度器
            result = factory.getScheduler();
            //加载任务执行监听
            result.getListenerManager().addTriggerListener(schedulerFacade.newJobTriggerListener());
        } catch (final SchedulerException ex) {
            throw new JobSystemException(ex);
        }
        return result;
    }


    /**
     * 将配置中的任务配置信息转换为quartz可用的信息
     * @return
     */
    private Properties getQuartzProps() {
        log.info("取出任务中的定时信息,创建Quartz任务,{},{},{}", getJobConfig().getCron(), getJobConfig().getTimeZone(), getJobConfig().getJobName());
        Properties result = new Properties();
        result.put("org.quartz.threadPool.class", SimpleThreadPool.class.getName());
        result.put("org.quartz.threadPool.threadCount", "1");
        result.put("org.quartz.scheduler.instanceName", getJobConfig().getJobName());
        result.put("org.quartz.jobStore.misfireThreshold", "1");
        //这里如果任务宕机或者关机 1、如果当前节点为主节点 那就移除掉主节点 2、在ZK上移除本机节点
        result.put("org.quartz.plugin.shutdownhook.class", JobShutdownHookPlugin.class.getName());
        result.put("org.quartz.plugin.shutdownhook.cleanShutdown", Boolean.TRUE.toString());
        return result;
    }

    /**
     * 任务执行的状态、任务所需的参数、任务标记
     * @return
     */
    private JobDetail createJobDetail() {
        log.info("创建Job对应的JobDetail:{}", getJobConfig().getJobName());
        JobDetail result = JobBuilder.newJob(LiteJob.class).withIdentity(getJobConfig().getJobName()).build();
        result.getJobDataMap().put(JOB_EXECUTOR_DATA_MAP_KEY, jobExecutor);
        return result;
    }

    /**
     *  注册任务启动信息
     *  工作路径
     *  实例信息
     *  分片信息
     *  注册本机信息到ZK
     */
    private void registerStartUpInfo() {
        //添加注册中心信息到本地缓存和ZK
        JobRegistry.getInstance().registerRegistryCenter(jobConfig.getJobName(), regCenter);
        //添加实例到本地缓存
        JobRegistry.getInstance().addJobInstance(jobConfig.getJobName(), new JobInstance());
        //添加分片信息到本地缓存
        JobRegistry.getInstance().setCurrentShardingTotalCount(jobConfig.getJobName(), jobConfig.getShardingTotalCount());
        //启动监听器、注册本机信息到ZK
        setUpFacade.registerStartUpInfo(!jobConfig.isDisabled());
    }

    /**
     * Shutdown job.
     */
    public void shutdown() {
        setUpFacade.tearDown();
        schedulerFacade.shutdownInstance();
        jobExecutor.shutdown();
    }
}
