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

package org.apache.shardingsphere.elasticjob.bootstrap.type;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import lombok.extern.slf4j.Slf4j;
import org.apache.shardingsphere.elasticjob.api.ElasticJob;
import org.apache.shardingsphere.elasticjob.api.JobConfiguration;
import org.apache.shardingsphere.elasticjob.bootstrap.JobBootstrap;
import org.apache.shardingsphere.elasticjob.kernel.internal.annotation.JobAnnotationBuilder;
import org.apache.shardingsphere.elasticjob.kernel.internal.schedule.JobScheduler;
import org.apache.shardingsphere.elasticjob.reg.base.CoordinatorRegistryCenter;

/**
 * Schedule job bootstrap.
 */
@Slf4j
public final class ScheduleJobBootstrap implements JobBootstrap {

    private final JobScheduler jobScheduler;

    public ScheduleJobBootstrap(final CoordinatorRegistryCenter regCenter, final ElasticJob elasticJob, final JobConfiguration jobConfig) {
        log.info("ScheduleJobBootstrap初始化");
        jobScheduler = new JobScheduler(regCenter, elasticJob, jobConfig);
        log.info("ScheduleJobBootstrap初始化...完成");
    }

    public ScheduleJobBootstrap(final CoordinatorRegistryCenter regCenter, final String elasticJobType, final JobConfiguration jobConfig) {
        log.info("ScheduleJobBootstrap初始化1");
        jobScheduler = new JobScheduler(regCenter, elasticJobType, jobConfig);
        log.info("ScheduleJobBootstrap初始化...完成");
    }

    public ScheduleJobBootstrap(final CoordinatorRegistryCenter regCenter, final ElasticJob elasticJob) {
        log.info("ScheduleJobBootstrap初始化2");
        JobConfiguration jobConfig = JobAnnotationBuilder.generateJobConfiguration(elasticJob.getClass());
        jobScheduler = new JobScheduler(regCenter, elasticJob, jobConfig);
        log.info("ScheduleJobBootstrap初始化...完成");
    }

    /**
     * Schedule job.
     */
    public void schedule() {
        Preconditions.checkArgument(!Strings.isNullOrEmpty(jobScheduler.getJobConfig().getCron()), "Cron can not be empty.");
        //这一步可以理解为 获取调度器控制器 然后通过调度器控制器调度任务
        log.info("jobScheduler通过Cron触发,name:{}", jobScheduler.getJobConfig().getJobName());
        jobScheduler.getJobScheduleController().scheduleJob(jobScheduler.getJobConfig().getCron(), jobScheduler.getJobConfig().getTimeZone());
    }

    @Override
    public void shutdown() {
        jobScheduler.shutdown();
    }
}
