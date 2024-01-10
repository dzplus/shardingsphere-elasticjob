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
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.shardingsphere.elasticjob.kernel.infra.exception.JobSystemException;
import org.quartz.*;

import java.util.List;
import java.util.TimeZone;

/**
 * Job schedule controller.
 */
@Slf4j
@RequiredArgsConstructor
public final class JobScheduleController {

    private final Scheduler scheduler;

    private final JobDetail jobDetail;

    private final String triggerIdentity;

    /**
     * Schedule job.
     *
     * @param cron     CRON expression
     * @param timeZone the time zone
     */
    public void scheduleJob(final String cron, final String timeZone) {
        log.info("创建Schedule:{},{},getName：{}", cron, timeZone, jobDetail.getKey().getName());
        try {
            if (!scheduler.checkExists(jobDetail.getKey())) {
                scheduler.scheduleJob(jobDetail, createCronTrigger(cron, timeZone));
            }
            // 获取正在等待执行的Job列表
            List<JobExecutionContext> pendingJobs = scheduler.getCurrentlyExecutingJobs();
            if (!pendingJobs.isEmpty()) {
                for (JobExecutionContext jobContext : pendingJobs) {
                    JobDetail jobDetail = jobContext.getJobDetail();
                    log.info("已经存在的Job名称：{},{}", jobDetail.getKey().getName(), jobDetail.getKey().getGroup());
                }
            }
            scheduler.start();
        } catch (final SchedulerException ex) {
            throw new JobSystemException(ex);
        }
    }

    /**
     * Reschedule job.
     *
     * @param cron     CRON expression
     * @param timeZone the time zone
     */
    public synchronized void rescheduleJob(final String cron, final String timeZone) {
        log.info("rescheduleJob:{},{}", cron, timeZone);
        try {
            CronTrigger trigger = (CronTrigger) scheduler.getTrigger(TriggerKey.triggerKey(triggerIdentity));
            if (!scheduler.isShutdown() && null != trigger && !cron.equals(trigger.getCronExpression())) {
                scheduler.rescheduleJob(TriggerKey.triggerKey(triggerIdentity), createCronTrigger(cron, timeZone));
            }
        } catch (final SchedulerException ex) {
            throw new JobSystemException(ex);
        }
    }

    /**
     * Reschedule OneOff job.
     */
    public synchronized void rescheduleJob() {
        log.info("rescheduleJob");
        try {
            SimpleTrigger trigger = (SimpleTrigger) scheduler.getTrigger(TriggerKey.triggerKey(triggerIdentity));
            if (!scheduler.isShutdown() && null != trigger) {
                scheduler.rescheduleJob(TriggerKey.triggerKey(triggerIdentity), createOneOffTrigger());
            }
        } catch (final SchedulerException ex) {
            throw new JobSystemException(ex);
        }
    }

    private Trigger createCronTrigger(final String cron, final String timeZoneString) {
        log.info("createCronTrigger:{},{}", cron, timeZoneString);
        return TriggerBuilder.newTrigger().withIdentity(triggerIdentity).withSchedule(
                CronScheduleBuilder.cronSchedule(cron).inTimeZone(parseTimeZoneString(timeZoneString)).withMisfireHandlingInstructionDoNothing()).build();
    }

    /**
     * Get the TimeZone for the time zone specification.
     *
     * @param timeZoneString must start with "GMT", such as "GMT+8:00"
     * @return the specified TimeZone, or the GMT zone if the `timeZoneString` cannot be understood.
     */
    private TimeZone parseTimeZoneString(final String timeZoneString) {
        if (Strings.isNullOrEmpty(timeZoneString)) {
            return TimeZone.getDefault();
        }
        Preconditions.checkArgument(timeZoneString.startsWith("GMT"), "Invalid time zone specification '%s'.", timeZoneString);
        return TimeZone.getTimeZone(timeZoneString);
    }

    /**
     * Judge job is pause or not.
     *
     * @return job is pause or not
     */
    public synchronized boolean isPaused() {
        try {
            return !scheduler.isShutdown() && Trigger.TriggerState.PAUSED == scheduler.getTriggerState(new TriggerKey(triggerIdentity));
        } catch (final SchedulerException ex) {
            throw new JobSystemException(ex);
        }
    }

    /**
     * Pause job.
     */
    public synchronized void pauseJob() {
        try {
            if (!scheduler.isShutdown()) {
                scheduler.pauseAll();
            }
        } catch (final SchedulerException ex) {
            throw new JobSystemException(ex);
        }
    }

    /**
     * Resume job.
     */
    public synchronized void resumeJob() {
        try {
            if (!scheduler.isShutdown()) {
                scheduler.resumeAll();
            }
        } catch (final SchedulerException ex) {
            throw new JobSystemException(ex);
        }
    }

    /**
     * Trigger job.
     */
    // TODO duanzhang 2024/1/8 15:28 此处为触发任务关键节点
    public synchronized void triggerJob() {
        log.info("触发任务" + jobDetail.getKey());
        try {
            if (scheduler.isShutdown()) {
                return;
            }
            //如果不存在就添加
            if (!scheduler.checkExists(jobDetail.getKey())) {
                scheduler.scheduleJob(jobDetail, createOneOffTrigger());
            } else {
                scheduler.triggerJob(jobDetail.getKey());
            }
            if (!scheduler.isStarted()) {
                scheduler.start();
            }
        } catch (final SchedulerException ex) {
            throw new JobSystemException(ex);
        }
    }

    private Trigger createOneOffTrigger() {
        return TriggerBuilder.newTrigger().withIdentity(triggerIdentity).withSchedule(SimpleScheduleBuilder.simpleSchedule()).build();
    }

    /**
     * Shutdown scheduler.
     */
    public synchronized void shutdown() {
        shutdown(false);
    }

    /**
     * Shutdown scheduler graceful.
     *
     * @param isCleanShutdown if wait jobs complete
     */
    public synchronized void shutdown(final boolean isCleanShutdown) {
        try {
            if (!scheduler.isShutdown()) {
                scheduler.shutdown(isCleanShutdown);
            }
        } catch (final SchedulerException ex) {
            throw new JobSystemException(ex);
        }
    }
}
