package com.xxl.job.core.biz.impl;

import com.xxl.job.core.biz.ExecutorBiz;
import com.xxl.job.core.biz.model.LogResult;
import com.xxl.job.core.biz.model.ReturnT;
import com.xxl.job.core.biz.model.TriggerParam;
import com.xxl.job.core.enums.ExecutorBlockStrategyEnum;
import com.xxl.job.core.executor.XxlJobExecutor;
import com.xxl.job.core.glue.GlueFactory;
import com.xxl.job.core.glue.GlueTypeEnum;
import com.xxl.job.core.handler.IJobHandler;
import com.xxl.job.core.handler.impl.GlueJobHandler;
import com.xxl.job.core.handler.impl.ScriptJobHandler;
import com.xxl.job.core.log.XxlJobFileAppender;
import com.xxl.job.core.thread.JobThread;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;

/**
 * Created by xuxueli on 17/3/1.
 * 该实现接口是执行器真正执行任务的接口，由调度调用执行器的ExecutorBizImpl.run进行任务调用
 */
public class ExecutorBizImpl implements ExecutorBiz {
    private static Logger logger = LoggerFactory.getLogger(ExecutorBizImpl.class);

    @Override
    public ReturnT<String> beat() {
        return ReturnT.SUCCESS;
    }

    /**
     * 心跳检查某个Job对应的JobThread是否空闲(路由策略会用到)
     * @param jobId 任务
     * @return
     * 1、获取job对应的执行线程JobThread
     * 2、检查当前JobThread线程是否处于running，或有任务正在队列里排队执行，是的话返回500，不然几句返回true
     */
    @Override
    public ReturnT<String> idleBeat(int jobId) {

        // isRunningOrHasQueue
        boolean isRunningOrHasQueue = false;
        JobThread jobThread = XxlJobExecutor.loadJobThread(jobId);
        if (jobThread != null && jobThread.isRunningOrHasQueue()) {
            isRunningOrHasQueue = true;
        }

        if (isRunningOrHasQueue) {
            return new ReturnT<String>(ReturnT.FAIL_CODE, "job thread is running or has trigger queue.");
        }
        return ReturnT.SUCCESS;
    }

    /***
     * 停止一个任务
     * @param jobId
     * @return
     * 1、根据任务id查找对应的任务线程
     * 2、修改线程的状态，并中断这个正在执行的线程（注意这边只是中断，并不是强制停止）
     */
    @Override
    public ReturnT<String> kill(int jobId) {
        // kill handlerThread, and create new one
        JobThread jobThread = XxlJobExecutor.loadJobThread(jobId);
        if (jobThread != null) {
            XxlJobExecutor.removeJobThread(jobId, "scheduling center kill job.");
            return ReturnT.SUCCESS;
        }

        return new ReturnT<String>(ReturnT.SUCCESS_CODE, "job thread aleady killed.");
    }

    @Override
    public ReturnT<LogResult> log(long logDateTim, int logId, int fromLineNum) {
        // log filename: logPath/yyyy-MM-dd/9999.log
        String logFileName = XxlJobFileAppender.makeLogFileName(new Date(logDateTim), logId);

        LogResult logResult = XxlJobFileAppender.readLog(logFileName, fromLineNum);
        return new ReturnT<LogResult>(logResult);
    }

    /***
     * 执行器开始执行调度器分配的任务
     * 开始调度任务（本质是把它放到队列里，有单独的线程从队列里拿出并进行触发）
     * 这边我们会发现，每个jobInfo会对应一个单独的JobThread
     * @param triggerParam
     * @return
     * 1、根据调度任务id检查内存里是否已存在对应的任务线程
     *      每个jobHandler都会绑定一个JobThread线程。如果是Bean的方式的话，那么在配置任务任务的时候是需要指定执行器上对应的任务处理器jobHandler。
     *      因为每个调度任务都会绑定一个特定的JobThread，用于专门执行该类Job的。
     *      要注意，一个JobThread可能会响应执行多个任务的调度。
     * 2、判断任务的运行模式，获取真正负责任务执行的任务处理器：jobHandler
     *      如果是Bean模式，则会根据任务配置的jobHandler从当前执行器中获取对应的JobHandler对象（执行器在启动的时候会把自身包含的所有的JobHandler都注册到内存里）
     *      如果是GLUE(Java)模式，则检查当前JobThread是否已经存在jobHandler，没有的话则会根据配置的源码通过反射生成加载并创建一个jobHandler跟当前JobThread绑定。
     * 3、检查调度任务的阻塞策略(调度器传过来的)
     *      a、DISCARD_LATER：表示丢弃后续任务策略，那么如果当前JobThread有任务则在running，或者说对列存在等待的任务，则丢弃当前需要执行的任务，执行调度器失败。
     *      b、覆盖策略：表示覆盖之前任务的策略，则会中断正在执行的JobThread，同时创建一个新的JobThread，并绑定到对应的Jobhandler(内存里也会更新映射关系)。
     * 4、将本次调度的任务推送到JobThread线程的对列中，并返回调度器执行成功.
     *      这里我们可以发现，当返回调度执行成功后，它其实只是放到JobThread的队列中等待执行，并未被真正的执行。
     *      如果JobThread的队列中包含本次要调度的任务(也就是logId已存在，不是JobId哦)，则会返回调度器某一次调度任务重复调度了。
     */
    @Override
    public ReturnT<String> run(TriggerParam triggerParam) {
        // load old：jobHandler + jobThread
        //根据jobId获得已存在的线程JobThread和jobHandler
        JobThread jobThread = XxlJobExecutor.loadJobThread(triggerParam.getJobId());
        IJobHandler jobHandler = jobThread!=null?jobThread.getHandler():null;
        String removeOldReason = null;

        // valid：jobHandler + jobThread
        //获得Glue类型
        GlueTypeEnum glueTypeEnum = GlueTypeEnum.match(triggerParam.getGlueType());
        if (GlueTypeEnum.BEAN == glueTypeEnum) {//如果是BEAN类型，则根据执行器名称查找注册的 IJobHandler
            //执行器在启动的时候会注册到admin上，并被保存到内存里
            // 根据执行器名称，从本地内存里里获取 执行器绑定的 IJobHandler
            IJobHandler newJobHandler = XxlJobExecutor.loadJobHandler(triggerParam.getExecutorHandler());

            // 如果存在旧的jobThread，且旧的jobHandler和新的IJobHandler不相等，则可能执行器或Glue发生改变，所以必须暂停旧的任务
            if (jobThread!=null && jobHandler != newJobHandler) {
                // change handler, need kill old thread
                removeOldReason = "change jobhandler or glue type, and terminate the old job thread.";

                jobThread = null;
                jobHandler = null;
            }

            // 如果旧的 jobHandler为空，则要新建一个执行器绑定的 IJobHandler
            if (jobHandler == null) {
                jobHandler = newJobHandler;
                if (jobHandler == null) {
                    return new ReturnT<String>(ReturnT.FAIL_CODE, "job handler [" + triggerParam.getExecutorHandler() + "] not found.");
                }
            }

        } else if (GlueTypeEnum.GLUE_GROOVY == glueTypeEnum) {

            // valid old jobThread
            if (jobThread != null &&
                    !(
                            jobThread.getHandler() instanceof GlueJobHandler
                            && ((GlueJobHandler) jobThread.getHandler()).getGlueUpdatetime()==triggerParam.getGlueUpdatetime() )) {
                // change handler or gluesource updated, need kill old thread
                removeOldReason = "change job source or glue type, and terminate the old job thread.";

                jobThread = null;
                jobHandler = null;
            }

            // valid handler
            if (jobHandler == null) {
                try {
                    IJobHandler originJobHandler = GlueFactory.getInstance().loadNewInstance(triggerParam.getGlueSource());
                    jobHandler = new GlueJobHandler(originJobHandler, triggerParam.getGlueUpdatetime());
                } catch (Exception e) {
                    logger.error(e.getMessage(), e);
                    return new ReturnT<String>(ReturnT.FAIL_CODE, e.getMessage());
                }
            }
        } else if (glueTypeEnum!=null && glueTypeEnum.isScript()) {

            // valid old jobThread
            if (jobThread != null &&
                    !(jobThread.getHandler() instanceof ScriptJobHandler
                            && ((ScriptJobHandler) jobThread.getHandler()).getGlueUpdatetime()==triggerParam.getGlueUpdatetime() )) {
                // change script or gluesource updated, need kill old thread
                removeOldReason = "change job source or glue type, and terminate the old job thread.";

                jobThread = null;
                jobHandler = null;
            }

            // valid handler
            if (jobHandler == null) {
                jobHandler = new ScriptJobHandler(triggerParam.getJobId(), triggerParam.getGlueUpdatetime(), triggerParam.getGlueSource(), GlueTypeEnum.match(triggerParam.getGlueType()));
            }
        } else {
            return new ReturnT<String>(ReturnT.FAIL_CODE, "glueType[" + triggerParam.getGlueType() + "] is not valid.");
        }

        // executor block strategy
        if (jobThread != null) {
            //获得阻塞策略
            ExecutorBlockStrategyEnum blockStrategy = ExecutorBlockStrategyEnum.match(triggerParam.getExecutorBlockStrategy(), null);
            if (ExecutorBlockStrategyEnum.DISCARD_LATER == blockStrategy) {//丢弃后面任务的策略
                // 如果当前线程还在跑，获取等待的队列不为空，则丢弃这新来的任务
                if (jobThread.isRunningOrHasQueue()) {
                    return new ReturnT<String>(ReturnT.FAIL_CODE, "block strategy effect："+ExecutorBlockStrategyEnum.DISCARD_LATER.getTitle());
                }
            } else if (ExecutorBlockStrategyEnum.COVER_EARLY == blockStrategy) {//如果是覆盖之前的任务
                // 如果当前有任务还在跑，或者队列不为空，则执行杀掉当前任务
                if (jobThread.isRunningOrHasQueue()) {
                    removeOldReason = "block strategy effect：" + ExecutorBlockStrategyEnum.COVER_EARLY.getTitle();

                    jobThread = null;
                }
            } else {
                // just queue trigger
            }
        }

        // replace thread (new or exists invalid)
        if (jobThread == null) {
            jobThread = XxlJobExecutor.registJobThread(triggerParam.getJobId(), jobHandler, removeOldReason);
        }

        // push data to queue
        ReturnT<String> pushResult = jobThread.pushTriggerQueue(triggerParam);
        return pushResult;
    }

}
