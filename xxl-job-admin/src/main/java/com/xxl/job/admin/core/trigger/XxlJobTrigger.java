package com.xxl.job.admin.core.trigger;

import com.xxl.job.admin.core.conf.XxlJobAdminConfig;
import com.xxl.job.admin.core.conf.XxlJobScheduler;
import com.xxl.job.admin.core.model.XxlJobGroup;
import com.xxl.job.admin.core.model.XxlJobInfo;
import com.xxl.job.admin.core.model.XxlJobLog;
import com.xxl.job.admin.core.route.ExecutorRouteStrategyEnum;
import com.xxl.job.admin.core.util.I18nUtil;
import com.xxl.job.core.biz.ExecutorBiz;
import com.xxl.job.core.biz.model.ReturnT;
import com.xxl.job.core.biz.model.TriggerParam;
import com.xxl.job.core.enums.ExecutorBlockStrategyEnum;
import com.xxl.rpc.util.IpUtil;
import com.xxl.rpc.util.ThrowableUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;

/**
 * xxl-job trigger
 * Created by xuxueli on 17/7/13.
 * 这个是一个xxl-job的触发器，它主要的职责：
 *      对待执行的任务的基本信息的校验，包括重试次数、分片信息、选择执行器地址并调用对应的执行器进行调度任务执行
 */
public class XxlJobTrigger {
    private static Logger logger = LoggerFactory.getLogger(XxlJobTrigger.class);

    /**
     * 调度器
     *
     * @param jobId 执行任务的jobinfo的id
     *              eg：6
     * @param triggerType 触发任务的类型
     *                    eg：MANUAL
     * @param failRetryCount 失败重试的次数
     *                       eg： -1
     * 			>=0: use this param
     * 			<0: use param from job info config
     * @param executorShardingParam 分片的参数
     *                  eg：null
     * @param executorParam eg：""
     *          null: use job param
     *          not null: cover job param
     *
     * 1、对待执行的任务的基本信息的校验，包括重试次数、分片信息
     * 2、根据路由策略选择执行器的地址
     * 3、根据选中的执行器的地址进行任务调度触发
     */
    public static void trigger(int jobId, TriggerTypeEnum triggerType, int failRetryCount, String executorShardingParam, String executorParam) {
        // 校验任务信息是否存在
        XxlJobInfo jobInfo = XxlJobAdminConfig.getAdminConfig().getXxlJobInfoDao().loadById(jobId);
        if (jobInfo == null) {
            logger.warn(">>>>>>>>>>>> trigger fail, jobId invalid，jobId={}", jobId);
            return;
        }
        //判断执行任务参数是否为空
        if (executorParam != null) {
            jobInfo.setExecutorParam(executorParam);
        }
        //获得配置的失败重试的次数，所以这个失败重试次数实时生效
        int finalFailRetryCount = failRetryCount>=0?failRetryCount:jobInfo.getExecutorFailRetryCount();
        // 获得调度任务对应的额执行器(会调用对应的执行器)
        XxlJobGroup group = XxlJobAdminConfig.getAdminConfig().getXxlJobGroupDao().load(jobInfo.getJobGroup());
        //获得分片的参数规则：根据'/'进行分割 。如果是重试的话，该分片信息为上一次分片的信息
        // sharding param
        int[] shardingParam = null;
        if (executorShardingParam!=null){
            String[] shardingArr = executorShardingParam.split("/");//根据'/'进行分割
            if (shardingArr.length==2 && isNumeric(shardingArr[0]) && isNumeric(shardingArr[1])) {
                shardingParam = new int[2];
                shardingParam[0] = Integer.valueOf(shardingArr[0]); // 分片的索引
                shardingParam[1] = Integer.valueOf(shardingArr[1]); // 分片总数
            }
        }
        //获得任务的执行路由策略
        //如果是广播策略 且执行器的地址不是空的，且不分片，则向每个注册地址触发任务
        if (ExecutorRouteStrategyEnum.SHARDING_BROADCAST==ExecutorRouteStrategyEnum.match(jobInfo.getExecutorRouteStrategy(), null)
                && group.getRegistryList()!=null && !group.getRegistryList().isEmpty()
                && shardingParam==null) {
            for (int i = 0; i < group.getRegistryList().size(); i++) {
                processTrigger(group, jobInfo, finalFailRetryCount, triggerType, i, group.getRegistryList().size());
            }
        } else {
            if (shardingParam == null) {
                shardingParam = new int[]{0, 1};
            }
            processTrigger(group, jobInfo, finalFailRetryCount, triggerType, shardingParam[0], shardingParam[1]);
        }

    }

    /**
     * 判断是否是数值
     * @param str
     * @return
     */
    private static boolean isNumeric(String str){
        try {
            int result = Integer.valueOf(str);
            return true;
        } catch (NumberFormatException e) {
            return false;
        }
    }

    /**
     * 处理触发任务
     * @param group      执行器信息
     * @param jobInfo   任务信息
     * @param finalFailRetryCount 失败重试的次数
     * @param triggerType     触发类型
     * @param index      分片的索引
     * @param total      分片的总数
     *  1、数据库记录本地任务的调度日志
     *  2、根据路由策略选中执行器的地址
     *  3、调用2runExecutor 进行调度任务
     */
    private static void processTrigger(XxlJobGroup group, XxlJobInfo jobInfo, int finalFailRetryCount, TriggerTypeEnum triggerType, int index, int total){

        // 获得阻塞策略
        ExecutorBlockStrategyEnum blockStrategy = ExecutorBlockStrategyEnum.match(jobInfo.getExecutorBlockStrategy(), ExecutorBlockStrategyEnum.SERIAL_EXECUTION);  // block strategy
        //获得路由策略
        ExecutorRouteStrategyEnum executorRouteStrategyEnum = ExecutorRouteStrategyEnum.match(jobInfo.getExecutorRouteStrategy(), null);    // route strategy
        //获得分片参数
        String shardingParam = (ExecutorRouteStrategyEnum.SHARDING_BROADCAST==executorRouteStrategyEnum)?String.valueOf(index).concat("/").concat(String.valueOf(total)):null;

        // 创建 jobLog
        XxlJobLog jobLog = new XxlJobLog();
        jobLog.setJobGroup(jobInfo.getJobGroup());
        jobLog.setJobId(jobInfo.getId());
        jobLog.setTriggerTime(new Date());
        XxlJobAdminConfig.getAdminConfig().getXxlJobLogDao().save(jobLog);
        logger.debug(">>>>>>>>>>> xxl-job trigger start, jobId:{}", jobLog.getId());

        // 2、初始化触发参数
        TriggerParam triggerParam = new TriggerParam();
        triggerParam.setJobId(jobInfo.getId());//任务id
        triggerParam.setExecutorHandler(jobInfo.getExecutorHandler());//执行器
        triggerParam.setExecutorParams(jobInfo.getExecutorParam());//执行参数
        triggerParam.setExecutorBlockStrategy(jobInfo.getExecutorBlockStrategy());//阻塞策略
        triggerParam.setExecutorTimeout(jobInfo.getExecutorTimeout());//执行超时时间
        triggerParam.setLogId(jobLog.getId());//日志id
        triggerParam.setLogDateTim(jobLog.getTriggerTime().getTime());//触发时间
        triggerParam.setGlueType(jobInfo.getGlueType());//Glue类型
        triggerParam.setGlueSource(jobInfo.getGlueSource());//Glue source源，Shell要用的
        triggerParam.setGlueUpdatetime(jobInfo.getGlueUpdatetime().getTime());
        triggerParam.setBroadcastIndex(index); // 分片的索引
        triggerParam.setBroadcastTotal(total); // 分片总数

        // 3、初始化执行器地址
        String address = null;
        ReturnT<String> routeAddressResult = null;
        //获得执行器的注册地址列表，每个执行器都有地址列表（xxl-job-group表中地址）
        //注意，如果这个执行器是自动注册的话，则地址是用客户端执行器注册保存的。客户端执行器在注册完保存到xxlJobRegistry之后，
        //JobRegistryMonitorHelper的线程会每隔30s把xxlJobRegistry里的注册的执行器信息，按照appName分组后，更新到xxl-job-group表中
        if (group.getRegistryList()!=null && !group.getRegistryList().isEmpty()) {
            //如果是分片广播策略
            if (ExecutorRouteStrategyEnum.SHARDING_BROADCAST == executorRouteStrategyEnum) {
                //根据分片索引，从执行器列表中获得对应的执行器地址
                if (index < group.getRegistryList().size()) {
                    address = group.getRegistryList().get(index);
                } else {
                    address = group.getRegistryList().get(0);
                }
            } else {//如果不是分片广播
                //根据触发参数和执行器列表，根据具体的策略方式，获得对应的路由地址
                routeAddressResult = executorRouteStrategyEnum.getRouter().route(triggerParam, group.getRegistryList());
                if (routeAddressResult.getCode() == ReturnT.SUCCESS_CODE) {
                    address = routeAddressResult.getContent();//192.168.0.103:9999
                }
            }
        } else {
            routeAddressResult = new ReturnT<String>(ReturnT.FAIL_CODE, I18nUtil.getString("jobconf_trigger_address_empty"));
        }

        // 4、trigger remote executor
        ReturnT<String> triggerResult = null;
        if (address != null) {
            triggerResult = runExecutor(triggerParam, address);
        } else {
            triggerResult = new ReturnT<String>(ReturnT.FAIL_CODE, null);
        }

        // 5、collection trigger info
        StringBuffer triggerMsgSb = new StringBuffer();
        triggerMsgSb.append(I18nUtil.getString("jobconf_trigger_type")).append("：").append(triggerType.getTitle());
        triggerMsgSb.append("<br>").append(I18nUtil.getString("jobconf_trigger_admin_adress")).append("：").append(IpUtil.getIp());
        triggerMsgSb.append("<br>").append(I18nUtil.getString("jobconf_trigger_exe_regtype")).append("：")
                .append( (group.getAddressType() == 0)?I18nUtil.getString("jobgroup_field_addressType_0"):I18nUtil.getString("jobgroup_field_addressType_1") );
        triggerMsgSb.append("<br>").append(I18nUtil.getString("jobconf_trigger_exe_regaddress")).append("：").append(group.getRegistryList());
        triggerMsgSb.append("<br>").append(I18nUtil.getString("jobinfo_field_executorRouteStrategy")).append("：").append(executorRouteStrategyEnum.getTitle());
        if (shardingParam != null) {
            triggerMsgSb.append("("+shardingParam+")");
        }
        triggerMsgSb.append("<br>").append(I18nUtil.getString("jobinfo_field_executorBlockStrategy")).append("：").append(blockStrategy.getTitle());
        triggerMsgSb.append("<br>").append(I18nUtil.getString("jobinfo_field_timeout")).append("：").append(jobInfo.getExecutorTimeout());
        triggerMsgSb.append("<br>").append(I18nUtil.getString("jobinfo_field_executorFailRetryCount")).append("：").append(finalFailRetryCount);

        triggerMsgSb.append("<br><br><span style=\"color:#00c0ef;\" > >>>>>>>>>>>"+ I18nUtil.getString("jobconf_trigger_run") +"<<<<<<<<<<< </span><br>")
                .append((routeAddressResult!=null&&routeAddressResult.getMsg()!=null)?routeAddressResult.getMsg()+"<br><br>":"").append(triggerResult.getMsg()!=null?triggerResult.getMsg():"");

        // 6、save log trigger-info 保存任务调度的信息
        jobLog.setExecutorAddress(address);//当前调度的执行器地址
        jobLog.setExecutorHandler(jobInfo.getExecutorHandler());//当前调度的执行器的JobHandler
        jobLog.setExecutorParam(jobInfo.getExecutorParam());//当前调度的执行参数
        jobLog.setExecutorShardingParam(shardingParam);//参数调度的分片信息(重试时，可用该信息保证尽量不在同一个调度器里重试)
        jobLog.setExecutorFailRetryCount(finalFailRetryCount);//重试的次数
        //jobLog.setTriggerTime();
        jobLog.setTriggerCode(triggerResult.getCode());
        jobLog.setTriggerMsg(triggerMsgSb.toString());
        XxlJobAdminConfig.getAdminConfig().getXxlJobLogDao().updateTriggerInfo(jobLog);

        logger.debug(">>>>>>>>>>> xxl-job trigger end, jobId:{}", jobLog.getId());
    }

    /**
     * 执行触发的任务
     * @param triggerParam
     *  {jobId = 1
     * executorHandler = "demoJobHandler"
     * executorParams = ""
     * executorBlockStrategy = "SERIAL_EXECUTION"
     * executorTimeout = 0
     * logId = 1023
     * logDateTim = 1577787507688
     * glueType = "BEAN"
     * glueSource = ""
     * glueUpdatetime = 1541254891000
     * broadcastIndex = 0
     * broadcastTotal = 1
     * }
     * @param address 被选中的执行器的地址：192.168.0.103:9999
     * @return
     * 1、根据执行器地址，检查执行器地址对应的客户端连接NettyHttpClient，没有则创建新的NettyHttpClient
     * 2、通过NettyHttpClient客户端连接向执行器发起调度请求
     */

    public static ReturnT<String> runExecutor(TriggerParam triggerParam, String address){
        ReturnT<String> runResult = null;
        try {
            //获得对应的执行器的网络连接对象（默认采用的是Netty网络连接）
            ExecutorBiz executorBiz = XxlJobScheduler.getExecutorBiz(address);
            //通过执行器的代理对象，调用远程的执行器，进行任务调度
            runResult = executorBiz.run(triggerParam);
        } catch (Exception e) {
            logger.error(">>>>>>>>>>> xxl-job trigger error, please check if the executor[{}] is running.", address, e);
            runResult = new ReturnT<String>(ReturnT.FAIL_CODE, ThrowableUtil.toString(e));
        }

        StringBuffer runResultSB = new StringBuffer(I18nUtil.getString("jobconf_trigger_run") + "：");
        runResultSB.append("<br>address：").append(address);
        runResultSB.append("<br>code：").append(runResult.getCode());
        runResultSB.append("<br>msg：").append(runResult.getMsg());

        runResult.setMsg(runResultSB.toString());
        return runResult;
    }

}
