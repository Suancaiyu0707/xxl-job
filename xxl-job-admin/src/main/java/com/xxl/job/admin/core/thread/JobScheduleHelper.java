package com.xxl.job.admin.core.thread;

import com.xxl.job.admin.core.conf.XxlJobAdminConfig;
import com.xxl.job.admin.core.cron.CronExpression;
import com.xxl.job.admin.core.model.XxlJobInfo;
import com.xxl.job.admin.core.trigger.TriggerTypeEnum;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

/**
 * @author xuxueli 2019-05-21
 * 这个类主要启动了两个线程：scheduleThread和ringThread线程
 *      scheduleThread：定时500-1000毫秒执行一次。
 *          1、查询表Xxl_Job_Info表，只查询最近10s内要执行的任务(也就是下次触发时间trigger_next_time<currentTime+10s)，包括trigger_next_time<currentTime
 *          2、遍历第1步查询出来的列表：
 *              如果下次触发时间trigger_next_time< currentTime-10s，也就是距离本该触发的时间超过了10s，则忽略，并重新记录下一次触发的时间。
 *              如果下次触发时间currentTime-10s<trigger_next_time< currentTime，则立马触发一次，
 *                  也就是把当前立马要执行的任务放到当前时刻的时间轮里，并计算下一次触发的时间。
 *              如果下次触发时间trigger_next_time> currentTime，则计算将要触发任务所处的时间轮，并计算下一次触发的时间
 *          3、通过第2步，会把10s内要执行的任务都放到它们在时间轮本该所处的位置。并更新任务下一次触发的时间节点
 *      ringThread：每隔1s中执行一次(因为时间轮的最小单位是1s)
 *          1、获得当前所处的秒钟 nowSecond，也就是映射时间轮里的索引.
 *          2、遍历时间轮ringData，从上一次遍历的时间节点索引lastSecond继续往后遍历，直到遍历到当前时间节点nowSecond暂停(因为后面的还没到执行的时间点)，遍历过程中遇到需要执行的任务都从ringData中取出来并准备执行：
 *              比如上一次调度到时间轮里的第25s(也就是lastSecond=25)。那么就继续从25s开始遍历，直到索引位置为nowSecond=35s的时间节点则暂停(因为当前才是1分钟内的第35s，那35s的时间节点之后的都还没到触发调度的点)
 *          3、每次遍历完后都记录已触发的时间节点刻度到lastSecond（set lastSecond = 35）
 *          4、依次触发此时遍历的需要执行的调度任务
 */
public class JobScheduleHelper {
    private static Logger logger = LoggerFactory.getLogger(JobScheduleHelper.class);

    private static JobScheduleHelper instance = new JobScheduleHelper();
    public static JobScheduleHelper getInstance(){
        return instance;
    }

    private Thread scheduleThread;
    private Thread ringThread;
    private volatile boolean toStop = false;
    /***
     * 维护的是一个时间轮，key是1-60s，value是秒里对应的需要执行XXJOBINFO
     */
    private volatile static Map<Integer, List<Integer>> ringData = new ConcurrentHashMap<>();

    /***
     * 启动当前任务
     */
    public void start(){

        // 创建一个线程不断的进行while循环 随机睡眠 5000-6000毫秒
        scheduleThread = new Thread(new Runnable() {
            @Override
            public void run() {

                try {
                    TimeUnit.MILLISECONDS.sleep(5000 + System.currentTimeMillis()%1000 );
                } catch (InterruptedException e) {
                    if (!toStop) {
                        logger.error(e.getMessage(), e);
                    }
                }
                logger.info(">>>>>>>>> init xxl-job admin scheduler success.");

                while (!toStop) {

                    // 匹配任务
                    Connection conn = null;
                    PreparedStatement preparedStatement = null;
                    try {
                        if (conn==null || conn.isClosed()) {
                            conn = XxlJobAdminConfig.getAdminConfig().getDataSource().getConnection();
                        }
                        conn.setAutoCommit(false);
                        //获得数据库锁
                        preparedStatement = conn.prepareStatement(  "select * from XXL_JOB_LOCK where lock_name = 'schedule_lock' for update" );
                        preparedStatement.execute();

                        // tx start

                        // 1、预读10s内调度任务
                        long maxNextTime = System.currentTimeMillis() + 10000;
                        long nowTime = System.currentTimeMillis();
                        //查询10秒内将被调度的任务
                        List<XxlJobInfo> scheduleList = XxlJobAdminConfig.getAdminConfig().getXxlJobInfoDao().scheduleJobQuery(maxNextTime);
                        //遍历所有的在未来10s内将被执行的调度任务
                        if (scheduleList!=null && scheduleList.size()>0) {
                            // 2、推送时间轮
                            for (XxlJobInfo jobInfo: scheduleList) {

                                // 时间轮刻度计算 单位是s
                                int ringSecond = -1;
                                //如果过期的时间超过10s
                                if (jobInfo.getTriggerNextTime() < nowTime - 10000) {   // 过期超10s：本地忽略，当前时间开始计算下次触发时间
                                    //如果下次触发时间trigger_next_time< currentTime-10s，也就是距离本该触发的时间超过了10s，则放弃此次任务
                                    ringSecond = -1;

                                    jobInfo.setTriggerLastTime(jobInfo.getTriggerNextTime());
                                    //计算下一次触发的时间
                                    jobInfo.setTriggerNextTime(
                                            new CronExpression(jobInfo.getJobCron())
                                                    .getNextValidTimeAfter(new Date())
                                                    .getTime()
                                    );
                                } else if (jobInfo.getTriggerNextTime() < nowTime) {    // 过期10s内：立即触发一次，当前时间开始计算下次触发时间
                                    //如果下次触发时间currentTime-10s<trigger_next_time< currentTime，则立马触发一次
                                    //计算当前属于第几秒中,把当前立马要执行的任务放到当前时刻的时间轮里
                                    ringSecond = (int)((nowTime/1000)%60);

                                    jobInfo.setTriggerLastTime(jobInfo.getTriggerNextTime());
                                    //计算下一次触发的时间
                                    jobInfo.setTriggerNextTime(
                                            new CronExpression(jobInfo.getJobCron())
                                                    .getNextValidTimeAfter(new Date())
                                                    .getTime()
                                    );
                                } else {    // 未过期：正常触发，递增计算下次触发时间 处于第几秒中
                                    ringSecond = (int)((jobInfo.getTriggerNextTime()/1000)%60);

                                    jobInfo.setTriggerLastTime(jobInfo.getTriggerNextTime());
                                    //计算下一次触发的时间
                                    jobInfo.setTriggerNextTime(
                                            new CronExpression(jobInfo.getJobCron())
                                                    .getNextValidTimeAfter(new Date(jobInfo.getTriggerNextTime()))
                                                    .getTime()
                                    );
                                }
                                if (ringSecond == -1) {
                                    continue;
                                }

                                // push async ring
                                List<Integer> ringItemData = ringData.get(ringSecond);
                                if (ringItemData == null) {
                                    ringItemData = new ArrayList<Integer>();
                                    ringData.put(ringSecond, ringItemData);
                                }
                                ringItemData.add(jobInfo.getId());

                                logger.debug(">>>>>>>>>>> xxl-job, push time-ring : " + ringSecond + " = " + Arrays.asList(ringItemData) );
                            }

                            // 3、更新trigger信息
                            for (XxlJobInfo jobInfo: scheduleList) {
                                XxlJobAdminConfig.getAdminConfig().getXxlJobInfoDao().scheduleUpdate(jobInfo);
                            }

                        }

                        // tx stop

                        conn.commit();
                    } catch (Exception e) {
                        if (!toStop) {
                            logger.error(">>>>>>>>>>> xxl-job, JobScheduleHelper#scheduleThread error:{}", e);
                        }
                    } finally {
                        if (conn != null) {
                            try {
                                conn.close();
                            } catch (SQLException e) {
                            }
                        }
                        if (null != preparedStatement) {
                            try {
                                preparedStatement.close();
                            } catch (SQLException ignore) {
                            }
                        }
                    }

                    // 随机休眠1s内
                    try {
                        TimeUnit.MILLISECONDS.sleep(500+new Random().nextInt(500));
                    } catch (InterruptedException e) {
                        if (!toStop) {
                            logger.error(e.getMessage(), e);
                        }
                    }

                }
                logger.info(">>>>>>>>>>> xxl-job, JobScheduleHelper#scheduleThread stop");
            }
        });
        scheduleThread.setDaemon(true);
        scheduleThread.setName("xxl-job, admin JobScheduleHelper#scheduleThread");
        scheduleThread.start();

        /***
         * 睡眠随机的毫秒
         */
        // ring thread
        ringThread = new Thread(new Runnable() {
            @Override
            public void run() {

                try {
                    TimeUnit.MILLISECONDS.sleep(System.currentTimeMillis()%1000 );
                } catch (InterruptedException e) {
                    if (!toStop) {
                        logger.error(e.getMessage(), e);
                    }
                }

                int lastSecond = -1;
                while (!toStop) {

                    try {
                        // second data
                        List<Integer> ringItemData = new ArrayList<>();
                        //获得当前所属的秒钟：比如当前是 1分钟内的第35s
                        int nowSecond = (int)((System.currentTimeMillis()/1000)%60);   // 避免处理耗时太长，跨过刻度；
                        if (lastSecond == -1) {
                            lastSecond = (nowSecond+59)%60;
                        }
                        //遍历时间轮ringData，从上一次遍历的时间节点索引lastSecond继续往后遍历，直到遍历到当前时间节点nowSecond暂停(因为后面的还没到执行的时间点)
                        //比如上次调度到时间轮里的第28s。那么就继续从25s开始遍历，直到索引位置为35s的时间节点则暂停(因为当前才是1分钟内的第35s，那35s的时间节点之后的都还没到触发调度的点)
                        for (int i = 1; i <=60; i++) {
                            int secondItem = (lastSecond+i)%60;
                            //从时间轮里根据坐标获得对应时间节点里的待执行的任务
                            List<Integer> tmpData = ringData.remove(secondItem);
                            if (tmpData != null) {
                                ringItemData.addAll(tmpData);
                            }

                            if (secondItem == nowSecond) {
                                break;
                            }
                        }
                        //记录当前调度的时间轮中的刻度，后面就从当前刻度继续往后遍历
                        lastSecond = nowSecond;


                        logger.debug(">>>>>>>>>>> xxl-job, time-ring beat : " + nowSecond + " = " + Arrays.asList(ringItemData) );
                        if (ringItemData!=null && ringItemData.size()>0) {
                            // do trigger
                            for (int jobId: ringItemData) {
                                // do trigger
                                JobTriggerPoolHelper.trigger(jobId, TriggerTypeEnum.CRON, -1, null, null);
                            }
                            // clear
                            ringItemData.clear();
                        }
                    } catch (Exception e) {
                        if (!toStop) {
                            logger.error(">>>>>>>>>>> xxl-job, JobScheduleHelper#ringThread error:{}", e);
                        }
                    }

                    try {
                        TimeUnit.SECONDS.sleep(1);
                    } catch (InterruptedException e) {
                        if (!toStop) {
                            logger.error(e.getMessage(), e);
                        }
                    }
                }
                logger.info(">>>>>>>>>>> xxl-job, JobScheduleHelper#ringThread stop");
            }
        });
        ringThread.setDaemon(true);
        ringThread.setName("xxl-job, admin JobScheduleHelper#ringThread");
        ringThread.start();
    }

    public void toStop(){
        toStop = true;

        // interrupt and wait
        scheduleThread.interrupt();
        try {
            scheduleThread.join();
        } catch (InterruptedException e) {
            logger.error(e.getMessage(), e);
        }

        // interrupt and wait
        ringThread.interrupt();
        try {
            ringThread.join();
        } catch (InterruptedException e) {
            logger.error(e.getMessage(), e);
        }
    }

}
