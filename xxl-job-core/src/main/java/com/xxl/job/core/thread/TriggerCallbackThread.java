package com.xxl.job.core.thread;

import com.xxl.job.core.biz.AdminBiz;
import com.xxl.job.core.biz.model.HandleCallbackParam;
import com.xxl.job.core.biz.model.ReturnT;
import com.xxl.job.core.enums.RegistryConfig;
import com.xxl.job.core.executor.XxlJobExecutor;
import com.xxl.job.core.log.XxlJobFileAppender;
import com.xxl.job.core.log.XxlJobLogger;
import com.xxl.job.core.util.FileUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * Created by xuxueli on 16/7/22.
 * 启动针对任务结果回调处理线程
 * 执行器通过当前线程向调度器发起回调，返回调度任务执行的结果
 * 不断的从排队队列里提取准备回调调用器，通知执行结果
 *
 * 线程triggerCallbackThread：
 *      1、从队列callBackQueue中获取任务执行结果。
 *      2、回调调度器，通知对应的任务的执行结果。
 *          如果回调失败了，将失败的回调信息记录到回调日志文件里：
 *          比如/data/applogs/xxl-job/jobhandler/callbacklog/xxl-job-callback-1514171109000.log
 * 线程triggerRetryCallbackThread：
 *      在线程triggerCallbackThread中，我们指定，在回调调度器通知执行结果的时候可能会失败，并且会把失败信息记录到/data/applogs/xxl-job/jobhandler/callbacklog目录下。
 *      所以triggerRetryCallbackThread的任务就是不断的扫描/data/applogs/xxl-job/jobhandler/callbacklog目录进行重试通知调度器执行结果。
 */
public class TriggerCallbackThread {
    private static Logger logger = LoggerFactory.getLogger(TriggerCallbackThread.class);

    private static TriggerCallbackThread instance = new TriggerCallbackThread();
    public static TriggerCallbackThread getInstance(){
        return instance;
    }

    /**
     * job results callback queue
     *
     * 执行器任务结果的回调队列
     */
    private LinkedBlockingQueue<HandleCallbackParam> callBackQueue = new LinkedBlockingQueue<HandleCallbackParam>();

    /**
     * 任务处理器处理完任务后，会把结果集放到callBackQueue队列中，然后有一个线程会不断的从队列里获取结果进行处理 triggerCallbackThread
     * @param callback
     */
    public static void pushCallBack(HandleCallbackParam callback){
        getInstance().callBackQueue.add(callback);
        logger.debug(">>>>>>>>>>> xxl-job, push callback request, logId:{}", callback.getLogId());
    }

    /**
     * callback thread
     */
    private Thread triggerCallbackThread;
    private Thread triggerRetryCallbackThread;
    private volatile boolean toStop = false;

    /***
     * 不断的从排队队列里提取准备回调调用器，通知执行结果
     * 线程triggerCallbackThread：
     *      1、从队列callBackQueue中获取任务执行结果。
     *      2、回调调度器，通知对应的任务的执行结果。
     *          如果回调失败了，将失败的回调信息记录到回调日志文件里：
     *          比如/data/applogs/xxl-job/jobhandler/callbacklog/xxl-job-callback-1514171109000.log
     * 线程triggerRetryCallbackThread：
     *      在线程triggerCallbackThread中，我们指定，在回调调度器通知执行结果的时候可能会失败，并且会把失败信息记录到/data/applogs/xxl-job/jobhandler/callbacklog目录下。
     *      所以triggerRetryCallbackThread的任务就是不断的扫描/data/applogs/xxl-job/jobhandler/callbacklog目录进行重试通知调度器执行结果。
     */
    public void start() {

        // 检查是否配置了服务端的地址：
        // eg：http://127.0.0.1:8080/xxl-job-admin/api
        if (XxlJobExecutor.getAdminBizList() == null) {
            logger.warn(">>>>>>>>>>> xxl-job, executor callback config fail, adminAddresses is null.");
            return;
        }

        // callback
        triggerCallbackThread = new Thread(new Runnable() {

            @Override
            public void run() {

                // normal callback
                while(!toStop){
                    try {
                        //检查回调队列里是否有回调对象
                        HandleCallbackParam callback = getInstance().callBackQueue.take();
                        if (callback != null) {

                            // callback list param
                            List<HandleCallbackParam> callbackParamList = new ArrayList<HandleCallbackParam>();
                            int drainToNum = getInstance().callBackQueue.drainTo(callbackParamList);
                            callbackParamList.add(callback);

                            // callback, will retry if error
                            if (callbackParamList!=null && callbackParamList.size()>0) {
                                //回调调度器执行结果
                                doCallback(callbackParamList);
                            }
                        }
                    } catch (Exception e) {
                        if (!toStop) {
                            logger.error(e.getMessage(), e);
                        }
                    }
                }
                //走到这边，说明当前执行器停止了，这时候要把剩余的执行结果回调调度器AdminImpl,并通知调度器
                // last callback
                try {
                    List<HandleCallbackParam> callbackParamList = new ArrayList<HandleCallbackParam>();
                    int drainToNum = getInstance().callBackQueue.drainTo(callbackParamList);
                    if (callbackParamList!=null && callbackParamList.size()>0) {
                        doCallback(callbackParamList);
                    }
                } catch (Exception e) {
                    if (!toStop) {
                        logger.error(e.getMessage(), e);
                    }
                }
                logger.info(">>>>>>>>>>> xxl-job, executor callback thread destory.");

            }
        });
        triggerCallbackThread.setDaemon(true);
        triggerCallbackThread.setName("xxl-job, executor TriggerCallbackThread");
        triggerCallbackThread.start();


        // retry
        triggerRetryCallbackThread = new Thread(new Runnable() {
            @Override
            public void run() {
                while(!toStop){
                    try {
                        retryFailCallbackFile();
                    } catch (Exception e) {
                        if (!toStop) {
                            logger.error(e.getMessage(), e);
                        }

                    }
                    try {
                        TimeUnit.SECONDS.sleep(RegistryConfig.BEAT_TIMEOUT);
                    } catch (InterruptedException e) {
                        if (!toStop) {
                            logger.error(e.getMessage(), e);
                        }
                    }
                }
                logger.info(">>>>>>>>>>> xxl-job, executor retry callback thread destory.");
            }
        });
        triggerRetryCallbackThread.setDaemon(true);
        triggerRetryCallbackThread.start();

    }

    /***
     * 停止的话，会中断triggerCallbackThread和triggerRetryCallbackThread线程
     */
    public void toStop(){
        toStop = true;
        // stop callback, interrupt and wait
        triggerCallbackThread.interrupt();
        try {
            triggerCallbackThread.join();
        } catch (InterruptedException e) {
            logger.error(e.getMessage(), e);
        }

        // stop retry, interrupt and wait
        triggerRetryCallbackThread.interrupt();
        try {
            triggerRetryCallbackThread.join();
        } catch (InterruptedException e) {
            logger.error(e.getMessage(), e);
        }
    }

    /**
     *  通过AdminBiz回调调度器通知执行结果
     * @param callbackParamList
     */
    private void doCallback(List<HandleCallbackParam> callbackParamList){
        boolean callbackRet = false;
        // callback, will retry if error
        for (AdminBiz adminBiz: XxlJobExecutor.getAdminBizList()) {
            try {
                ReturnT<String> callbackResult = adminBiz.callback(callbackParamList);
                if (callbackResult!=null && ReturnT.SUCCESS_CODE == callbackResult.getCode()) {
                    callbackLog(callbackParamList, "<br>----------- xxl-job job callback finish.");
                    callbackRet = true;
                    break;
                } else {//如果回调没有成功，将回调调度器的结果记录到对应的日志文件，比如：/data/applogs/xxl-job/jobhandler/yyyy-MM-dd/9999.log
                    callbackLog(callbackParamList, "<br>----------- xxl-job job callback fail, callbackResult:" + callbackResult);
                }
            } catch (Exception e) {
                callbackLog(callbackParamList, "<br>----------- xxl-job job callback error, errorMsg:" + e.getMessage());
            }
        }
        //如果回调失败了，将失败的回调信息记录到回调日志文件里，比如/data/applogs/xxl-job/jobhandler/callbacklog/xxl-job-callback-1514171109000.log
        if (!callbackRet) {
            appendFailCallbackFile(callbackParamList);
        }
    }

    /**
     * callback log
     * 将回调调度器的结果记录到对应的日志文件，比如：/data/applogs/xxl-job/jobhandler/yyyy-MM-dd/9999.log
     *  其中9999是任务的某次调度
     */
    private void callbackLog(List<HandleCallbackParam> callbackParamList, String logContent){
        for (HandleCallbackParam callbackParam: callbackParamList) {
            String logFileName = XxlJobFileAppender.makeLogFileName(new Date(callbackParam.getLogDateTim()), callbackParam.getLogId());
            XxlJobFileAppender.contextHolder.set(logFileName);
            XxlJobLogger.log(logContent);
        }
    }


    // ---------------------- fail-callback file ----------------------

    private static String failCallbackFilePath = XxlJobFileAppender.getLogPath().concat(File.separator).concat("callbacklog").concat(File.separator);
    private static String failCallbackFileName = failCallbackFilePath.concat("xxl-job-callback-{x}").concat(".log");

    /***
     * 当执行器要把任务执行的结果回调通知给调度器失败的时候，会触发该方法，记录回调失败的日志
     *
     * @param callbackParamList
     * 1、判断对调度器的回调参数不为空
     * 2、将回调参数序列化
     * 3、将本次回调参数记录到 /data/applogs/xxl-job/jobhandler/callbacklog/xxl-job-callback-1514171109000.log 日志文件中
     */
    private void appendFailCallbackFile(List<HandleCallbackParam> callbackParamList){
        // valid
        if (callbackParamList==null || callbackParamList.size()==0) {
            return;
        }

        // append file
        byte[] callbackParamList_bytes = XxlJobExecutor.getSerializer().serialize(callbackParamList);
        //回调记录回调参数的日志文件：eg:/data/applogs/xxl-job/jobhandler/callbacklog/xxl-job-callback-1514171109000.log
        File callbackLogFile = new File(failCallbackFileName.replace("{x}", String.valueOf(System.currentTimeMillis())));
        if (callbackLogFile.exists()) {
            for (int i = 0; i < 100; i++) {
                callbackLogFile = new File(failCallbackFileName.replace("{x}", String.valueOf(System.currentTimeMillis()).concat("-").concat(String.valueOf(i)) ));
                if (!callbackLogFile.exists()) {
                    break;
                }
            }
        }
        FileUtil.writeFileContent(callbackLogFile, callbackParamList_bytes);
    }

    /***
     * 读取回调失败的目录，遍历每一个失败的文件，不断的进行重试回调通知调度器
     */
    private void retryFailCallbackFile(){

        // valid /data/applogs/xxl-job/jobhandler/callbacklog/
        File callbackLogPath = new File(failCallbackFilePath);
        if (!callbackLogPath.exists()) {
            return;
        }
        if (callbackLogPath.isFile()) {//判断callbackLogPath不能是一个文件
            callbackLogPath.delete();
        }
        //判断callbackLogPath是一个目录，且/data/applogs/xxl-job/jobhandler/callbacklog含有文件，则继续走下去
        if (!(callbackLogPath.isDirectory() && callbackLogPath.list()!=null && callbackLogPath.list().length>0)) {
            return;
        }

        // load and clear file, retry
        for (File callbaclLogFile: callbackLogPath.listFiles()) {
            byte[] callbackParamList_bytes = FileUtil.readFileContent(callbaclLogFile);
            List<HandleCallbackParam> callbackParamList = (List<HandleCallbackParam>) XxlJobExecutor.getSerializer().deserialize(callbackParamList_bytes, HandleCallbackParam.class);

            callbaclLogFile.delete();
            doCallback(callbackParamList);
        }

    }

}
