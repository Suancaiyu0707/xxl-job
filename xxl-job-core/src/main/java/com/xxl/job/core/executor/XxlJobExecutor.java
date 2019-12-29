package com.xxl.job.core.executor;

import com.xxl.job.core.biz.AdminBiz;
import com.xxl.job.core.biz.ExecutorBiz;
import com.xxl.job.core.biz.impl.ExecutorBizImpl;
import com.xxl.job.core.handler.IJobHandler;
import com.xxl.job.core.log.XxlJobFileAppender;
import com.xxl.job.core.thread.ExecutorRegistryThread;
import com.xxl.job.core.thread.JobLogFileCleanThread;
import com.xxl.job.core.thread.JobThread;
import com.xxl.job.core.thread.TriggerCallbackThread;
import com.xxl.rpc.registry.ServiceRegistry;
import com.xxl.rpc.remoting.invoker.XxlRpcInvokerFactory;
import com.xxl.rpc.remoting.invoker.call.CallType;
import com.xxl.rpc.remoting.invoker.reference.XxlRpcReferenceBean;
import com.xxl.rpc.remoting.invoker.route.LoadBalance;
import com.xxl.rpc.remoting.net.NetEnum;
import com.xxl.rpc.remoting.provider.XxlRpcProviderFactory;
import com.xxl.rpc.serialize.Serializer;
import com.xxl.rpc.util.IpUtil;
import com.xxl.rpc.util.NetUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by xuxueli on 2016/3/2 21:14.
 * 通过XxlJobExecutor.start启动执行器
 */
public class XxlJobExecutor  {
    private static final Logger logger = LoggerFactory.getLogger(XxlJobExecutor.class);

    // ---------------------- param ----------------------
    private String adminAddresses;//http://127.0.0.1:8080/xxl-job-admin
    private String appName;
    private String ip;
    private int port;
    private String accessToken;
    private String logPath;// /data/applogs/xxl-job/jobhandler
    private int logRetentionDays;

    public void setAdminAddresses(String adminAddresses) {
        this.adminAddresses = adminAddresses;
    }
    public void setAppName(String appName) {
        this.appName = appName;
    }
    public void setIp(String ip) {
        this.ip = ip;
    }
    public void setPort(int port) {
        this.port = port;
    }
    public void setAccessToken(String accessToken) {
        this.accessToken = accessToken;
    }
    public void setLogPath(String logPath) {
        this.logPath = logPath;
    }
    public void setLogRetentionDays(int logRetentionDays) {
        this.logRetentionDays = logRetentionDays;
    }


    // ---------------------- start + stop ----------------------

    /***
     *
     * @throws Exception
     * 1、初始化日志文件的配置（默认/data/applogs/xxl-job/jobhandler"）
     * 2、向配置的调度中心admin地址发送一个注册执行器的restful请求，用于初始化调用调度中心的client列表
     * 3、启动定时清除日志的线程
     * 4、初始化执行器的服务
     * 5、初始化执行器服务，一个以Netty为网络传输的方式的RPC调用接口，用于接收调度中心的回调请求
     */
    public void start() throws Exception {

        // 初始化日志文件的配置（默认/data/applogs/xxl-job/jobhandler"）
        XxlJobFileAppender.initLogPath(logPath);

        // 向配置的调度中心admin地址发送一个注册执行器的restful请求：http://127.0.0.1:8080/xxl-job-admin/api
        initAdminBizList(adminAddresses, accessToken);


        // 启动定时清除日志的线程
        JobLogFileCleanThread.getInstance().start(logRetentionDays);

        TriggerCallbackThread.getInstance().start();

        // 启动另一个执行器的执行线程XxlRpcProviderFactory这个类是XXl其他的开源项目，自研RPC
        port = port>0?port: NetUtil.findAvailablePort(9999);
        ip = (ip!=null&&ip.trim().length()>0)?ip: IpUtil.getIp();
        //初始化一个以Netty为网络传输的方式的RPC调用接口，用于接收调度中心的回调请求
        initRpcProvider(ip, port, appName, accessToken);
    }
    public void destroy(){
        // destory jobThreadRepository
        if (jobThreadRepository.size() > 0) {
            for (Map.Entry<Integer, JobThread> item: jobThreadRepository.entrySet()) {
                removeJobThread(item.getKey(), "web container destroy and kill the job.");
            }
            jobThreadRepository.clear();
        }
        jobHandlerRepository.clear();


        // destory JobLogFileCleanThread
        JobLogFileCleanThread.getInstance().toStop();

        // destory TriggerCallbackThread
        TriggerCallbackThread.getInstance().toStop();

        // destory executor-server
        stopRpcProvider();

        // destory invoker
        stopInvokerFactory();
    }


    // ---------------------- admin-client (rpc invoker) ----------------------
    private static List<AdminBiz> adminBizList;
    private static Serializer serializer;

    /***
     *  初始化 admin-client 向adminBizList字段中放入XxlRpcReferenceBean返回的代理类
     * @param adminAddresses 调度中心的地址(这里是xxl-job的admin)
     * @param accessToken
     * @throws Exception
     */
    private void initAdminBizList(String adminAddresses, String accessToken) throws Exception {
        serializer = Serializer.SerializeEnum.HESSIAN.getSerializer();
        if (adminAddresses!=null && adminAddresses.trim().length()>0) {
            for (String address: adminAddresses.trim().split(",")) {
                if (address!=null && address.trim().length()>0) {
                    //http://127.0.0.1:8080/xxl-job-admin/api
                    String addressUrl = address.concat(AdminBiz.MAPPING);

                    AdminBiz adminBiz = (AdminBiz) new XxlRpcReferenceBean(
                            NetEnum.NETTY_HTTP,
                            serializer,
                            CallType.SYNC,
                            LoadBalance.ROUND,
                            AdminBiz.class,
                            null,
                            10000,
                            addressUrl,
                            accessToken,
                            null,
                            null
                    ).getObject();

                    if (adminBizList == null) {
                        adminBizList = new ArrayList<AdminBiz>();
                    }
                    adminBizList.add(adminBiz);
                }
            }
        }
    }
    private void stopInvokerFactory(){
        // stop invoker factory
        try {
            XxlRpcInvokerFactory.getInstance().stop();
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        }
    }
    public static List<AdminBiz> getAdminBizList(){
        return adminBizList;
    }
    public static Serializer getSerializer() {
        return serializer;
    }


    // ---------------------- executor-server (rpc provider) ----------------------
    private XxlRpcProviderFactory xxlRpcProviderFactory = null;

    /**
     * 启动另一个执行器的执行线程XxlRpcProviderFactory这个类是XXl其他的开源项目，自研RPC
     * @param ip eg：192.168.0.103
     * @param port eg：9996
     * @param appName
     * @param accessToken
     * @throws Exception
     * 启动了一个以netty作为通讯模型、Hessian作为序列化方式的、ExecutorServiceRegistry作为注册逻辑实现类的服务提供端。
     */
    private void initRpcProvider(String ip, int port, String appName, String accessToken) throws Exception {

        // init, provider factory 初始化提供者工厂
        String address = IpUtil.getIpPort(ip, port);
        Map<String, String> serviceRegistryParam = new HashMap<String, String>();
        serviceRegistryParam.put("appName", appName);
        serviceRegistryParam.put("address", address);
        // 初始化提供者工厂
        xxlRpcProviderFactory = new XxlRpcProviderFactory();
        xxlRpcProviderFactory.initConfig(NetEnum.NETTY_HTTP, Serializer.SerializeEnum.HESSIAN.getSerializer(), ip, port, accessToken, ExecutorServiceRegistry.class, serviceRegistryParam);

        // 增加服务接口和服务实现,供给调用中心调用
        xxlRpcProviderFactory.addService(ExecutorBiz.class.getName(), null, new ExecutorBizImpl());

        // start
        xxlRpcProviderFactory.start();

    }

    public static class ExecutorServiceRegistry extends ServiceRegistry {

        @Override
        public void start(Map<String, String> param) {
            // start registry
            ExecutorRegistryThread.getInstance().start(param.get("appName"), param.get("address"));
        }
        @Override
        public void stop() {
            // stop registry
            ExecutorRegistryThread.getInstance().toStop();
        }

        @Override
        public boolean registry(Set<String> keys, String value) {
            return false;
        }
        @Override
        public boolean remove(Set<String> keys, String value) {
            return false;
        }
        @Override
        public Map<String, TreeSet<String>> discovery(Set<String> keys) {
            return null;
        }
        @Override
        public TreeSet<String> discovery(String key) {
            return null;
        }

    }

    private void stopRpcProvider() {
        // stop provider factory
        try {
            xxlRpcProviderFactory.stop();
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        }
    }


    // ---------------------- job handler repository ----------------------
    private static ConcurrentHashMap<String, IJobHandler> jobHandlerRepository = new ConcurrentHashMap<String, IJobHandler>();

    /***
     * 执行器注册的时候，会缓存到内存jobHandlerRepository里
     * @param name 执行器名称
     * @param jobHandler 执行器
     * @return
     */
    public static IJobHandler registJobHandler(String name, IJobHandler jobHandler){
        logger.info(">>>>>>>>>>> xxl-job register jobhandler success, name:{}, jobHandler:{}", name, jobHandler);
        return jobHandlerRepository.put(name, jobHandler);
    }

    /**
     * 根据执行器名称获得对应的执行器
     * @param name
     * @return
     */
    public static IJobHandler loadJobHandler(String name){
        return jobHandlerRepository.get(name);
    }


    // ---------------------- job thread repository ----------------------
    private static ConcurrentHashMap<Integer, JobThread> jobThreadRepository = new ConcurrentHashMap<Integer, JobThread>();

    /***
     * 注册一个线程，
     * @param jobId 任务id
     * @param handler 实现IJobHandler的handler,通常由客户端提供
     * @param removeOldReason
     * @return
     */
    public static JobThread registJobThread(int jobId, IJobHandler handler, String removeOldReason){
        JobThread newJobThread = new JobThread(jobId, handler);
        newJobThread.start();
        logger.info(">>>>>>>>>>> xxl-job regist JobThread success, jobId:{}, handler:{}", new Object[]{jobId, handler});

        JobThread oldJobThread = jobThreadRepository.put(jobId, newJobThread);	// putIfAbsent | oh my god, map's put method return the old value!!!
        if (oldJobThread != null) {
            oldJobThread.toStop(removeOldReason);
            oldJobThread.interrupt();
        }

        return newJobThread;
    }
    public static void removeJobThread(int jobId, String removeOldReason){
        JobThread oldJobThread = jobThreadRepository.remove(jobId);
        if (oldJobThread != null) {
            oldJobThread.toStop(removeOldReason);
            oldJobThread.interrupt();
        }
    }

    /***
     * 根据jobId 从本地内存里获得job已有的Thread
     * @param jobId
     * @return
     */
    public static JobThread loadJobThread(int jobId){
        JobThread jobThread = jobThreadRepository.get(jobId);
        return jobThread;
    }

}
