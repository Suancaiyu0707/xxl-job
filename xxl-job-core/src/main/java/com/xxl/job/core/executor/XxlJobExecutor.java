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
 * 任务的执行器
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

        // 初始化XxlRpcReferenceBean列表，每个XxlRpcReferenceBean是一个内部持有到调度器地址的netty网络连接AdminBiz代理对象的，用于向配置的调度中心admin地址列表中的每个调度中心发送注册执行器的restful请求：http://127.0.0.1:8080/xxl-job-admin/api
        // 执行器向调度器发送心跳，就是通过该XxlRpcReferenceBean每隔30s发送一次
        initAdminBizList(adminAddresses, accessToken);

        // 启动定时清除日志的线程
        JobLogFileCleanThread.getInstance().start(logRetentionDays);

        TriggerCallbackThread.getInstance().start();

        // 启动另一个执行器的执行线程XxlRpcProviderFactory这个类是XXl其他的开源项目，自研RPC
        port = port>0?port: NetUtil.findAvailablePort(9999);
        ip = (ip!=null&&ip.trim().length()>0)?ip: IpUtil.getIp();
        /**
         * 初始化一个基于Netty的服务端，该提供者主要是用于接收执行器对接口ExecutorBiz请求
         * 1、对XxlRpcProviderFactory进行初始化NettyHttpServer（q底层本质还是Netty实现）配置：
         *      传输方式：netty
         *      系列化方式：hessian
         * 2、为Netty服务端绑定一个key-value，指定对对应接口的请求，交给对应的实例处理：
         *      这边指定了该NettyHttpServer会把对ExecutorBiz接口的请求交给对应的绑定的ExecutorBizImpl实例处理
         *
         * 总结：实际上，当前xxlRpcProviderFactory对应的Netty服务端会接收执行器发过来的一种请求：
         *      a、调度器在触发调度任务的时候，会通过Netty向执行器分配执行任务，这个时候任务的调度会被ExecutorBizImpl.run执行
         */
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
     * @param adminAddresses 调度中心的地址(这里是xxl-job的admin)
     * @param accessToken
     * @throws Exception
     * 1、遍历当前执行器配置的调度器的列表
     *      注意:因为调度器支持集群配置，所以当有多个调度器地址时候，用','号分隔
     * 2、遍历每个调度地址，并根据调度系统的地址，为每一个调度系统地址创建AdminBiz代理对象XxlRpcReferenceBean，代理对象底层内部都持有一个Netty的客户端连接，NettyClient的连接方式：
     *      序列化方式：hessian
     *      同步
     *      负载均衡：轮询
     *      连接方式：NettyHttpClient（本质就是一个NettyClient）
     *      地址：http://127.0.0.1:8080/xxl-job-admin
     *      iface: AdminBiz 指定这个请求要请求的接口名称
     *          注意：在XxlJobScheduler.initRpcProvider()初始化的xxlRpcProviderFactory时候，我们维护了一对key-value，其中key的名称就是这个iface。所以这个客户端的后续的请求都会被那个对应AdminBizImpl实例执行
     *   这些指向调度器地址的代理对象（NettyClient）的主要有两个作用：
     *      a、用于定时向调度器注册(包括保持心跳)或移除注册。
     *      b、执行器将执行结果通过该代理对象对象进行回调。
     */
    private void initAdminBizList(String adminAddresses, String accessToken) throws Exception {
        serializer = Serializer.SerializeEnum.HESSIAN.getSerializer();
        if (adminAddresses!=null && adminAddresses.trim().length()>0) {
            //遍历调度器地址列表(可能是集群部署)
            for (String address: adminAddresses.trim().split(",")) {
                if (address!=null && address.trim().length()>0) {
                    //http://127.0.0.1:8080/xxl-job-admin/api
                    String addressUrl = address.concat(AdminBiz.MAPPING);
                    //根据AdminBiz接口创建一个代理对象XxlRpcReferenceBean，这个XxlRpcReferenceBean中持有一个指向调度器地址的Netty客户端连接。当调用adminBiz方法的时候，该代理对象会通过Netty客户端向调度发送tcp请求
                            AdminBiz adminBiz = (AdminBiz) new XxlRpcReferenceBean(
                            NetEnum.NETTY_HTTP,//指定代理对象XxlRpcReferenceBean
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
                    ).getObject();//获得AdminBiz的代理对象
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
     * 调用中心调用过来的request:
     *  requestId = "008c8a78-209b-407a-8bb9-6e090e025870"
     * createMillisTime = 1577712758542
     * accessToken = ""
     * className = "com.xxl.job.core.biz.ExecutorBiz"
     * methodName = "run"
     * parameterTypes = {Class[1]@6060}
     * parameters = {Object[1]@6061}
     * version = null
     *
     *
     * 启动另一个执行器的执行线程XxlRpcProviderFactory这个类是XXl其他的开源项目，自研RPC
     * @param ip eg：192.168.0.103
     * @param port eg：9996
     * @param appName
     * @param accessToken
     * @throws Exception
     * 初始化一个基于Netty的服务端，该提供者主要是用于接收执行器对接口ExecutorBiz请求
     * 1、对XxlRpcProviderFactory进行初始化NettyHttpServer（q底层本质还是Netty实现）配置：
     *      传输方式：netty
     *      系列化方式：hessian
     * 2、为Netty服务端绑定一个key-value，指定对对应接口的请求，交给对应的实例处理：
     *      这边指定了该NettyHttpServer会把对ExecutorBiz接口的请求交给对应的绑定的ExecutorBizImpl实例处理
     *
     * 总结：实际上，当前xxlRpcProviderFactory对应的Netty服务端会接收执行器发过来的一种请求：
     *      a、调度器在触发调度任务的时候，会通过Netty向执行器分配执行任务，这个时候任务的调度会被ExecutorBizImpl.run执行
     */
    private void initRpcProvider(String ip, int port, String appName, String accessToken) throws Exception {

        // init, provider factory 初始化提供者工厂
        String address = IpUtil.getIpPort(ip, port);//172.17.208.173:9999
        Map<String, String> serviceRegistryParam = new HashMap<String, String>();
        serviceRegistryParam.put("appName", appName);//xxl-job-executor-sample
        serviceRegistryParam.put("address", address);
        // 初始化提供者工厂
        xxlRpcProviderFactory = new XxlRpcProviderFactory();
        xxlRpcProviderFactory.initConfig(NetEnum.NETTY_HTTP,
                Serializer.SerializeEnum.HESSIAN.getSerializer(),
                ip, port, accessToken,
                ExecutorServiceRegistry.class,//xxlRpcProviderFactory.start()时候会调用该类的start方法
                serviceRegistryParam);

        // 增加服务接口和服务实现,供给调用中心调用
        xxlRpcProviderFactory.addService(ExecutorBiz.class.getName(), null, new ExecutorBizImpl());

        // start，这里的内部会触发ExecutorServiceRegistry的start()凡方法
        xxlRpcProviderFactory.start();

    }

    /***
     * 启动一个向调度器注册和取消注册管理器
     */
    public static class ExecutorServiceRegistry extends ServiceRegistry {
        /**
         * 启动向调度器注册
         * @param param
         */
        @Override
        public void start(Map<String, String> param) {
            // 开始将执行器本身注册到调度器上，并每隔30s更新发送心跳
            ExecutorRegistryThread.getInstance().start(param.get("appName"), param.get("address"));
        }

        /***
         * 停止向调度器注册
         */
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

    /***
     * 关闭用于向调度器请求的RPC网络连接
     */
    private void stopRpcProvider() {
        // stop provider factory
        try {
            xxlRpcProviderFactory.stop();
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        }
    }


    // ---------------------- job handler repository ----------------------
    /***
     * 用于缓存任务处理器名称和任务处理器的映射关系，任务处理器名称不能重复。
     * 一个执行器下面可能有多个不同的任务处理器，每个处理器负责处理不同的任务
     */
    private static ConcurrentHashMap<String, IJobHandler> jobHandlerRepository = new ConcurrentHashMap<String, IJobHandler>();

    /***
     * 缓存本地启动的任务处理器，会缓存到内存jobHandlerRepository里（一个执行器下面可能挂着很多任务处理器，用于处理不同的任务）
     * @param name 执行器名称
     * @param jobHandler 执行器
     * @return
     */
    public static IJobHandler registJobHandler(String name, IJobHandler jobHandler){
        logger.info(">>>>>>>>>>> xxl-job register jobhandler success, name:{}, jobHandler:{}", name, jobHandler);
        return jobHandlerRepository.put(name, jobHandler);
    }

    /**
     * xxl-job执行器端根据任务处理器名称获得对应的任务处理器（一个执行器下面可能挂着很多任务处理器，用于处理不同的任务）
     * @param name
     * @return
     */
    public static IJobHandler loadJobHandler(String name){
        return jobHandlerRepository.get(name);
    }


    // ---------------------- job thread repository ----------------------
    /***
     * 用于缓存任务处理器需要处理的调度任务id和任务处理器对应的线程的映射关系，
     * 一个执行器下面可能有多个不同的任务处理器，每个处理器负责处理不同的任务id
     */
    private static ConcurrentHashMap<Integer, JobThread> jobThreadRepository = new ConcurrentHashMap<Integer, JobThread>();

    /***
     * 注册一个线程，绑定一个任务id和一个任务处理器，这样对应的调度任务会由对应的任务处理器的线程进行执行
     * @param jobId 任务id
     * @param handler 实现IJobHandler的handler,通常由客户端提供
     * @param removeOldReason
     * @return
     */
    public static JobThread registJobThread(int jobId, IJobHandler handler, String removeOldReason){
        //根据任务id和任务处理器创建一个线程JobThread
        JobThread newJobThread = new JobThread(jobId, handler);
        //启动JobThread，这个JobThread会循环的从它负责的队列里拉取待执行的调度任务进行执行
        newJobThread.start();
        logger.info(">>>>>>>>>>> xxl-job regist JobThread success, jobId:{}, handler:{}", new Object[]{jobId, handler});
        //维护任务id和执行任务的线程的映射关系，这样指定的调度任务会交由对应绑定的线程去执行
        JobThread oldJobThread = jobThreadRepository.put(jobId, newJobThread);	// putIfAbsent | oh my god, map's put method return the old value!!!
        if (oldJobThread != null) {
            oldJobThread.toStop(removeOldReason);
            oldJobThread.interrupt();
        }

        return newJobThread;
    }

    /***
     * 执行器移除一个任务
     * @param jobId
     * @param removeOldReason
     * 1、移除缓存中任务对应的线程JobThread
     * 2、修改这个被移除的线程的状态属性。
     * 3、中断这个正在执行的线程（注意这边只是中断，并不是强制停止）
     */
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
