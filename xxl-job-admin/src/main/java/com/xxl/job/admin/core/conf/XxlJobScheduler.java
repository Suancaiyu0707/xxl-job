package com.xxl.job.admin.core.conf;

import com.xxl.job.admin.core.thread.JobFailMonitorHelper;
import com.xxl.job.admin.core.thread.JobRegistryMonitorHelper;
import com.xxl.job.admin.core.thread.JobScheduleHelper;
import com.xxl.job.admin.core.thread.JobTriggerPoolHelper;
import com.xxl.job.admin.core.util.I18nUtil;
import com.xxl.job.core.biz.AdminBiz;
import com.xxl.job.core.biz.ExecutorBiz;
import com.xxl.job.core.enums.ExecutorBlockStrategyEnum;
import com.xxl.rpc.remoting.invoker.XxlRpcInvokerFactory;
import com.xxl.rpc.remoting.invoker.call.CallType;
import com.xxl.rpc.remoting.invoker.reference.XxlRpcReferenceBean;
import com.xxl.rpc.remoting.invoker.route.LoadBalance;
import com.xxl.rpc.remoting.net.NetEnum;
import com.xxl.rpc.remoting.net.impl.servlet.server.ServletServerHandler;
import com.xxl.rpc.remoting.provider.XxlRpcProviderFactory;
import com.xxl.rpc.serialize.Serializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.context.annotation.Configuration;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author xuxueli 2018-10-28 00:18:17
 *
 * XxlJobScheduler在服务启动后，会为实例初始化完成后，调用afterPropertiesSet方法，进行一些准备工作：
 * 1、初始化国际化标准 遍历每个阻塞策略，将title设置为中文（意义不大，只是为了可视化清晰）
 * 2、启动一个线程，每隔30s根据会根据注册表xxl_job_resgiry里的地址列表，自动更新执行器xxl_job_group中自动注册的地址列表。
 * 3、启动一个线程，定时检查检查最近失败的xxlJobLog日志，并根据失败信息以及任务配置，判断是否需要重试或者发送邮件。
 * 4、服务会初始化一个基于Netty网络传输的的RPC服务提供者，该提供者主要是用于接收执行器的请求和注册。并把注册信息维护到xxl_job_resgiry表中。
 *      传输方式：netty
 *      系列化方式：hessian
 * 5、启动一个线程，定时的检查xxl_job_info表，把最近10S内将调度的任务提取出来，并交由JobTriggerPoolHelper进行触发。
 */
@Configuration
public class XxlJobScheduler implements InitializingBean, DisposableBean {
    private static final Logger logger = LoggerFactory.getLogger(XxlJobScheduler.class);

    /***
     * XxlJobScheduler实例化完成后调用
     * 实例初始化完成后，会调用该方法
     * @throws Exception
     * 1、初始化国际化标准 遍历每个阻塞策略，将title设置为中文（意义不大，只是为了可视化清晰）
     * 2、启动一个线程，每隔30s根据会根据注册表xxl_job_resgiry里的地址列表，自动更新执行器xxl_job_group中自动注册的地址列表。
     * 3、启动一个线程，定时检查检查最近失败的xxlJobLog日志，并根据失败信息以及任务配置，判断是否需要重试或者发送邮件。
     * 4、服务会初始化一个基于Netty网络传输的的RPC服务提供者，该提供者主要是用于接收执行器的请求和注册。并把注册信息维护到xxl_job_resgiry表中。
     * 5、启动一个线程，定时的检查xxl_job_info表，把最近10S内将调度的任务提取出来，并交由JobTriggerPoolHelper进行触发。
     */
    @Override
    public void afterPropertiesSet() throws Exception {
        // init i18n 初始化国际化标准 遍历每个阻塞策略，将title设置为中文
        initI18n();

        // 启动一个线程定时每隔30s根据注册表更新执行器信息
        JobRegistryMonitorHelper.getInstance().start();

        // 启动一个线程，检查最近失败的xxlJobLog日志，并根据失败信息以及任务配置，判断是否需要重试或者发送邮件
        JobFailMonitorHelper.getInstance().start();

        /***
         * 执行器调用过来的request：
         *      requestId = "458b2416-3ca3-49a4-b947-8b41a029af6a"
         *      createMillisTime = 1577706301371
         *      accessToken = ""
         *      className = "com.xxl.job.core.biz.AdminBiz"
         *      methodName = "registry"
         *      parameterTypes = {Class[1]@6627}
         *      parameters = {Object[1]@6628}
         *      version = null
         * 初始化一个基于Netty的服务端，该提供者主要是用于接收执行器对接口AdminBiz请求(包括注册、心跳和执行结果回调)
         * 1、对XxlRpcProviderFactory进行初始化NettyHttpServer（q底层本质还是Netty实现）配置：
         *      传输方式：netty
         *      系列化方式：hessian
         * 2、为Netty服务端绑定一个key-value，指定对对应接口的请求，交给对应的实例处理：
         *      这边指定了该NettyHttpServer会把对AdminBiz接口的请求交给对应的绑定的AdminBizImpl实例处理
         *
         * 总结：实际上，当前xxlRpcProviderFactory对应的Netty服务端会接收执行器发过来的两种请求：
         *      a、执行器会每隔30s向当前nettyHttpServer发送registry请求，从而执行器和调度器保持了心跳连接。
         *      b、执行器在执行完调度任务后，会向当前nettyHttpServer发送callback请求，回调通知调度器执行结果。
         */
        initRpcProvider();

        // 启动一个定时任务，不断的从 xxl_job_info 表中提取将要执行的任务，更新下次执行时间的，调用JobTriggerPoolHelper类，来给执行器发送调度任务的
        JobScheduleHelper.getInstance().start();

        logger.info(">>>>>>>>> init xxl-job admin success.");
    }

    @Override
    public void destroy() throws Exception {

        // stop-schedule
        JobScheduleHelper.getInstance().toStop();

        // admin trigger pool stop
        JobTriggerPoolHelper.toStop();

        // admin registry stop
        JobRegistryMonitorHelper.getInstance().toStop();

        // admin monitor stop
        JobFailMonitorHelper.getInstance().toStop();

        // admin-server
        stopRpcProvider();
    }

    // ---------------------- I18n ----------------------
    //遍历每个阻塞策略，将title设置为中文
    private void initI18n(){
        for (ExecutorBlockStrategyEnum item:ExecutorBlockStrategyEnum.values()) {
            item.setTitle(I18nUtil.getString("jobconf_block_".concat(item.name())));
        }
    }

    // ---------------------- admin rpc provider (no server version) ----------------------
    private static ServletServerHandler servletServerHandler;

    /***
     *
     * 执行器调用过来的request：
     *  requestId = "458b2416-3ca3-49a4-b947-8b41a029af6a"
     * createMillisTime = 1577706301371
     * accessToken = ""
     * className = "com.xxl.job.core.biz.AdminBiz"
     * methodName = "registry"
     * parameterTypes = {Class[1]@6627}
     * parameters = {Object[1]@6628}
     * version = null
     *
     *
     * 初始化一个基于Netty的服务端，该提供者主要是用于接收执行器对接口AdminBiz请求(包括注册、心跳和执行结果回调)
     * 1、对XxlRpcProviderFactory进行初始化NettyHttpServer（q底层本质还是Netty实现）配置：
     *      传输方式：netty
     *      系列化方式：hessian
     * 2、为Netty服务端绑定一个key-value，指定对对应接口的请求，交给对应的实例处理：
     *      这边指定了该NettyHttpServer会把对AdminBiz接口的请求交给对应的绑定的AdminBizImpl实例处理
     *
     * 总结：实际上，当前xxlRpcProviderFactory对应的Netty服务端会接收执行器发过来的两种请求：
     *      a、执行器会每隔30s向当前nettyHttpServer发送registry请求，从而执行器和调度器保持了心跳连接。
     *      b、执行器在执行完调度任务后，会向当前nettyHttpServer发送callback请求，回调通知调度器执行结果。
     */
    private void initRpcProvider(){
        // 创建一个 XxlRpcProviderFactory 实例
        XxlRpcProviderFactory xxlRpcProviderFactory = new XxlRpcProviderFactory();
        xxlRpcProviderFactory.initConfig(//初始化一个NettyHttpServer
                NetEnum.NETTY_HTTP,//底层网络连接方式
                Serializer.SerializeEnum.HESSIAN.getSerializer(),//rpc的序列化方式
                null,
                0,
                XxlJobAdminConfig.getAdminConfig().getAccessToken(),
                null,
                null);

        // 为Netty服务端绑定一个key-value，指定对对应接口的请求，交给对应的实例处理
        xxlRpcProviderFactory
                .addService(
                        AdminBiz.class.getName(),
                        null,
                        XxlJobAdminConfig.getAdminConfig().getAdminBiz()//AdminBizImpl实例，真正处理nettyClient请求的对象
                );

        servletServerHandler = new ServletServerHandler(xxlRpcProviderFactory);
    }
    private void stopRpcProvider() throws Exception {
        XxlRpcInvokerFactory.getInstance().stop();
    }

    /***
     * 客户端调用时，由servletServerHandler.handle()调用AdminBiz的方法
     * @param request
     * @param response
     * @throws IOException
     * @throws ServletException
     */
    public static void invokeAdminService(HttpServletRequest request, HttpServletResponse response) throws IOException, ServletException {
        servletServerHandler.handle(null, request, response);
    }


    // ---------------------- executor-client ----------------------
    private static ConcurrentHashMap<String, ExecutorBiz> executorBizRepository = new ConcurrentHashMap<String, ExecutorBiz>();

    /***
     * 根据执行器的地址返回执行器代理对象，用于调用相应的执行进行任务调用
     * @param address 执行器地址：192.168.0.103:9999
     * @return
     * @throws Exception
     * 1、创建一个 ExecutorBiz代理对象，该代理对象底层采用Netty网络连接向执行器地址发起请求，并维护到内存里
     * 2、将执行器映射的带有Netty网络连接执行对象缓存到内存里，每个执行器地址，只需维护一个NettyClient客户端连接即可
     */
    public static ExecutorBiz getExecutorBiz(String address) throws Exception {
        // 校验执行器的地址是为空
        if (address==null || address.trim().length()==0) {
            return null;
        }
        // 每隔执行器地址都对应一个ExecutorBiz，用于发送网络请求
        address = address.trim();
        //检查内存里是否已经维护了该执行器对应的ExecutorBiz，有则直接返回
        ExecutorBiz executorBiz = executorBizRepository.get(address);
        if (executorBiz != null) {
            return executorBiz;
        }
        //创建一个 ExecutorBiz代理对象，该代理对象底层采用Netty网络连接向执行器地址发起请求，并维护到内存里
        // 每个执行器地址，只需维护一个NettyClient客户端连接即可，会缓存到内存里
        //默认采用的是Netty网络连接
        executorBiz = (ExecutorBiz) new XxlRpcReferenceBean(
                NetEnum.NETTY_HTTP, // 网络传输方式
                Serializer.SerializeEnum.HESSIAN.getSerializer(), //序列化方式
                CallType.SYNC, //同步
                LoadBalance.ROUND,//负载均衡方式
                ExecutorBiz.class,
                null, //版本
                5000, //超时时间
                address,//执行器地址
                XxlJobAdminConfig.getAdminConfig().getAccessToken(), //认证token
                null,
                null).getObject();
        //将执行器映射的带有Netty网络连接执行对象缓存到内存里，每个执行器地址，只需维护一个NettyClient客户端连接即可
        executorBizRepository.put(address, executorBiz);
        return executorBiz;
    }

}
