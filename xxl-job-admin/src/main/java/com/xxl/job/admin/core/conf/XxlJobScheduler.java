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
 */
@Configuration
public class XxlJobScheduler implements InitializingBean, DisposableBean {
    private static final Logger logger = LoggerFactory.getLogger(XxlJobScheduler.class);

    /***
     * 实例初始化完成后，会调用该方法
     * @throws Exception
     */
    @Override
    public void afterPropertiesSet() throws Exception {
        // init i18n 初始化国际化标准
        initI18n();

        // 任务注册的监控启动
        JobRegistryMonitorHelper.getInstance().start();

        // 任务失败监控的启动
        JobFailMonitorHelper.getInstance().start();

        // 启动一个nettyServer用于接收执行器的请求和注册
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

    private void initI18n(){
        for (ExecutorBlockStrategyEnum item:ExecutorBlockStrategyEnum.values()) {
            item.setTitle(I18nUtil.getString("jobconf_block_".concat(item.name())));
        }
    }

    // ---------------------- admin rpc provider (no server version) ----------------------
    private static ServletServerHandler servletServerHandler;
    private void initRpcProvider(){
        // init
        XxlRpcProviderFactory xxlRpcProviderFactory = new XxlRpcProviderFactory();
        xxlRpcProviderFactory.initConfig(
                NetEnum.NETTY_HTTP,
                Serializer.SerializeEnum.HESSIAN.getSerializer(),
                null,
                0,
                XxlJobAdminConfig.getAdminConfig().getAccessToken(),
                null,
                null);

        // add services
        xxlRpcProviderFactory.addService(AdminBiz.class.getName(), null, XxlJobAdminConfig.getAdminConfig().getAdminBiz());

        // servlet handler
        servletServerHandler = new ServletServerHandler(xxlRpcProviderFactory);
    }
    private void stopRpcProvider() throws Exception {
        XxlRpcInvokerFactory.getInstance().stop();
    }
    public static void invokeAdminService(HttpServletRequest request, HttpServletResponse response) throws IOException, ServletException {
        servletServerHandler.handle(null, request, response);
    }


    // ---------------------- executor-client ----------------------
    private static ConcurrentHashMap<String, ExecutorBiz> executorBizRepository = new ConcurrentHashMap<String, ExecutorBiz>();

    /***
     * 根据地址获得对应的业务执行器 ExecutorBiz
     * @param address 执行器地址：192.168.0.103:9999
     * @return
     * @throws Exception
     */
    public static ExecutorBiz getExecutorBiz(String address) throws Exception {
        // 校验执行器的地址是为空
        if (address==null || address.trim().length()==0) {
            return null;
        }

        // 每隔执行器地址都对应一个ExecutorBiz，用于发送网络请求
        address = address.trim();
        ExecutorBiz executorBiz = executorBizRepository.get(address);
        if (executorBiz != null) {
            return executorBiz;
        }

        // set-cache
        executorBiz = (ExecutorBiz) new XxlRpcReferenceBean(
                NetEnum.NETTY_HTTP, // 网络传输方式
                Serializer.SerializeEnum.HESSIAN.getSerializer(), //序列化方式
                CallType.SYNC, //同步/异步
                LoadBalance.ROUND,//负载均衡方式
                ExecutorBiz.class,
                null, //版本
                5000, //超时时间
                address,//执行器地址
                XxlJobAdminConfig.getAdminConfig().getAccessToken(), //认证token
                null,
                null).getObject();
        //存放到内存里
        executorBizRepository.put(address, executorBiz);
        return executorBiz;
    }

}
