package com.xxl.job.core.executor.impl;

import com.xxl.job.core.executor.XxlJobExecutor;
import com.xxl.job.core.glue.GlueFactory;
import com.xxl.job.core.handler.IJobHandler;
import com.xxl.job.core.handler.annotation.JobHandler;
import org.springframework.beans.BeansException;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;

import java.util.Map;

/**
 * 执行器要依赖此core包，所以会初始化XxlJobSpringExecutor对象
 *
 * @author xuxueli 2018-11-01 09:24:52
 * 实现ApplicationContextAware接口，用来保存spring的上下文信息
 * 会遍历spring容器，并把容器里所有的IJobHandler执行器都放到缓存里
 */
public class XxlJobSpringExecutor extends XxlJobExecutor implements ApplicationContextAware {


    @Override
    public void start() throws Exception {

        // 会遍历spring容器，并把容器里所有的IJobHandler执行器都放到缓存里
        initJobHandlerRepository(applicationContext);

        // 初始化一个SpringGlueFactory
        GlueFactory.refreshInstance(1);


        // super start
        super.start();
    }

    /***
     *
     * @param applicationContext
     * 1、初始化spring容器中的执行器 JobHandler
     * 2、将spring容器中的执行器 JobHandler注册到本地内存jobHandlerRepository中，名称不能重复
     */
    private void initJobHandlerRepository(ApplicationContext applicationContext){
        if (applicationContext == null) {
            return;
        }

        // 从容器里获取JobHandler的实现类
        Map<String, Object> serviceBeanMap = applicationContext.getBeansWithAnnotation(JobHandler.class);

        if (serviceBeanMap!=null && serviceBeanMap.size()>0) {
            //遍历将 JobHandler 实现类存放在内存里，
            for (Object serviceBean : serviceBeanMap.values()) {
                if (serviceBean instanceof IJobHandler){
                    String name = serviceBean.getClass().getAnnotation(JobHandler.class).value();
                    IJobHandler handler = (IJobHandler) serviceBean;
                    if (loadJobHandler(name) != null) {
                        throw new RuntimeException("xxl-job jobhandler naming conflicts.");
                    }
                    //注册
                    registJobHandler(name, handler);
                }
            }
        }
    }

    // ---------------------- applicationContext ----------------------
    private static ApplicationContext applicationContext;
    @Override
    public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
        this.applicationContext = applicationContext;
    }
    public static ApplicationContext getApplicationContext() {
        return applicationContext;
    }

}
