package com.xxl.job.admin.core.route;

import com.xxl.job.core.biz.model.ReturnT;
import com.xxl.job.core.biz.model.TriggerParam;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * Created by xuxueli on 17/3/10.
 * 路由选择器的抽象类，所有的子类必须实现route，路由选择一个执行器
 */
public abstract class ExecutorRouter {
    protected static Logger logger = LoggerFactory.getLogger(ExecutorRouter.class);

    /***
     *
     * @param triggerParam
     * @param addressList
     * @return
     */
    public abstract ReturnT<String> route(TriggerParam triggerParam, List<String> addressList);

}
