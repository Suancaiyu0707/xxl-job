package com.xxl.job.admin.core.trigger;

import com.xxl.job.admin.core.util.I18nUtil;

/**
 * trigger type enum
 *
 * @author xuxueli 2018-09-16 04:56:41
 * 任务触发的类型
 */
public enum TriggerTypeEnum {
    //页面手动执行
    MANUAL(I18nUtil.getString("jobconf_trigger_type_manual")),
    //定时触发执行
    CRON(I18nUtil.getString("jobconf_trigger_type_cron")),
    //重试执行
    RETRY(I18nUtil.getString("jobconf_trigger_type_retry")),
    //父任务调度执行
    PARENT(I18nUtil.getString("jobconf_trigger_type_parent")),
    API(I18nUtil.getString("jobconf_trigger_type_api"));

    private TriggerTypeEnum(String title){
        this.title = title;
    }
    private String title;
    public String getTitle() {
        return title;
    }

}
