package com.xxl.job.admin.core.thread;

import com.xxl.job.admin.core.conf.XxlJobAdminConfig;
import com.xxl.job.admin.core.model.XxlJobGroup;
import com.xxl.job.admin.core.model.XxlJobInfo;
import com.xxl.job.admin.core.model.XxlJobLog;
import com.xxl.job.admin.core.trigger.TriggerTypeEnum;
import com.xxl.job.admin.core.util.I18nUtil;
import com.xxl.job.core.biz.model.ReturnT;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.mail.javamail.MimeMessageHelper;

import javax.mail.internet.MimeMessage;
import java.text.MessageFormat;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * job monitor instance
 *
 * @author xuxueli 2015-9-1 18:05:56
 * 主要启动一个线程monitorThread用于监控任务失败告警：
 * 	1、每隔10s检查一次
 * 	2、每次从数据库调度结果日志表中获取最近的1000条失败的日志。
 * 	3、检查每个失败日志所对应的调度任务的重试情况：
 * 		如果重试次数还没重试完，则继续重试触发。
 * 		如果重试次数全部重试完，则准备发送告警邮件
 *  4、发送失败告警邮件
 *  	如果调度任务配置了告警邮箱，则开始发送告警邮件，并更新执行日志中的告警状态
 *  	如果调度任务未配置告警邮箱，则直接将执行日志中的告警状态更新为无需告警。
 *
 *
 */
public class JobFailMonitorHelper {
	private static Logger logger = LoggerFactory.getLogger(JobFailMonitorHelper.class);
	
	private static JobFailMonitorHelper instance = new JobFailMonitorHelper();
	public static JobFailMonitorHelper getInstance(){
		return instance;
	}

	// ---------------------- monitor ----------------------

	private Thread monitorThread;
	private volatile boolean toStop = false;
	public void start(){
		monitorThread = new Thread(new Runnable() {

			@Override
			public void run() {

				// monitor
				while (!toStop) {
					try {
						//获得最近执行失败的log.id,每次查询1000条
						List<Integer> failLogIds = XxlJobAdminConfig.getAdminConfig().getXxlJobLogDao().findFailJobLogIds(1000);
						if (failLogIds!=null && !failLogIds.isEmpty()) {
							for (int failLogId: failLogIds) {

								// 锁住这些日志记录：0-默认、1-无需告警、2-告警成功、3-告警失败
								int lockRet = XxlJobAdminConfig.getAdminConfig().getXxlJobLogDao().updateAlarmStatus(failLogId, 0, -1);
								if (lockRet < 1) {
									continue;
								}
								//循环遍历获得失败的日志XxlJobLog,并根据jobId获得每个执行任务XxlJobInfo
								XxlJobLog log = XxlJobAdminConfig.getAdminConfig().getXxlJobLogDao().load(failLogId);
								XxlJobInfo info = XxlJobAdminConfig.getAdminConfig().getXxlJobInfoDao().loadById(log.getJobId());

								// 如果当前的失败日志里记录的剩余失败重试的次数，则重新触发这个调度任务。同时更新失败重试次数
								if (log.getExecutorFailRetryCount() > 0) {//如果任务配置了失败重试，则重试触发
									JobTriggerPoolHelper.trigger(log.getJobId(), TriggerTypeEnum.RETRY, (log.getExecutorFailRetryCount()-1), log.getExecutorShardingParam(), null);
									String retryMsg = "<br><br><span style=\"color:#F39C12;\" > >>>>>>>>>>>"+ I18nUtil.getString("jobconf_trigger_type_retry") +"<<<<<<<<<<< </span><br>";
									log.setTriggerMsg(log.getTriggerMsg() + retryMsg);
									XxlJobAdminConfig.getAdminConfig().getXxlJobLogDao().updateTriggerInfo(log);
								}

								// 走到这一步，对于配置了重试的任务意味着所有的重试都失败了
								int newAlarmStatus = 0;		// 告警状态：0-默认、-1=锁定状态、1-无需告警、2-告警成功、3-告警失败
								//检查调度任务配置的告警的邮箱，如果调度任务配置了告警邮箱，则要向指定邮箱发送告警邮件
								if (info!=null && info.getAlarmEmail()!=null && info.getAlarmEmail().trim().length()>0) {
									boolean alarmResult = true;
									try {
										//发送告警邮件并返沪i 结果
										alarmResult = failAlarm(info, log);
									} catch (Exception e) {
										alarmResult = false;
										logger.error(e.getMessage(), e);
									}
									//处理告警邮件的发送结果状态
									newAlarmStatus = alarmResult?2:3;
								} else {//如果没有配置邮箱，则无需告警
									newAlarmStatus = 1;
								}
								//更新当前错误日志的告警状态
								XxlJobAdminConfig.getAdminConfig().getXxlJobLogDao().updateAlarmStatus(failLogId, -1, newAlarmStatus);
							}
						}

						TimeUnit.SECONDS.sleep(10);
					} catch (Exception e) {
						if (!toStop) {
							logger.error(">>>>>>>>>>> xxl-job, job fail monitor thread error:{}", e);
						}
					}
				}

				logger.info(">>>>>>>>>>> xxl-job, job fail monitor thread stop");

			}
		});
		monitorThread.setDaemon(true);
		monitorThread.setName("xxl-job, admin JobFailMonitorHelper");
		monitorThread.start();
	}

	public void toStop(){
		toStop = true;
		// interrupt and wait
		monitorThread.interrupt();
		try {
			monitorThread.join();
		} catch (InterruptedException e) {
			logger.error(e.getMessage(), e);
		}
	}


	// ---------------------- alarm ----------------------

	// email alarm template
	private static final String mailBodyTemplate = "<h5>" + I18nUtil.getString("jobconf_monitor_detail") + "：</span>" +
			"<table border=\"1\" cellpadding=\"3\" style=\"border-collapse:collapse; width:80%;\" >\n" +
			"   <thead style=\"font-weight: bold;color: #ffffff;background-color: #ff8c00;\" >" +
			"      <tr>\n" +
			"         <td width=\"20%\" >"+ I18nUtil.getString("jobinfo_field_jobgroup") +"</td>\n" +
			"         <td width=\"10%\" >"+ I18nUtil.getString("jobinfo_field_id") +"</td>\n" +
			"         <td width=\"20%\" >"+ I18nUtil.getString("jobinfo_field_jobdesc") +"</td>\n" +
			"         <td width=\"10%\" >"+ I18nUtil.getString("jobconf_monitor_alarm_title") +"</td>\n" +
			"         <td width=\"40%\" >"+ I18nUtil.getString("jobconf_monitor_alarm_content") +"</td>\n" +
			"      </tr>\n" +
			"   </thead>\n" +
			"   <tbody>\n" +
			"      <tr>\n" +
			"         <td>{0}</td>\n" +
			"         <td>{1}</td>\n" +
			"         <td>{2}</td>\n" +
			"         <td>"+ I18nUtil.getString("jobconf_monitor_alarm_type") +"</td>\n" +
			"         <td>{3}</td>\n" +
			"      </tr>\n" +
			"   </tbody>\n" +
			"</table>";

	/**
	 * fail alarm
	 *
	 * @param jobLog
	 */
	private boolean failAlarm(XxlJobInfo info, XxlJobLog jobLog){
		boolean alarmResult = true;

		// send monitor email
		if (info!=null && info.getAlarmEmail()!=null && info.getAlarmEmail().trim().length()>0) {

			// alarmContent
			String alarmContent = "Alarm Job LogId=" + jobLog.getId();
			if (jobLog.getTriggerCode() != ReturnT.SUCCESS_CODE) {
				alarmContent += "<br>TriggerMsg=<br>" + jobLog.getTriggerMsg();
			}
			if (jobLog.getHandleCode()>0 && jobLog.getHandleCode() != ReturnT.SUCCESS_CODE) {
				alarmContent += "<br>HandleCode=" + jobLog.getHandleMsg();
			}

			// email info
			XxlJobGroup group = XxlJobAdminConfig.getAdminConfig().getXxlJobGroupDao().load(Integer.valueOf(info.getJobGroup()));
			String personal = I18nUtil.getString("admin_name_full");
			String title = I18nUtil.getString("jobconf_monitor");
			String content = MessageFormat.format(mailBodyTemplate,
					group!=null?group.getTitle():"null",
					info.getId(),
					info.getJobDesc(),
					alarmContent);

			Set<String> emailSet = new HashSet<String>(Arrays.asList(info.getAlarmEmail().split(",")));
			for (String email: emailSet) {

				// make mail
				try {
					MimeMessage mimeMessage = XxlJobAdminConfig.getAdminConfig().getMailSender().createMimeMessage();

					MimeMessageHelper helper = new MimeMessageHelper(mimeMessage, true);
					helper.setFrom(XxlJobAdminConfig.getAdminConfig().getEmailUserName(), personal);
					helper.setTo(email);
					helper.setSubject(title);
					helper.setText(content, true);

					XxlJobAdminConfig.getAdminConfig().getMailSender().send(mimeMessage);
				} catch (Exception e) {
					logger.error(">>>>>>>>>>> xxl-job, job fail alarm email send error, JobLogId:{}", jobLog.getId(), e);

					alarmResult = false;
				}

			}
		}

		// do something, custom alarm strategy, such as sms


		return alarmResult;
	}

}
