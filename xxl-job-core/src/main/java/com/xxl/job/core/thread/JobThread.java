package com.xxl.job.core.thread;

import com.xxl.job.core.biz.model.HandleCallbackParam;
import com.xxl.job.core.biz.model.ReturnT;
import com.xxl.job.core.biz.model.TriggerParam;
import com.xxl.job.core.executor.XxlJobExecutor;
import com.xxl.job.core.handler.IJobHandler;
import com.xxl.job.core.log.XxlJobFileAppender;
import com.xxl.job.core.log.XxlJobLogger;
import com.xxl.job.core.util.ShardingUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.Collections;
import java.util.Date;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.*;


/**
 * 	一个执行器下面可能有很多个任务处理器。当调度器把要执行器的任务分配给当前执行器时，执行器会根据任务信息，把它交给相应的任务处理器执行器。
 *	每个任务处理器JobHandler都会有一个专门对应的线程JobThread，用于处理调度器分配给当前任务处理器JobHandler的调度任务。
 *	1、调度器会通过RPC请求向执行器分配任务
 *	2、执行器ExecutorBizImpl在接收到调度器发送过来的调度任务后，会根据任务配置的处理器，获取对应的任务线程JobThread，并放到任务线程绑定的队列：triggerQueue，并异步响应给调度器
 *	3、任务线程本身会不停的从队列	triggerQueue弹出待执行的任务，并交给绑定的JobHandler去执行，并返回结果。
 *
 * @author xuxueli 2016-1-16 19:52:47
 *
 */
public class JobThread extends Thread{
	private static Logger logger = LoggerFactory.getLogger(JobThread.class);
	/**
	 * 调度id
	 */
	private int jobId;
	/***
	 * 调度任务真正的任务处理器
	 */
	private IJobHandler handler;
	/***
	 * 每次调度器分发任务的时候，都会先放到对列中，然后循环的从队列里弹出并触发任务执行
	 */
	private LinkedBlockingQueue<TriggerParam> triggerQueue;
	private Set<Integer> triggerLogIdSet;		// avoid repeat trigger for the same TRIGGER_LOG_ID

	private volatile boolean toStop = false;
	private String stopReason;

    private boolean running = false;    // if running job
	private int idleTimes = 0;			// idel times

	/***
	 * 每个执行器会绑定一个JobThread线程，每个线程会绑定对应的 handler类型
	 * @param jobId
	 * @param handler
	 */
	public JobThread(int jobId, IJobHandler handler) {
		this.jobId = jobId;
		this.handler = handler;
		this.triggerQueue = new LinkedBlockingQueue<TriggerParam>();
		this.triggerLogIdSet = Collections.synchronizedSet(new HashSet<Integer>());
	}
	public IJobHandler getHandler() {
		return handler;
	}

    /**
     * 调度器会把任务推到队列里，然后定时线程定时的从队列里拉取任务
     *
     * @param triggerParam
     * @return
     */
	public ReturnT<String> pushTriggerQueue(TriggerParam triggerParam) {
		// 如果定时线程里存在本次调度，则返回重复触发的失败结果，这里判断的是logId，不是jobId，表示某次的触发不能重复被放到待执行的对列triggerQueue中
		if (triggerLogIdSet.contains(triggerParam.getLogId())) {//表示某一个调度任务的某一次重复调用了，所以返回失败
			logger.info(">>>>>>>>>>> repeate trigger job, logId:{}", triggerParam.getLogId());
			return new ReturnT<String>(ReturnT.FAIL_CODE, "repeate trigger job, logId:" + triggerParam.getLogId());
		}
		//记录当前某个调度任务的某次执行的id
		triggerLogIdSet.add(triggerParam.getLogId());
		//将调度任务的此次调度放到队列triggerQueue中，等待线程执行
		triggerQueue.add(triggerParam);
        return ReturnT.SUCCESS;
	}

    /**
     * kill job thread
     *
     * @param stopReason
     */
	public void toStop(String stopReason) {
		/**
		 * Thread.interrupt只支持终止线程的阻塞状态(wait、join、sleep)，
		 * 在阻塞出抛出InterruptedException异常,但是并不会终止运行的线程本身；
		 * 所以需要注意，此处彻底销毁本线程，需要通过共享变量方式；
		 */
		this.toStop = true;
		this.stopReason = stopReason;
	}

    /**
     * is running job
     * @return
     */
    public boolean isRunningOrHasQueue() {
        return running || triggerQueue.size()>0;
    }

	/***
	 * 线程不断的从triggerQueue中执行
	 * 1、不断的从triggerQueue中拉取待执行的任务TriggerParam（最长可阻塞3s）
	 * 2、从triggerLogIdSet中移除调度任务当次的调度记录
	 * 3、根据触发日期创建一个目录，比如/data/applogs/xxl-job/jobhandler/yyyy-MM-dd
	 * 4、在创建的目录下根据logId创建当前任务调度对应的调度记录文件：/data/applogs/xxl-job/jobhandler/yyyy-MM-dd/9999.log
	 * 		该日志文件主要是用来记录某个任务调度的某一次的调度日志。这些日志文件会被JobLogFileCleanThread定时清理。
	 * 5、调用JobHandler开始执行任务：
	 * 		配置了超时时间，则会new一个线程，并通过FutureTask来控制超时时间。
	 * 		没有配置超时时间，则直接交给JobHandler执行。
	 * 6、将执行的日志记录到/data/applogs/xxl-job/jobhandler/yyyy-MM-dd/9999.log日志文件中
	 * 7、将执行结果存放到TriggerCallbackThread的回调队列callBackQueue中，并最终回调调度器通知执行结果。
	 * 注意：
	 * 		如果存在连续90s，都没有从triggerQueue中获取到等待执行的任务，那么可以把这个JobThread从内存移除并中断，这样节省内存，还避免了CPU浪费。
	 *		如果这个处理器在执行过程中被中止了，则记录任务被中止的原因，并通知调度器。
	 *		如果线程在遍历过程中，发生自身被中止了，这个时候它会遍历队列中等待执行的任务，并全部返回执行失败的结果，回调给调度器。
	 */
    @Override
	public void run() {

    	// init
    	try {
    		//初始化handler信息
			handler.init();
		} catch (Throwable e) {
    		logger.error(e.getMessage(), e);
		}

		// execute
		while(!toStop){
			running = false;
			idleTimes++;

            TriggerParam triggerParam = null;
            ReturnT<String> executeResult = null;
            try {
				// to check toStop signal, we need cycle, so wo cannot use queue.take(), instand of poll(timeout)
				triggerParam = triggerQueue.poll(3L, TimeUnit.SECONDS);
				if (triggerParam!=null) {//如果有任务等待执行
					running = true;
					idleTimes = 0;
					triggerLogIdSet.remove(triggerParam.getLogId());

					// log filename, like "logPath/yyyy-MM-dd/9999.log"
					String logFileName = XxlJobFileAppender.makeLogFileName(new Date(triggerParam.getLogDateTim()), triggerParam.getLogId());
					XxlJobFileAppender.contextHolder.set(logFileName);
					//设置分片信息
					ShardingUtil.setShardingVo(new ShardingUtil.ShardingVO(triggerParam.getBroadcastIndex(), triggerParam.getBroadcastTotal()));

					// execute
					XxlJobLogger.log("<br>----------- xxl-job job execute start -----------<br>----------- Param:" + triggerParam.getExecutorParams());
					//如果设置了超时时间
					if (triggerParam.getExecutorTimeout() > 0) {
						// limit timeout
						Thread futureThread = null;
						try {
							final TriggerParam triggerParamTmp = triggerParam;
							FutureTask<ReturnT<String>> futureTask = new FutureTask<ReturnT<String>>(new Callable<ReturnT<String>>() {
								@Override
								public ReturnT<String> call() throws Exception {
									//执行器开始进行调用任务
									return handler.execute(triggerParamTmp.getExecutorParams());
								}
							});
							futureThread = new Thread(futureTask);
							futureThread.start();
							//等待执行结果，超过一定的时间没返回的话，则请求超时
							executeResult = futureTask.get(triggerParam.getExecutorTimeout(), TimeUnit.SECONDS);
						} catch (TimeoutException e) {//请求超时
							//将调度任务执行的日志记录到 logFileName 中
							XxlJobLogger.log("<br>----------- xxl-job job execute timeout");
							XxlJobLogger.log(e);

							executeResult = new ReturnT<String>(IJobHandler.FAIL_TIMEOUT.getCode(), "job execute timeout ");
						} finally {
							futureThread.interrupt();
						}
					} else {//如果没有设置超时时间，则一直等待执行
						// just execute
						executeResult = handler.execute(triggerParam.getExecutorParams());
					}
					//处理任务执行结果，如果超时还没返回，则表示执行失败
					if (executeResult == null) {
						executeResult = IJobHandler.FAIL;
					} else {//如果返回结果
						executeResult.setMsg(
								(executeResult!=null&&executeResult.getMsg()!=null&&executeResult.getMsg().length()>50000)
										?executeResult.getMsg().substring(0, 50000).concat("...")
										:executeResult.getMsg());
						executeResult.setContent(null);	// limit obj size
					}
					XxlJobLogger.log("<br>----------- xxl-job job execute end(finish) -----------<br>----------- ReturnT:" + executeResult);

				} else {//如果队列是空的，表示没有任务
					if (idleTimes > 30) {//如果连续90s都没有有任务，则把这个任务移除掉
						XxlJobExecutor.removeJobThread(jobId, "excutor idel times over limit.");
					}
				}
			} catch (Throwable e) {//如果执行报错了
				if (toStop) {
					XxlJobLogger.log("<br>----------- JobThread toStop, stopReason:" + stopReason);
				}

				StringWriter stringWriter = new StringWriter();
				e.printStackTrace(new PrintWriter(stringWriter));
				String errorMsg = stringWriter.toString();
				executeResult = new ReturnT<String>(ReturnT.FAIL_CODE, errorMsg);

				XxlJobLogger.log("<br>----------- JobThread Exception:" + errorMsg + "<br>----------- xxl-job job execute end(error) -----------");
			} finally {
            	//
                if(triggerParam != null) {//表示从队列里有等待执行的任务

                    if (!toStop) {// 如果这个JobHandler没有停止，则将执行结果放到TriggerCallbackThread.callBackQueue队列中
                        TriggerCallbackThread.pushCallBack(new HandleCallbackParam(triggerParam.getLogId(), triggerParam.getLogDateTim(), executeResult));
                    } else {// 如果这个JobHandler停止，则将执行失败的结果放到TriggerCallbackThread.callBackQueue队列中
                        // is killed
                        ReturnT<String> stopResult = new ReturnT<String>(ReturnT.FAIL_CODE, stopReason + " [job running，killed]");
                        TriggerCallbackThread.pushCallBack(new HandleCallbackParam(triggerParam.getLogId(), triggerParam.getLogDateTim(), stopResult));
                    }
                }
            }
        }

		// 如果handler已经stop了，则对队列中剩余的待触发的任务都直接失败TriggerParam
		while(triggerQueue !=null && triggerQueue.size()>0){
			TriggerParam triggerParam = triggerQueue.poll();
			if (triggerParam!=null) {
				// is killed
				ReturnT<String> stopResult = new ReturnT<String>(ReturnT.FAIL_CODE, stopReason + " [job not executed, in the job queue, killed.]");
				TriggerCallbackThread.pushCallBack(new HandleCallbackParam(triggerParam.getLogId(), triggerParam.getLogDateTim(), stopResult));
			}
		}

		// destroy
		try {
			handler.destroy();
		} catch (Throwable e) {
			logger.error(e.getMessage(), e);
		}

		logger.info(">>>>>>>>>>> xxl-job JobThread stoped, hashCode:{}", Thread.currentThread());
	}
}
