package com.job.zktool.cluster;

public class AppFacade implements Runnable {

	private static String[] args;
	private static IJobLogic jobLogic;
	public static final Object LOCKOBJ = new Object();

	/**
	 * 启动线程
	 * @param jobName
	 * @param _args
	 * @throws Exception
	 */
	public static void startJob(IJobLogic jobLogic,String jobName,String[] _args) throws Exception{
		//注册关闭钩子，服务停止运行，记录日志
		Thread t = new Thread(new ShutdownHook());
		Runtime.getRuntime().addShutdownHook(t);
		//启动服务线程
		startThread(jobName, _args);
	}
	
	@Override
	public void run() {
		synchronized (LOCKOBJ) {
			try {
				LOCKOBJ.wait();
			} catch (InterruptedException e) {
			}
		}
		try {
			jobLogic.startLogic(args);
		} catch (Exception e) {
			System.out.println(e);
		}
	}

	/**
	 * 开始运行
	 * @throws Exception 
	 */
	private static void startThread(String jobName,String[] _args) throws Exception {
		setArgs(_args);
		HAManager.initScheduleLock(jobName);
		Thread t = new Thread(new AppFacade());
		t.start();
		HAManager.validScheduleLock(jobName);
	}

	/**
	 * 停止运行
	 */
	public static void stop(){
		HAManager.close();
	}
	
	public static void setArgs(String[] _args) {
		args = _args;
	}
}
