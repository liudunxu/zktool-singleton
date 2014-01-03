package com.job.zktool.cluster;

public class ShutdownHook implements Runnable {

	/**
	 * 关闭zookeeper连接
	 */
	@Override
	public void run() {
		HAManager.close();
	}
}
