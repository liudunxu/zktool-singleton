package com.job.zktool.cluster;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

import org.I0Itec.zkclient.IZkChildListener;
import org.I0Itec.zkclient.ZkClient;

import com.dang.common.job.shell.util.ConsoleUtil;
import com.dang.metaq.job.common.Config;

public class HAManager {
	
	private static ConcurrentHashMap<String, ZkNodeAttr> pathMap;// 节点映射
	private static ZkClient zkClient;// zookeeper客户端
	private static String createdPath;//生成的zookeeper临时节点
	private static String jobName;//作业名
	/**
	 * 静态构造函数
	 */
	static {
		pathMap = new ConcurrentHashMap<String, ZkNodeAttr>();
		zkClient = new ZkClient(Config.ZK_CONN_URL);
	}

	/**
	 * job维度的分布式锁初始化
	 * 
	 * @param jobName
	 * @param scheduleName
	 * @throws InterruptedException
	 */
	public static void initScheduleLock(String _jobName) throws InterruptedException {
		jobName = _jobName;
		String schedulerParentPath = "/schedule/" + jobName;
		final String clusterName = "/cluster";

		// zk节点不存在，建立新永久节点
		if (!zkClient.exists(schedulerParentPath)) {
			zkClient.createPersistent(schedulerParentPath, true);
		}

		// 注册临时节点到zookeeper
		createdPath = zkClient.createEphemeralSequential(schedulerParentPath + clusterName, null);
		
		// 注册节点
		pathMap.put(schedulerParentPath, new ZkNodeAttr(createdPath));

		// 注册节点修改事件
		zkClient.subscribeChildChanges(schedulerParentPath, new IZkChildListener() {
			@Override
			public void handleChildChange(String parentPath, List<String> currentChilds) throws Exception {
				long minTime = Long.MAX_VALUE;
				String resultNode = parentPath;
				for (String node : currentChilds) {
					long creationTime = zkClient.getCreationTime(parentPath + "/" + node);
					if (creationTime < minTime) {
						minTime = creationTime;
						resultNode = parentPath + "/" + node;
					}
				}
				ZkNodeAttr attr = pathMap.get(parentPath);
				if (null != attr) {
					attr.setRunningPath(resultNode);
					if (attr.getPath().equals(resultNode)) {
						synchronized (AppFacade.LOCKOBJ) {
							AppFacade.LOCKOBJ.notifyAll();
						}
					}
				}
			}
		});

	}

	/**
	 * schedule维度的分布式锁验证
	 * 
	 * @param jobName
	 * @param scheduleName
	 * @throws InterruptedException
	 */
	public static void validScheduleLock(String jobName) throws InterruptedException {
		
		String schedulerParentPath = "/schedule/" + jobName;
		String resultNode = schedulerParentPath;
		long minTime = Long.MAX_VALUE;
		List<String> currentChilds = zkClient.getChildren(schedulerParentPath);
		for (String node : currentChilds) {
			String tmpPath = schedulerParentPath + "/" + node;
			long creationTime = zkClient.getCreationTime(tmpPath);
			if (creationTime < minTime) {
				minTime = creationTime;
				resultNode = tmpPath;
			}
		}
		if (createdPath.equals(resultNode)) {
			synchronized (AppFacade.LOCKOBJ) {
				AppFacade.LOCKOBJ.notifyAll();
			}
		}else{
			ConsoleUtil.println("已经有相同的作业实例，作业阻塞");
		}
	}
	
	/**
	 * 关闭zookeeper连接
	 */
	public static void close() {
		if (null != zkClient) {
			zkClient.close();
		}
	}
}

/**
 * zookeeper节点属性工具
 * 
 * @author liudunxu
 * 
 */
class ZkNodeAttr {

	// 节点路径
	private String path;
	// 正在运行的path
	private String runningPath;

	/**
	 * 构造函数
	 * 
	 * @param path
	 * @param schedulerName
	 */
	public ZkNodeAttr(String _path) {
		path = _path;
	}

	public String getPath() {
		return path;
	}

	public void setPath(String path) {
		this.path = path;
	}

	public String getRunningPath() {
		return runningPath;
	}

	public void setRunningPath(String runningPath) {
		this.runningPath = runningPath;
	}
}
