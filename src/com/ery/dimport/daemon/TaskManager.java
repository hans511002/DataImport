/**
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.ery.dimport.daemon;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs.Ids;

import com.ery.base.support.utils.Utils;
import com.ery.dimport.DImportConstant;
import com.ery.dimport.db.ExecImport;
import com.ery.dimport.task.LogPO.LogHostRunInfoPO;
import com.ery.dimport.task.LogPO.LogHostRunLogPO;
import com.ery.dimport.task.TaskInfo;
import com.ery.dimport.task.TaskInfo.TaskType;
import com.ery.server.conf.Configuration;
import com.ery.server.conf.HadoopConf;
import com.ery.server.executor.ExecutorService;
import com.ery.server.util.HasThread;
import com.ery.server.util.KeyLocker;
import com.ery.server.util.ReflectionUtils;
import com.ery.server.util.StringUtils;
import com.ery.server.util.Threads;
import com.ery.server.zk.ZKUtil;
import com.ery.server.zk.ZooKeeperListener;
import com.ery.server.zk.ZooKeeperWatcher;
import com.google.common.collect.LinkedHashMultimap;

/**
 * topology 生成及分配提交，去除服务端TopologyTracker NodeTracker
 */
public class TaskManager extends ZooKeeperListener {
	public static final Log LOG = LogFactory.getLog(TaskManager.class);

	public final DaemonMaster master;
	public final ServerManager serverManager;
	final ExecutorService executorService;
	// A bunch of ZK events workers. Each is a single thread executor service
	private final java.util.concurrent.ExecutorService zkEventWorkers;

	final private KeyLocker<String> locker = new KeyLocker<String>();

	public Configuration conf;
	public TaskInfo lastTask;
	List<ExecImport> runInters = new ArrayList<ExecImport>();

	public TaskManager(DaemonMaster master) throws KeeperException, IOException {
		super(master.getZooKeeper());
		this.master = master;
		this.serverManager = master.getServerManager();

		this.executorService = master.getExecutorService();
		conf = master.getConfiguration();

		int workers = conf.getInt(DImportConstant.DIMPORT_PROCESS_THREAD_NUMBER,
				DImportConstant.DEFAULT_DIMPORT_PROCESS_THREAD_NUMBER);
		ThreadFactory threadFactory = Threads.newDaemonThreadFactory("TM.Worker");
		zkEventWorkers = Threads.getBoundedCachedThreadPool(workers, 60L, TimeUnit.SECONDS, threadFactory);
		Class<?>[] runs = conf.getClasses("dimport.process.interface");
		if (runs != null && runs.length > 0) {
			for (int i = 0; i < runs.length; i++) {
				try {
					runInters.add((ExecImport) ReflectionUtils.newInstance(runs[i]));
				} catch (Exception e) {
					LOG.warn("exception init " + runs[i].getName(), e);
				}
			}
		}
	}

	public final Map<String, ProcessInfo> taskInProgress = new HashMap<String, ProcessInfo>();
	// In a LinkedHashMultimap, the put order is kept when we retrieve the
	// collection back. We need
	// this as we want the events to be managed in the same order as we received
	// them.
	private final LinkedHashMultimap<String, TaskOrderRunnable> zkEventWorkerWaitingList = LinkedHashMultimap.create();

	/**
	 * A specific runnable that works only on a region.
	 */
	private abstract class TaskOrderRunnable implements Runnable {
		final TaskInfo task;

		public TaskOrderRunnable(TaskInfo task) {
			this.task = task;
		}

		String getTaskName() {
			return task.getTaskId();
		};
	}

	/**
	 * Submit a task, ensuring that there is only one task at a time that working on a given region. Order is respected.
	 */
	protected void zkEventWorkersSubmit(final TaskOrderRunnable regRunnable) {
		synchronized (taskInProgress) {
			if (taskInProgress.containsKey(regRunnable.getTaskName())) {
				synchronized (zkEventWorkerWaitingList) {
					zkEventWorkerWaitingList.put(regRunnable.getTaskName(), regRunnable);
				}
				return;
			}
			taskInProgress.put(regRunnable.getTaskName(), new ProcessInfo(this.master, regRunnable.task));
			zkEventWorkers.submit(new Runnable() {
				@Override
				public void run() {
					try {
						regRunnable.run();
					} finally {
						// now that we have finished, let's see if there is an
						// event for the same region in the
						// waiting list. If it's the case, we can now submit it
						// to the pool.
						synchronized (taskInProgress) {
							taskInProgress.remove(regRunnable.getTaskName());
							synchronized (zkEventWorkerWaitingList) {
								java.util.Set<TaskOrderRunnable> waiting = zkEventWorkerWaitingList.get(regRunnable
										.getTaskName());
								if (!waiting.isEmpty()) {
									// We want the first object only. The only
									// way to get it is through an iterator.
									TaskOrderRunnable toSubmit = waiting.iterator().next();
									zkEventWorkerWaitingList.remove(toSubmit.getTaskName(), toSubmit);
									zkEventWorkersSubmit(toSubmit);
								}
							}
						}
					}
				}
			});
		}
	}

	// ZooKeeper events
	/**
	 * New unassigned node has been created.
	 */
	@Override
	public void nodeCreated(String path) {
		if (path.equals(watcher.dimportOrderNode)) {// 命令
			handleAssignmentEvent(path);
		} else if (path.startsWith(watcher.dimportRunTaskNode + "/") && master.isActiveMaster()) {
			nodeDataChanged(path);
		}
	}

	/**
	 * Existing unassigned node has had data changed.
	 */
	@Override
	public void nodeDataChanged(final String path) {
		if (path.equals(watcher.dimportOrderNode)) {// 命令
			handleAssignmentEvent(path);
		} else if (path.startsWith(watcher.dimportRunTaskNode + "/")) {
			TaskLogDataChengedThread log = new TaskLogDataChengedThread(path);
			log.setDaemon(true).setName("TaskLogDataChanged").start();
		}
	}

	public class TaskLogDataChengedThread extends HasThread {
		final String path;

		public TaskLogDataChengedThread(String path) {
			this.path = path;
		}

		@Override
		public void run() {
			handleTaskLogDataEvent(path);
		}
	}

	public void nodeChildrenChanged(String path) {
		if (path.startsWith(watcher.dimportRunTaskNode)) {
			handleTaskLogDirEvent(path);
		}
	}

	public void nodeDeleted(String path) {
		if (path.startsWith(watcher.dimportRunTaskNode + "/") && master.isActiveMaster()) {// 任务数变更
			String taskHost = path.substring(watcher.dimportRunTaskNode.length() + 1);
			if (taskHost.indexOf("/") > 0) {// host
				String hostName = ZKUtil.getNodeName(path);
				String taskId = ZKUtil.getNodeName(ZKUtil.getParent(path));
				synchronized (allTaskInfo) {
					if (allTaskInfo.get(taskId) != null)
						allTaskInfo.get(taskId).remove(hostName);
				}
				if (!allTaskInfo.containsKey(taskId) ||
						(allTaskInfo.get(taskId) != null && allTaskInfo.get(taskId).size() == 0)) {
					synchronized (allTask) {
						allTask.remove(taskId);
					}
				}
			} else {
				String taskId = ZKUtil.getNodeName(path);
				synchronized (allTask) {
					allTask.remove(taskId);
				}
				synchronized (allTaskInfo) {
					allTaskInfo.remove(taskId);
				}
				synchronized (allHisTask) {
					allHisTask.remove(taskId);
				}
			}
		}
	}

	// 任务命令解析

	public class OrderChengedThread extends HasThread {
		final TaskInfo task;

		public OrderChengedThread(TaskInfo task) {
			this.task = task;
		}

		@Override
		public void run() {
			zkEventWorkersSubmit(new TaskOrderRunnable(task) {
				@Override
				public void run() {
					startTask(this.task);
				}
			});
			List<String> taskIds = null;
			synchronized (allHisTask) {
				if (allHisTask.size() > 100) {
					taskIds = new ArrayList<String>(allHisTask.keySet());
				}
			}
			if (taskIds != null) {
				for (String taskId : taskIds) {
					if (!allTask.containsKey(taskId)) {
						TaskInfo taskInfo = allHisTask.get(taskId);
						if (taskInfo.END_TIME.getTime() > taskInfo.START_TIME.getTime() &&
								System.currentTimeMillis() - taskInfo.END_TIME.getTime() > 3600000) {
							synchronized (allHisTask) {
								allHisTask.remove(taskId);
							}
						} else if (System.currentTimeMillis() - taskInfo.SUBMIT_TIME.getTime() > 7200000 &&
								taskInfo.START_TIME.getTime() < taskInfo.SUBMIT_TIME.getTime()) {// 二小时未进入执行
							synchronized (allHisTask) {
								allHisTask.remove(taskId);
							}
						}
					}
				}
			}
		}
	}

	private synchronized void handleAssignmentEvent(String path) {
		try {
			if (path.equals(watcher.dimportOrderNode)) {// 命令
				byte[] data = ZKUtil.getDataAndWatch(watcher, path);
				if (data != null) {
					TaskInfo task = DImportConstant.castObject(data);
					synchronized (allHisTask) {
						if (allHisTask.containsKey(task.TASK_ID)) {
							TaskInfo hisTask = allHisTask.get(task.TASK_ID);
							if (task.taskType == hisTask.taskType) {// 防止重复执行
								return;
							}
						} else {
							allHisTask.put(task.TASK_ID, task);
						}
					}
					OrderChengedThread orderThread = new OrderChengedThread(task);
					orderThread.setDaemon(true).setName("orderThread").start();
				}
			}
		} catch (KeeperException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public Map<String, TaskInfo> allTask = new HashMap<String, TaskInfo>();
	public Map<String, TaskInfo> allHisTask = new HashMap<String, TaskInfo>();
	public Map<String, Map<String, List<LogHostRunInfoPO>>> allTaskInfo = new HashMap<String, Map<String, List<LogHostRunInfoPO>>>();

	// 检测所有主机运行任务的日志状态
	synchronized void handleTaskLogDirEvent(String path) {
		try { // 子节点变更
			if (watcher.dimportRunTaskNode.equals(path)) {// 任务数变更
				List<String> taskIds = ZKUtil.listChildrenAndWatchThem(this.watcher, path);
				if (taskIds != null) {
					for (String taskId : taskIds) {
						if (!allTask.containsKey(taskId)) {
							byte[] data = getData(path + "/" + taskId);
							if (data != null && data.length > 0) {
								TaskInfo taskInfo = DImportConstant.castObject(data);
								allTask.put(taskId, taskInfo);
							} else {
								ZKUtil.deleteNodeRecursively(watcher, path + "/" + taskId);
								continue;
							}
							if (!allTaskInfo.containsKey(taskId)) {
								allTaskInfo.put(taskId, new HashMap<String, List<LogHostRunInfoPO>>());
							}
							handleTaskLogDirEvent(path + "/" + taskId);
						}
					}
				}
			} else if (master.isActiveMaster()) {// 任务状态变更,增加或减少主机
				String taskId = ZKUtil.getNodeName(path);
				if (!allTask.containsKey(taskId)) {
					byte[] data = getData(path);
					if (data != null && data.length > 0) {
						TaskInfo taskInfo = DImportConstant.castObject(data);
						allTask.put(taskId, taskInfo);
					}
				}
				List<String> hostNames = ZKUtil.listChildrenAndWatchThem(this.watcher, path);
				if (hostNames != null) {
					for (String hostName : hostNames) {
						Map<String, List<LogHostRunInfoPO>> map = allTaskInfo.get(taskId);
						byte[] data = getData(path + "/" + hostName);
						if (data != null) {
							List<LogHostRunInfoPO> allFiles = DImportConstant.castObject(data);
							map.put(hostName, allFiles);
						}
					}
				}
			}
		} catch (KeeperException e) {
			LOG.error("handleTaskLogDirEvent:" + path, e);
		} catch (IOException e) {
			LOG.error("handleTaskLogDirEvent:" + path, e);
		}
	}

	synchronized void handleTaskLogDataEvent(String path) {
		try {
			String taskHost = path.substring(watcher.dimportRunTaskNode.length() + 1);
			if (taskHost.indexOf("/") > 0 && master.isActiveMaster()) {// host
				String hostName = ZKUtil.getNodeName(path);
				String taskId = ZKUtil.getNodeName(ZKUtil.getParent(path));
				synchronized (allTaskInfo) {
					if (!allTaskInfo.containsKey(taskId)) {
						allTaskInfo.put(taskId, new HashMap<String, List<LogHostRunInfoPO>>());
					}
				}
				LOG.info("任务" + taskId + "更新主机[" + hostName + "]日志 " + path);
				Map<String, List<LogHostRunInfoPO>> map = allTaskInfo.get(taskId);
				byte[] data = getData(path);
				if (data != null) {
					List<LogHostRunInfoPO> allFiles = DImportConstant.castObject(data);
					map.put(hostName, allFiles);
					checkTaskAndWriteLog(false);
				} else {
					map.remove(hostName);
				}
			} else if (taskHost.indexOf("/") == -1) {// taskInfo
				String taskId = ZKUtil.getNodeName(path);
				byte[] data = getData(path);
				if (data != null) {
					TaskInfo taskInfo = DImportConstant.castObject(data);
					synchronized (allTask) {
						allTask.put(taskId, taskInfo);
						if (allHisTask.containsKey(taskId))// 只做更新
							allHisTask.put(taskId, taskInfo);
					}
				}
			}
		} catch (Exception e) {
			LOG.error("handleTaskLogDataEvent:" + path, e);
		}
	}

	// 所有主机，所有任务都成功完成，则删除ZK节点
	private void checkTaskAndWriteLog(boolean force) throws KeeperException, IOException {
		if (!master.isActiveMaster())
			return;
		List<String> taskIDS = new ArrayList<String>(allTask.keySet());
		for (String taskId : taskIDS) {
			TaskInfo taskInfo = allTask.get(taskId);
			Map<String, List<LogHostRunInfoPO>> map = allTaskInfo.get(taskId);
			if (map == null || map.size() == 0) {
				if (taskInfo.START_TIME.getTime() > 1000000) {
					if (System.currentTimeMillis() - taskInfo.START_TIME.getTime() > 600000) {
						LOG.warn("任务:" + taskId + " 超时无响应，删除任务");
						synchronized (allTask) {
							allTask.remove(taskId);
						}
						ZKUtil.deleteNodeRecursively(watcher, watcher.dimportRunTaskNode + "/" + taskId);
						taskInfo.END_TIME = new Date(System.currentTimeMillis());
						taskInfo.HOST_SIZE = 0;
						taskInfo.IS_ALL_SUCCESS = 0;
						taskInfo.FILE_NUMBER = 0;
						LOG.warn("任务:" + taskId + " 超时无响应，更新任务日志：" + taskInfo);
						master.logWriter.updateLog(taskInfo);// 更新task日志
					}
				}
				continue;
			}
			boolean isAllEnd = true;
			boolean isAllSuccess = true;
			// if (taskInfo.hosts == null)
			Map<String, Integer> hosts = new HashMap<String, Integer>();
			taskInfo.FILE_NUMBER = 0;
			for (String hostName : map.keySet()) {
				List<LogHostRunInfoPO> allFiles = map.get(hostName);
				taskInfo.FILE_NUMBER += allFiles.size();
				hosts.put(hostName, 0);
				if (allFiles.size() == 0) {
					isAllEnd = false;
					isAllSuccess = false;
					continue;
				}
				for (LogHostRunInfoPO fileInfo : allFiles) {
					if (taskInfo.START_TIME.getTime() < 10000 && fileInfo.START_TIME.getTime() > 0) {
						if (LOG.isDebugEnabled())
							LOG.debug("更新任务的开始时间:" + taskInfo);
						taskInfo.START_TIME = fileInfo.START_TIME;
					} else if (taskInfo.START_TIME.getTime() > fileInfo.START_TIME.getTime()) {
						if (LOG.isDebugEnabled())
							LOG.debug("更新任务的开始时间:" + taskInfo);
						taskInfo.START_TIME = fileInfo.START_TIME;
					}
					if (!master.getServerManager().getOnlineServers().containsKey(hostName)) {// 需要运行的主机不在线，任务失败
						if (fileInfo.START_TIME.getTime() == 0)
							fileInfo.START_TIME = new Date(System.currentTimeMillis() - 1000);
						fileInfo.END_TIME = new Date(System.currentTimeMillis());
						fileInfo.RETURN_CODE = -4;
						fileInfo.ERROR_MSG = "主机不在线";
						isAllEnd = true;
						master.logWriter.updateLog(fileInfo);// 更新文件日志
					}
					if ((fileInfo.START_TIME.getTime() > 0 && fileInfo.END_TIME.getTime() > 0 && fileInfo.END_TIME
							.getTime() > fileInfo.START_TIME.getTime())) {// 未完成
						if (fileInfo.RETURN_CODE != 0) {
							isAllSuccess = false;
						}
					} else {
						isAllSuccess = false;
						isAllEnd = false;
						if (force) {
							if (fileInfo.ERROR_MSG == null || fileInfo.equals(""))
								fileInfo.ERROR_MSG = "命令停止";
							master.logWriter.updateLog(fileInfo);// 写host日志
						} else {
							break;
						}
					}
				}
				if (force) {
					LOG.warn("命令强制停止:" + taskId + " on " + hostName);
					synchronized (allTaskInfo) {
						allTaskInfo.remove(hostName);
					}
					ZKUtil.deleteNodeRecursively(watcher, watcher.dimportRunTaskNode + "/" + taskId + "/" + hostName);
				} else if (!isAllEnd) {
					break;
				}
			}
			if (isAllEnd) {
				List<String> waitHosts = new ArrayList<String>();
				if (taskInfo.hosts == null) {
					if (hosts.size() != master.getServerManager().getOnlineServers().size()) {
						isAllEnd = false;
					} else {
						for (String host : master.getServerManager().getOnlineServers().keySet()) {
							if (!hosts.containsKey(host)) {
								waitHosts.add(host);
								isAllEnd = false;
							}
						}
					}
				} else {
					List<String> onLineHost = new ArrayList<String>(master.getServerManager().getOnlineServers()
							.keySet());
					List<String> taskHost = new ArrayList<String>(taskInfo.hosts);
					taskHost.removeAll(onLineHost);
					onLineHost.removeAll(hosts.keySet());
					if (onLineHost.size() > 0) {// 有主机在线，未执行的
						isAllEnd = false; // 重新发布命令
						waitHosts.addAll(onLineHost);
					} else if (taskHost.size() > 0) {// 存在不在线的主机，并且需要执行
						taskInfo.hosts.removeAll(taskHost);// 排除不在线主机
					}
				}
				if (waitHosts.size() > 0) {
					reWriteTaskOrder(taskInfo);
				}
			}
			if (isAllEnd && taskInfo.hosts == null) {
				taskInfo.hosts = new ArrayList<String>(hosts.keySet());
			}
			if (taskInfo.hosts != null && taskInfo.hosts.size() < taskInfo.HOST_SIZE) {
				taskInfo.HOST_SIZE = taskInfo.hosts.size();
			}
			if (force || isAllEnd) {
				taskInfo.IS_ALL_SUCCESS = isAllSuccess ? 1 : 0;
				taskInfo.END_TIME = new Date(System.currentTimeMillis());
				if (!force) {// 最后 更新日志
					LOG.info("完成任务:" + taskId + " 更新所有执行日志 ");
					List<LogHostRunInfoPO> taskAllFile = new ArrayList<LogHostRunInfoPO>();
					for (String hostName : map.keySet()) {
						for (LogHostRunInfoPO runInfo : map.get(hostName)) {
							if (runInfo.START_TIME.getTime() < 100000) {
								taskAllFile.add(runInfo);
							}
						}
						// taskAllFile.addAll(map.get(hostName));
					}
					master.logWriter.updateLog(taskAllFile);// 写host日志
				}
				master.logWriter.updateLog(taskInfo);// 更新task日志
				synchronized (allTask) {
					allTask.remove(taskId);
				}
				synchronized (allTaskInfo) {
					allTaskInfo.remove(taskId);
				}
				LOG.info("完成任务： " + taskId + " 删除任务信息:" + watcher.dimportRunTaskNode + "/" + taskId);
				ZKUtil.deleteNodeRecursively(watcher, watcher.dimportRunTaskNode + "/" + taskId);
			} else {
				String waitHost = "";
				for (String host : taskInfo.hosts) {
					if (!map.containsKey(host)) {
						waitHost += host + ",";
					}
				}
			}

		}
	}

	private void reWriteTaskOrder(TaskInfo taskInfo) {
		LOG.info("重写命令:" + taskInfo);
		while (true) {
			try {
				watcher.getRecoverableZooKeeper().create(watcher.baseZNode + "/metux", null, Ids.READ_ACL_UNSAFE,
						CreateMode.EPHEMERAL);
				// ZKUtil.createAndWatch(watcher, watcher.baseZNode
				// + "/metux", null);
				LOG.info("完成锁命令：" + watcher.baseZNode + "/metux");
				break;
			} catch (Exception e) {
				Utils.sleep(1000);
			}
		}
		try {
			ZKUtil.createSetData(watcher, watcher.dimportOrderNode, TaskInfo.Serialize(taskInfo));
		} catch (Exception e) {
			LOG.warn("重写任务失败：" + taskInfo, e);
		} finally {
			Utils.sleep(1000);
			while (true) {
				try {
					ZKUtil.deleteNodeRecursively(watcher, watcher.baseZNode + "/metux");
					break;
				} catch (Exception e) {
				}
			}
		}
	}

	public void cleanRunTaskInfos() throws KeeperException, IOException {
		for (String taskId : allTask.keySet()) {
			TaskInfo taskInfo = allTask.get(taskId);
			Map<String, List<LogHostRunInfoPO>> map = allTaskInfo.get(taskId);
			boolean isAllSuccess = true;
			if (taskInfo.hosts == null)
				taskInfo.hosts = new ArrayList<String>();
			for (String hostName : map.keySet()) {
				if (!hostName.equals(master.hostName)) {
					continue;
				}
				List<LogHostRunInfoPO> allFiles = map.get(hostName);
				for (LogHostRunInfoPO fileInfo : allFiles) {
					if (taskInfo.START_TIME.getTime() == 0 && fileInfo.START_TIME.getTime() > 0) {
						taskInfo.START_TIME = fileInfo.START_TIME;
					} else if (taskInfo.START_TIME.getTime() < fileInfo.START_TIME.getTime()) {
						taskInfo.START_TIME = fileInfo.START_TIME;
					}
					if ((fileInfo.START_TIME.getTime() > 0 && fileInfo.END_TIME.getTime() > 0 && fileInfo.END_TIME
							.getTime() > fileInfo.START_TIME.getTime())) {// 未完成
						if (fileInfo.RETURN_CODE != 0) {
							isAllSuccess = false;
						}
					} else {
						isAllSuccess = false;
						if (fileInfo.ERROR_MSG == null || fileInfo.equals(""))
							fileInfo.ERROR_MSG = "异常停止";
						fileInfo.END_TIME = new Date(System.currentTimeMillis());
						fileInfo.RETURN_CODE = 2;
						master.logWriter.updateLog(fileInfo);// 写host日志
					}
				}
				ZKUtil.createSetData(watcher, watcher.dimportRunTaskNode + "/" + taskId + "/" + hostName,
						DImportConstant.Serialize(allFiles));
			}
			if (taskInfo.hosts.size() < taskInfo.HOST_SIZE) {
				taskInfo.HOST_SIZE = taskInfo.hosts.size();
			}
			taskInfo.IS_ALL_SUCCESS = isAllSuccess ? 1 : 0;
			taskInfo.END_TIME = new Date(System.currentTimeMillis());
			master.logWriter.updateLog(taskInfo);// 更新task日志
		}
	}

	public void cleanAllTaskInfos() {
		try {
			checkTaskAndWriteLog(true);
			ZKUtil.deleteChildrenRecursively(watcher, watcher.dimportRunTaskNode);
		} catch (KeeperException e) {
			LOG.error("cleanTaskInfos", e);
		} catch (IOException e) {
			LOG.error("cleanTaskInfos", e);
		}

	}

	public static java.util.regex.Pattern hdfsUrlPattern = Pattern.compile("hdfs:///?(.+?)(/.*)");

	// 启动任务，执行外部命令
	public void runTask(final TaskInfo task) {
		List<LogHostRunInfoPO> allFiles = new ArrayList<LogHostRunInfoPO>();
		try {
			task.START_TIME = new Date(System.currentTimeMillis());
			boolean needUpdate = false;
			TaskInfo exists = allTask.get(task.TASK_ID);
			if (exists == null) {
				needUpdate = true;
			} else {
				task.hosts = exists.hosts;
			}
			if (task.hosts == null || task.hosts.size() == 0) {
				task.hosts = new ArrayList<String>(master.getServerManager().getOnlineServers().keySet());
				needUpdate = true;
			}
			if (ZKUtil.checkExists(watcher, watcher.dimportRunTaskNode + "/" + task.TASK_ID) == -1) {
				needUpdate = true;
			}
			if (needUpdate) {
				try {
					task.HOST_SIZE = task.hosts.size();
					master.logWriter.writeLog(task);
					ZKUtil.createSetData(watcher, watcher.dimportRunTaskNode + "/" + task.TASK_ID,
							DImportConstant.Serialize(task));
				} catch (Throwable e) {
				}
			}
			Thread thread = Thread.currentThread();
			ProcessInfo procInfo = null;
			synchronized (taskInProgress) {
				procInfo = taskInProgress.get(task.getRunTaskId());
			}
			procInfo.thread = thread;
			procInfo.startTime = System.currentTimeMillis();
			procInfo.startTime = System.currentTimeMillis();
			String filePath = task.FILE_PATH;
			boolean isInHdfs = false;
			final Map<String, Long> files = new HashMap<String, Long>();
			String tmpPath = conf.get(DImportConstant.DIMPORT_PROCESS_TMPDATA_DIR, System.getProperty("user.home"));
			if (tmpPath.endsWith("/")) {
				tmpPath = tmpPath.substring(0, tmpPath.length() - 1);
			}
			if (filePath == null || filePath.equals("")) {
				files.put("", 0l);
			} else {
				if (task.fileNamePattern != null || (task.FILE_FILTER != null && !task.FILE_FILTER.equals(""))) {
					task.FILE_FILTER = DImportConstant.macroProcess(task.FILE_FILTER);
					task.FILE_FILTER = task.FILE_FILTER.replaceAll("\\{host\\}", this.master.hostName);
					task.fileNamePattern = Pattern.compile(task.FILE_FILTER);
				}
				Matcher m = hdfsUrlPattern.matcher(filePath);
				if (m.matches()) {
					isInHdfs = true;
					filePath = m.group(2);
					// for (String string : conf.getValByRegex(".*").keySet()) {
					// System.out.println(string + "=" + conf.get(string));
					// }
					Path dirPath = new Path(filePath);
					FileSystem fs = FileSystem.get(HadoopConf.getConf(conf));
					if (!fs.exists(dirPath) || !fs.isDirectory(dirPath)) {
						throw new IOException("HDFS任务数据路径 " + filePath + "不存在,或不是目录");
					}
					FileStatus[] hFiles = fs.listStatus(dirPath, new PathFilter() {
						@Override
						public boolean accept(Path name) {
							if (task.fileNamePattern != null) {
								System.out.println("hdfs listStatus:" + name.getParent() + "/" + name.getName());
								return task.fileNamePattern.matcher(name.getName()).matches();
							} else {
								return true;
							}
						}
					});
					for (int i = 0; i < hFiles.length; i++) {
						files.put(hFiles[i].getPath().toString(), hFiles[i].getLen());
					}
				} else {
					java.io.File f = new File(filePath);
					if (!f.exists() || !f.isDirectory()) {
						throw new IOException("本地任务数据路径 " + filePath + "不存在 ,或不是目录");
					}
					File[] lFiles = f.listFiles(new FilenameFilter() {
						public boolean accept(File dir, String name) {
							if (task.fileNamePattern != null) {
								System.out.println("local fs listStatus:" + dir + "/" + name);
								return task.fileNamePattern.matcher(name).matches();
							} else {
								return true;
							}
						}
					});
					for (int i = 0; i < lFiles.length; i++) {
						files.put(lFiles[i].getAbsolutePath(), lFiles[i].length());
					}
				}
			}
			for (String fileName : files.keySet()) {
				LogHostRunInfoPO runInfo = new LogHostRunInfoPO(task);
				runInfo.RUN_LOG_ID = DImportConstant.shdf.format(task.SUBMIT_TIME) + "_" + allFiles.size() + "_" +
						fileName.hashCode();
				runInfo.FILE_NAME = fileName;
				runInfo.RETURN_CODE = 255;
				runInfo.IS_RUN_SUCCESS = -1;
				runInfo.FILE_SIZE = files.get(fileName);
				runInfo.HOST_NAME = master.hostName;
				String localFile = fileName;
				if (isInHdfs) {// 下载
					localFile = tmpPath + "/" + fileName.substring(fileName.lastIndexOf("/") + 1);
				}
				// 写日志
				String[] cmds = procInfo.task.getCommand();
				for (int j = 0; j < cmds.length; j++) {
					cmds[j] = DImportConstant.macroProcess(cmds[j]);
					cmds[j] = cmds[j].replaceAll("\\{file\\}", localFile);
					cmds[j] = cmds[j].replaceAll("\\{host\\}", master.hostName);
				}
				runInfo.RUN_COMMAND = StringUtils.join(" ", cmds);
				master.logWriter.writeLog(runInfo);
				LOG.info("完成执行日志初始写入：" + runInfo);
				allFiles.add(runInfo);
			}
			ZKUtil.createSetData(watcher, watcher.dimportRunTaskNode + "/" + task.TASK_ID + "/" + master.hostName,
					DImportConstant.Serialize(allFiles));
			for (LogHostRunInfoPO runInfo : allFiles) {
				if (procInfo.stoped)
					break;
				String fileName = runInfo.FILE_NAME;
				LOG.info("开始处理文件:" + fileName);
				procInfo.RUN_LOG_ID = runInfo.RUN_LOG_ID;
				runInfo.START_TIME = new Date(System.currentTimeMillis());
				procInfo.processFile = fileName;
				String localFile = fileName;
				try {
					if (isInHdfs) {// 下载
						localFile = tmpPath + "/" + fileName.substring(fileName.lastIndexOf("/") + 1);
					}
					procInfo.task.TASK_COMMAND = runInfo.RUN_COMMAND;
					if (isInHdfs) {// 下载
						File lf = new File(localFile);
						if (lf.exists())
							lf.delete();
						FileSystem fs = FileSystem.get(HadoopConf.getConf(conf));
						LOG.info("开始下载远程HDFS文件:" + fileName + "===>" + localFile);
						long btime = System.currentTimeMillis();
						fs.copyToLocalFile(new Path(fileName), new Path(localFile));
						LOG.info("下载远程HDFS文件完成:" + fileName + "===>" + localFile);
						runInfo.downTime = System.currentTimeMillis() - btime;
						fileName = localFile;
					}
					updateHostInfoLog(runInfo, allFiles);
					LOG.info(procInfo.task.TASK_NAME + " commandline: " + procInfo.task.TASK_COMMAND);
					procInfo.proc = execResult(runInfo.RUN_COMMAND);
					runInfo.IS_RUN_SUCCESS = 1;
					runInfo.RETURN_CODE = writeProcessLog(procInfo);
					LOG.info(procInfo.task.TASK_NAME + " return value: " + runInfo.RETURN_CODE);
					// runInfo.RETURN_CODE = procInfo.proc.exitValue();
				} catch (Throwable e) {
					runInfo.ERROR_MSG = e.getMessage();
					if (procInfo.proc != null) {
						try {
							procInfo.proc.destroy();
						} catch (Exception ex) {
						}
					}
					procInfo.proc = null;
					LOG.error("执行命令失败：", e);
				} finally { // 写日志
					runInfo.END_TIME = new Date(System.currentTimeMillis());
					master.logWriter.updateLog(runInfo);
					updateHostInfoLog(runInfo, allFiles);
					ZKUtil.createSetData(watcher, watcher.dimportRunTaskNode + "/" + task.TASK_ID + "/" +
							master.hostName, DImportConstant.Serialize(allFiles));
					if (isInHdfs) {
						File lf = new File(localFile);
						if (lf.exists())
							lf.delete();
					}
				}
			}
		} catch (Throwable e) {
			LOG.error("执行命令失败：" + task, e);
			try {
				if (allFiles.size() > 0) {
					for (LogHostRunInfoPO logHostRunInfoPO : allFiles) {
						if (logHostRunInfoPO.END_TIME.getTime() < 10000) {
							logHostRunInfoPO.END_TIME = new Date(System.currentTimeMillis());
							logHostRunInfoPO.IS_RUN_SUCCESS = 1;
							logHostRunInfoPO.RETURN_CODE = 2;
						}
					}
					ZKUtil.createSetData(watcher, watcher.dimportRunTaskNode + "/" + task.TASK_ID + "/" +
							master.hostName, DImportConstant.Serialize(allFiles));
				}
			} catch (KeeperException e1) {
				LOG.error("update task run info on host :" + watcher.dimportRunTaskNode + "/" + task.TASK_ID + "/" +
						master.hostName, e);
			} catch (IOException e1) {
				LOG.error("update task run info on host " + watcher.dimportRunTaskNode + "/" + task.TASK_ID + "/" +
						master.hostName, e);
			}
		} finally { // 写日志
			synchronized (taskInProgress) {
				taskInProgress.remove(task.getRunTaskId());
			}
		}
	}

	void updateHostInfoLog(LogHostRunInfoPO runInfo, List<LogHostRunInfoPO> allFiles) throws KeeperException,
			IOException {
		master.logWriter.updateLog(runInfo);
		ZKUtil.createSetData(watcher, watcher.dimportRunTaskNode + "/" + runInfo.TASK_ID + "/" + master.hostName,
				DImportConstant.Serialize(allFiles));
	}

	public void stopTask(TaskInfo task) {
		ProcessInfo procInfo = null;
		try {
			synchronized (taskInProgress) {
				procInfo = taskInProgress.get(task.getRunTaskId());
				if (procInfo != null)
					procInfo.endTime = System.currentTimeMillis();
			}
			if (procInfo != null) {
				procInfo.stoped = true;
				if (procInfo.proc != null) {
					try {
						procInfo.proc.destroy();
					} catch (Exception e) {
					}
				}
				if (procInfo.proc != null) {
					Utils.sleep(1000);
					if (procInfo.thread != null) {
						try {
							procInfo.thread.interrupt();
						} catch (Exception e) {
						}
					}
				}
			}
		} catch (Exception e) {
		} finally {// 写结束日志
			synchronized (taskInProgress) {
				taskInProgress.remove(task.getTaskId());
			}
			Utils.sleep(1000);
			try {
				ZKUtil.deleteNodeRecursively(watcher, watcher.dimportRunTaskNode + "/" + task.TASK_ID + "/" +
						master.hostName);
			} catch (KeeperException e1) {
				LOG.error("runTask:" + task, e1);
			} catch (IOException e1) {
				LOG.error("runTask:" + task, e1);
			}
		}
	}

	public void startTask(TaskInfo task) {
		if (task.taskType == TaskType.START) {
			if (task.hosts == null || task.hosts.contains(master.hostName)) {
				boolean isExeced = false;
				for (ExecImport exec : this.runInters) {
					if (exec.accept(task)) {
						LOG.info("使用特定方法[" + exec.getClass().getName() + "]执行任务命令:" + task);
						isExeced = true;
						exec.runTask(this, task);
						break;
					}
				}
				if (!isExeced) {
					LOG.info("使用默认执行方法执行任务命令:" + task);
					runTask(task);
				}
			}
		} else if (task.taskType == TaskType.STOP) {
			stopTask(task);
		}
	}

	public void startTaskRunLog() {
		try {
			if (master.isActiveMaster())
				checkTaskAndWriteLog(false);
		} catch (Exception e) {
			LOG.error("监听集群命令节点错误：", e);
		}
	}

	public void start() {
		try {
			ZKUtil.watchAndCheckExists(watcher, watcher.dimportOrderNode);// 启动集群命令监听
			ZKUtil.watchAndCheckExists(watcher, watcher.dimportRunTaskNode);// 启动集群命令监听
			nodeChildrenChanged(watcher.dimportRunTaskNode);
		} catch (Exception e) {
			LOG.error("监听集群命令节点错误：", e);
		}
	}

	public void stop() {
		watcher.unregisterListener(this);
	}

	public static Process execResult(String... command) throws IOException {
		if (null == command || command.length <= 0) {
			return null;
		}
		return Runtime.getRuntime().exec(StringUtils.join(" ", command));
		// ProcessBuilder pb = new ProcessBuilder(command);
		// return pb.start();
	}

	public int writeProcessLog(ProcessInfo procInfo) throws IOException {
		BufferedInputStream in = null;
		BufferedReader br = null;
		try {
			in = new BufferedInputStream(procInfo.proc.getInputStream());
			br = new BufferedReader(new InputStreamReader(in));
			String s;
			while ((s = br.readLine()) != null) {
				LOG.info("processFile:" + procInfo.processFile + " " + s);
				if (master.logWriter.logLevel == 1) {// 写详细日志
					LogHostRunLogPO detailPo = new LogHostRunLogPO();
					detailPo.START_TIME = new Date(System.currentTimeMillis());
					detailPo.TASK_ID = procInfo.task.TASK_ID;
					detailPo.MSG = s;
					detailPo.RUN_LOG_ID = procInfo.RUN_LOG_ID;
					master.logWriter.addDetailLogMsg(detailPo);
				}
			}
			br.close();
			in.close();
			in = new BufferedInputStream(procInfo.proc.getErrorStream());
			br = new BufferedReader(new InputStreamReader(in));
			while ((s = br.readLine()) != null) {
				LOG.info("processFile:" + procInfo.processFile + " " + s);
				if (master.logWriter.logLevel == 1) {// 写详细日志
					LogHostRunLogPO detailPo = new LogHostRunLogPO();
					detailPo.START_TIME = new Date(System.currentTimeMillis());
					detailPo.TASK_ID = procInfo.task.TASK_ID;
					detailPo.MSG = s;
					detailPo.RUN_LOG_ID = procInfo.RUN_LOG_ID;
					master.logWriter.addDetailLogMsg(detailPo);
				}
			}
			return procInfo.proc.waitFor();
		} catch (IOException | InterruptedException e) {
			throw new IOException(e);
		} finally {
			br.close();
			in.close();
		}
	}

	// 用于MASTER提交任务
	public synchronized void submitTaskInfo(final TaskInfo task) throws IOException, KeeperException {
		if (task.taskType == TaskType.START) {
			if (task.TASK_ID == null || task.TASK_ID.equals("")) {
				// 查分cmd是否存在于路径中
				task.TASK_ID = UUID.randomUUID().toString();
				task.SUBMIT_TIME = new Date(System.currentTimeMillis());
				task.IS_ALL_SUCCESS = 0;
				task.HOST_SIZE = master.getServerManager().getOnlineServers().size();
				if (task.FILE_FILTER != null && !task.FILE_FILTER.equals("")) {
					try {
						// 只是用于检查语法
						task.fileNamePattern = Pattern.compile(task.FILE_FILTER.replaceAll("\\{|\\}", ""));
					} catch (Exception e) {
						throw new IOException(e);
					}
				}
				// 写日志使用
				if (task.TASK_COMMAND == null || task.TASK_COMMAND.equals("")) {
					String[] cmds = task.getCommand();
					for (int j = 0; j < cmds.length; j++) {
						cmds[j] = DImportConstant.macroProcess(cmds[j]);
					}
					task.TASK_COMMAND = StringUtils.join(" ", cmds);
				}
			}
			// 更新ZK命令，发布到各主机执行
			ZKUtil.createSetData(watcher, watcher.dimportOrderNode, DImportConstant.Serialize(task));
			master.logWriter.writeLog(task);
			ZKUtil.createSetData(watcher, watcher.dimportRunTaskNode + "/" + task.TASK_ID,
					DImportConstant.Serialize(task));
		} else if (task.taskType == TaskType.STOP) {
			task.END_TIME = new Date(System.currentTimeMillis());
			master.logWriter.updateLog(task);
			ZKUtil.createSetData(watcher, watcher.dimportOrderNode, DImportConstant.Serialize(task));

			new HasThread() {
				@Override
				public void run() {
					try {// 等待结束，更新日志
						long l = System.currentTimeMillis();
						while (System.currentTimeMillis() - l < 120000) {
							// 判断ZK是否还存在
							if (ZKUtil.checkExists(watcher, watcher.dimportRunTaskNode + "/" + task.TASK_ID) == -1) {
								break;
							} else {
								Utils.sleep(1000);
							}
						}
						if (ZKUtil.checkExists(watcher, watcher.dimportRunTaskNode + "/" + task.TASK_ID) >= 0) {
							ZKUtil.deleteNodeRecursively(watcher, watcher.dimportRunTaskNode + "/" + task.TASK_ID);
						}
					} catch (Exception e) {
						LOG.error("wait task :" + task, e);
					}
				}
			}.setDaemon(true).setName("STOP WAIT").start();
		}
	}

	public ZooKeeperWatcher getWatcher() {
		return this.watcher;
	}
}
