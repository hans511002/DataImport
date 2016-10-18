package com.ery.dimport.task;

import java.io.IOException;
import java.util.Date;
import java.util.List;
import java.util.UUID;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.SessionExpiredException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;

import com.ery.base.support.utils.Utils;
import com.ery.dimport.task.TaskInfo.TaskType;
import com.googlecode.aviator.AviatorEvaluator;

public class TaskClient {
	private static final Log LOG = LogFactory.getLog(TaskClient.class.getName());

	final String baseNode;
	final String orderNode;
	final String quorumServers;
	private ZooKeeper zk;
	boolean needClose = true;
	boolean autoClose = true;

	public void setAutoClose(boolean autoClose) {
		this.autoClose = autoClose;
	}

	public TaskClient(String quorumServers, String baseNode) throws IOException {
		this.baseNode = baseNode;
		this.zk = new ZooKeeper(quorumServers, 30000, new Watcher() {
			@Override
			public void process(WatchedEvent event) {
			}
		});
		this.orderNode = baseNode + "/order";
		this.quorumServers = quorumServers;
	}

	public void close() {
		try {
			if (needClose)
				if (zk != null)
					zk.close();
		} catch (InterruptedException e) {
		}
	}

	public synchronized boolean submitTaskInfo(final TaskInfo task) throws IOException {
		LOG.info("锁命令：" + baseNode + "/metux");
		while (true) {
			try {
				zk.create(baseNode + "/metux", null, Ids.READ_ACL_UNSAFE, CreateMode.EPHEMERAL);
				LOG.info("完成锁命令：" + baseNode + "/metux");
				break;
			} catch (Exception e) {
				Utils.sleep(1000);
				if (e instanceof SessionExpiredException) {
					this.zk = new ZooKeeper(quorumServers, 30000, new Watcher() {
						@Override
						public void process(WatchedEvent event) {
						}
					});
				}
			}
		}
		try {
			if (task.taskType == TaskType.START) {
				if (task.TASK_ID == null || task.TASK_ID.equals("")) {
					// 查分cmd是否存在于路径中
					task.TASK_ID = UUID.randomUUID().toString();
					task.SUBMIT_TIME = new Date(System.currentTimeMillis());
					task.IS_ALL_SUCCESS = 0;
					task.HOST_SIZE = -1;// master.getServerManager().getOnlineServers().size();
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
							cmds[j] = macroProcess(cmds[j]);
						}
						task.TASK_COMMAND = join(" ", cmds);
					}
				}
				// 更新ZK命令，发布到各主机执行
				zk.setData(orderNode, TaskInfo.Serialize(task), -1);
				// master.logWriter.writeLog(task);
				return true;
			} else if (task.taskType == TaskType.STOP) {
				task.END_TIME = new Date(System.currentTimeMillis());
				if (task.TASK_ID == null || task.TASK_ID.equals(""))
					return false;
				zk.setData(orderNode, TaskInfo.Serialize(task), -1);
				// master.logWriter.updateLog(task);
				needClose = false;
				Thread th = new Thread() {
					@Override
					public void run() {
						try {// 等待结束，更新日志
							long l = System.currentTimeMillis();
							while (System.currentTimeMillis() - l < 120000) {
								// 判断ZK是否还存在
								if (zk.exists(orderNode, null) == null) {
									break;
								} else {
									Utils.sleep(1000);
								}
							}
							if (zk.exists(orderNode, null) == null) {
								deleteNodeRecursively(zk, baseNode + "/runTask/" + task.TASK_ID);
							}
						} catch (Exception e) {
							LOG.error("wait task :" + task, e);
						} finally {
							if (autoClose)
								TaskClient.this.close();
						}
					}
				};
				th.setDaemon(true);
				th.setName("STOP WAIT");
				th.start();
				return true;
			}
		} catch (Exception e) {
			throw new IOException(e);
		} finally {
			Utils.sleep(1000);
			while (true) {
				try {
					zk.delete(baseNode + "/metux", -1);
					LOG.info("释放锁命令：" + baseNode + "/metux");
					break;
				} catch (Exception e) {
				}
			}
		}
		return false;
	}

	public static String join(CharSequence separator, String[] strings) {
		StringBuilder sb = new StringBuilder();
		boolean first = true;
		for (String s : strings) {
			if (first) {
				first = false;
			} else {
				sb.append(separator);
			}
			sb.append(s);
		}
		return sb.toString();
	}

	public static final Pattern evalPattern = Pattern.compile("eval\\(.*\\)");

	public static String macroProcess(String str) {
		Matcher m = evalPattern.matcher(str);
		while (m.find()) {
			String exp = m.group(1);
			Object obj = AviatorEvaluator.execute(exp);
			str = m.replaceFirst(obj.toString());
			m = evalPattern.matcher(str);
		}
		return str;
	}

	public static void deleteNodeRecursively(ZooKeeper zk, String path) throws KeeperException, IOException,
			InterruptedException {
		List<String> children = zk.getChildren(path, null);
		// the node is already deleted, so we just finish
		if (children == null)
			return;

		if (!children.isEmpty()) {
			for (String child : children) {
				deleteNodeRecursively(zk, path + "/" + child);
			}
		}
		zk.delete(path, -1);
	}
}
