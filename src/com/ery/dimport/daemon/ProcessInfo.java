package com.ery.dimport.daemon;

import java.sql.Connection;
import java.sql.DriverManager;
import java.util.ArrayList;
import java.util.List;

import com.ery.base.support.jdbc.DataAccess;
import com.ery.dimport.task.TaskInfo;
import com.ery.server.conf.Configuration;

public class ProcessInfo {
	final DaemonMaster master;
	final TaskManager taskManager;
	public TaskInfo task;
	public Thread thread;
	public Process proc;
	public String RUN_LOG_ID;
	public String processFile;
	public long startTime;
	public long endTime;
	public boolean stoped = false;

	public List<String> nodeMsgLogs = new ArrayList<String>();
	private static int LogFlushSize = 1000;// 10000条更新一次
	private static int LogFlushInterval = 600 * 1000;//
	static DataAccess access = new DataAccess();
	static Connection con = null;// 获取数据库连接

	static void open(Configuration conf) throws Exception {
		if (con == null) {
			con = DriverManager.getConnection("", "", "");
			access.setConnection(con);
			access.beginTransaction();
		}
	}

	public ProcessInfo(DaemonMaster master, TaskInfo task) {
		this.master = master;
		this.taskManager = master.getTaskManager();
		this.task = task;

	}

}
