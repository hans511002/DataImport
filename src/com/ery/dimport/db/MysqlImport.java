package com.ery.dimport.db;

import java.util.ArrayList;
import java.util.List;

import com.ery.dimport.daemon.TaskManager;
import com.ery.dimport.task.TaskInfo;

public class MysqlImport extends ExecImport {
	static List<String> acceptCmds = new ArrayList<String>();
	static {
		acceptCmds.add("mysql");
		acceptCmds.add("mysqlimport");
		acceptCmds.add("cpimport");// infindb import
	}

	// 是否接收此任务,可根据任务名称，命令等判断
	public boolean accept(final TaskInfo task) {
		for (String key : acceptCmds) {
			if (task.cmd.equals(key) || task.cmd.endsWith("/" + key))
				return true;
		}
		return false;

	}

	// 启动任务，执行外部命令
	public void runTask(final TaskManager taskManager, final TaskInfo task) {
		taskManager.runTask(task);

	}
}
