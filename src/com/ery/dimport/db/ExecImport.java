package com.ery.dimport.db;

import com.ery.dimport.daemon.TaskManager;
import com.ery.dimport.task.TaskInfo;

public abstract class ExecImport {
	// 是否接收此任务,可根据任务名称，命令等判断
	public abstract boolean accept(final TaskInfo task);

	public abstract void runTask(final TaskManager taskManager, final TaskInfo task);

}
