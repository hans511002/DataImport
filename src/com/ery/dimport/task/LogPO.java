package com.ery.dimport.task;

import java.io.Serializable;
import java.util.Date;

import com.ery.dimport.DImportConstant;

public abstract class LogPO implements Serializable {
	private static final long serialVersionUID = -7907523931925827928L;
	public String RUN_LOG_ID;
	public String TASK_ID;
	public Date START_TIME = new Date(0);

	public static class LogHostRunInfoPO extends LogPO {
		private static final long serialVersionUID = -3386158007529913526L;
		public String HOST_NAME;
		public String FILE_NAME;
		public long FILE_SIZE;
		public int IS_RUN_SUCCESS = -1;
		public int RETURN_CODE = 255;
		public String ERROR_MSG;
		public Date END_TIME = new Date(0);
		public long downTime = -1;
		public String RUN_COMMAND;

		public LogHostRunInfoPO(TaskInfo task) {
			this.TASK_ID = task.TASK_ID;
		}

		public String toString() {
			StringBuffer sb = new StringBuffer();
			sb.append("RUN_LOG_ID=" + RUN_LOG_ID);
			sb.append(" TASK_ID=" + TASK_ID);
			sb.append(" HOST_NAME=" + HOST_NAME);
			sb.append(" downTime=" + downTime);
			sb.append(" IS_RUN_SUCCESS=" + IS_RUN_SUCCESS);
			sb.append(" RETURN_CODE=" + RETURN_CODE);
			sb.append(" START_TIME=" + DImportConstant.sdf.format(START_TIME));
			sb.append(" END_TIME=" + DImportConstant.sdf.format(END_TIME));
			sb.append(" FILE_NAME=" + FILE_NAME);
			sb.append(" FILE_SIZE=" + FILE_SIZE);
			sb.append(" RUN_COMMAND=" + RUN_COMMAND);
			return sb.toString();
		}

	}

	public static class LogHostRunLogPO extends LogPO {
		private static final long serialVersionUID = -6388382730692063934L;
		public String MSG;

		public String toString() {
			StringBuffer sb = new StringBuffer();
			sb.append(" RUN_LOG_ID=" + RUN_LOG_ID);
			sb.append(" TASK_ID=" + TASK_ID);
			sb.append(" START_TIME=" + DImportConstant.sdf.format(START_TIME));
			sb.append(" MSG=" + MSG);
			return sb.toString();
		}
	}

}
