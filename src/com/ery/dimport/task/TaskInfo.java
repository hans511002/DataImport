package com.ery.dimport.task;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.regex.Pattern;

public class TaskInfo implements Serializable {
	public static java.text.SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
	private static final long serialVersionUID = 4076877637699137585L;
	public String TASK_NAME;
	public TaskType taskType;
	public String cmd;
	public List<String> params;
	public List<String> hosts;

	public String FILE_PATH;
	public String FILE_FILTER;
	public Pattern fileNamePattern;

	public Date SUBMIT_TIME = new Date(0);
	public Date START_TIME = new Date(0);
	public Date END_TIME = new Date(0);
	public int IS_ALL_SUCCESS;
	public int FILE_NUMBER = -1;
	public int HOST_SIZE;

	// 取得任务时生成UUID
	public String TASK_ID;
	// 运行前拼装生成
	public String TASK_COMMAND;

	public boolean equals(Object o) {
		if (o == null)
			return false;
		if (!(o instanceof TaskInfo)) {
			return false;
		}
		TaskInfo ot = (TaskInfo) o;
		if (!TASK_NAME.equals(ot.TASK_NAME))
			return false;
		if (!FILE_PATH.equals(ot.FILE_PATH))
			return false;
		if (!FILE_FILTER.equals(ot.FILE_FILTER))
			return false;
		if (!cmd.equals(ot.cmd))
			return false;
		if (taskType != ot.taskType)
			return false;
		if (params != null && ot.params != null) {
			if (params.size() != ot.params.size())
				return false;
			for (int i = 0; i < params.size(); i++) {
				if (!params.get(i).trim().equals(ot.params.get(i).trim())) {
					return false;
				}
			}
		} else if ((params != null && ot.params == null) || (params == null && ot.params != null)) {
			return false;
		}
		// 指定不同主机运行
		if (hosts == null && ot.hosts != null) {
			return false;
		}
		if (hosts != null && ot.hosts != null) {
			if (hosts.size() != ot.hosts.size())
				return false;
			for (int i = 0; i < hosts.size(); i++) {
				if (!hosts.get(i).trim().equals(ot.hosts.get(i).trim())) {
					return false;
				}
			}
		}
		return true;
	}

	public String toString() {
		StringBuffer sb = new StringBuffer();
		sb.append("[" + this.taskType + "]TASK_ID=" + TASK_ID);
		sb.append(" TASK_NAME=" + TASK_NAME);
		sb.append(" FILE_PATH=" + FILE_PATH);
		sb.append(" FILE_FILTER=" + FILE_FILTER);
		sb.append(" FILE_NUMBER=" + FILE_NUMBER);
		sb.append(" START_TIME=" + sdf.format(START_TIME));
		sb.append(" END_TIME=" + sdf.format(END_TIME));
		sb.append(" IS_ALL_SUCCESS=" + IS_ALL_SUCCESS);
		sb.append(" hosts=" + hosts);
		if (TASK_COMMAND != null) {
			sb.append(" TASK_COMMAND=" + TASK_COMMAND);
		} else {
			sb.append(" cmd=" + cmd);
			sb.append(" params=" + params);
		}
		return sb.toString();
	}

	public String[] getCommand() {
		if (params.size() == 0) {
			return new String[] { cmd };
		}
		List<String> p = new ArrayList<String>(params);
		p.add(0, cmd);
		return p.toArray(new String[0]);
	}

	public String getRunTaskId() {
		return TaskType.START + " " + TASK_NAME;
	}

	public String getTaskId() {
		return taskType + " " + TASK_NAME;
	}

	// public String getRunTaskId() {
	// return taskType + " " + TASK_ID;
	// }

	public void read(DataInputStream in) throws IOException {
		TASK_ID = in.readUTF();
		TASK_NAME = in.readUTF();
		if (in.readByte() == 0) {
			taskType = TaskType.STOP;
		} else {
			taskType = TaskType.START;
		}
		TASK_COMMAND = in.readUTF();
		cmd = in.readUTF();
		FILE_PATH = in.readUTF();
		FILE_FILTER = in.readUTF();
		String fnPat = in.readUTF();
		if (fnPat == null || fnPat.equals("")) {
			fileNamePattern = null;
		} else {
			fileNamePattern = Pattern.compile(fnPat);
		}
		SUBMIT_TIME = new Date(in.readLong());
		START_TIME = new Date(in.readLong());
		END_TIME = new Date(in.readLong());
		IS_ALL_SUCCESS = in.readInt();
		FILE_NUMBER = in.readInt();
		HOST_SIZE = in.readInt();
		int parLen = in.readInt();
		if (parLen > 0) {
			params = new ArrayList<String>();
			for (int i = 0; i < parLen; i++) {
				params.add(in.readUTF());
			}
		} else {
			params = null;
		}
		parLen = in.readInt();
		if (parLen > 0) {
			hosts = new ArrayList<String>();
			for (int i = 0; i < parLen; i++) {
				hosts.add(in.readUTF());
			}
		} else {
			hosts = null;
		}
	}

	public void write(DataOutputStream out) throws IOException {
		out.writeUTF(TASK_ID);
		out.writeUTF(TASK_NAME);
		out.writeByte(taskType == TaskType.START ? 1 : 0);
		if (TASK_COMMAND == null)
			TASK_COMMAND = "";
		out.writeUTF(TASK_COMMAND);

		out.writeUTF(cmd);
		out.writeUTF(FILE_PATH);
		if (FILE_FILTER == null)
			FILE_FILTER = "";
		out.writeUTF(FILE_FILTER);
		if (fileNamePattern == null)
			out.writeUTF("");
		else
			out.writeUTF(fileNamePattern.pattern());

		out.writeLong(SUBMIT_TIME.getTime());
		out.writeLong(START_TIME.getTime());
		out.writeLong(END_TIME.getTime());
		out.writeInt(IS_ALL_SUCCESS);
		out.writeInt(FILE_NUMBER);
		out.writeInt(HOST_SIZE);
		if (params == null) {
			out.writeInt(0);
		} else {
			out.writeInt(params.size());
			for (String par : params) {
				out.writeUTF(par);
			}
		}
		if (hosts == null) {
			out.writeInt(0);
		} else {
			out.writeInt(hosts.size());
			for (String par : hosts) {
				out.writeUTF(par);
			}
		}
	}

	public static enum TaskType {
		START, STOP;
	}

	public static <T> T castObject(byte[] data) throws IOException {
		ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(data);
		ObjectInputStream objectInputStream = new ObjectInputStream(byteArrayInputStream);
		try {
			T a = (T) objectInputStream.readObject();
			return a;
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
			throw new IOException(e);
		} finally {
			objectInputStream.close();
			byteArrayInputStream.close();
		}
	}

	public static byte[] Serialize(Object obj) throws IOException {
		ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
		ObjectOutputStream objectOutputStream = new ObjectOutputStream(byteArrayOutputStream);
		objectOutputStream.writeObject(obj);
		byte[] bts = byteArrayOutputStream.toByteArray();
		objectOutputStream.close();
		byteArrayOutputStream.close();
		return bts;
	}
}
