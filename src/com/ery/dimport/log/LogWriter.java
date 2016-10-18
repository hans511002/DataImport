package com.ery.dimport.log;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.ery.base.support.jdbc.DataAccess;
import com.ery.base.support.utils.Utils;
import com.ery.dimport.DImportConstant;
import com.ery.dimport.daemon.DaemonMaster;
import com.ery.dimport.daemon.ProcessInfo;
import com.ery.dimport.task.LogPO.LogHostRunInfoPO;
import com.ery.dimport.task.LogPO.LogHostRunLogPO;
import com.ery.dimport.task.TaskInfo;
import com.ery.server.conf.Configuration;
import com.ery.server.util.HasThread;
import com.ery.server.util.StringUtils;

public class LogWriter {
	private static final Log LOG = LogFactory.getLog(LogWriter.class);

	final Configuration conf;
	final DaemonMaster master;
	static final List<LogHostRunLogPO> bufferdLogs = new ArrayList<LogHostRunLogPO>();
	public int logLevel = 0;// 只记录任务和运行日志 1:记录详细日志
	final WriteLogThread writeLog;
	public int flushLogLength = 1000;
	public int flushLogInterval = 60000;

	public LogWriter(DaemonMaster master) {
		this.master = master;
		this.conf = master.getConfiguration();
		writeLog = new WriteLogThread();
		this.logLevel = conf.getInt(DImportConstant.DIMPORT_LOG_WRITE_LEVEL, 0);
		this.flushLogLength = conf.getInt(DImportConstant.DIMPORT_LOG_WRITE_FLUASH_LENGTH, 1000);
		this.flushLogInterval = conf.getInt(DImportConstant.DIMPORT_LOG_WRITE_FLUASH_INTERVAL, 60000);
		if (logLevel == 1) {
			writeLog.start();
		}
	}

	public Connection getOutputConnection() throws ClassNotFoundException, SQLException {
		String driver = conf.get(DImportConstant.CONFIG_DB_DRIVER_KEY);
		String user = conf.get(DImportConstant.CONFIG_DB_USER_KEY);
		String hostUrl = conf.get(DImportConstant.CONFIG_DB_URL_KEY);
		String pwd = conf.get(DImportConstant.CONFIG_DB_PASS_KEY);
		Class.forName(driver);
		if (null == user) {
			return DriverManager.getConnection(hostUrl);
		} else {
			return DriverManager.getConnection(hostUrl, user, pwd);
		}
	}

	// 直接 写数据库
	public void writeLog(TaskInfo info) {
		DataAccess access = null;
		Connection con = null;
		try {
			if (LOG.isDebugEnabled()) {
				LOG.debug("添加任务日志： " + info);
			}
			List<String> hosts = info.hosts;
			if (info.hosts == null) {
				hosts = new ArrayList<String>();
				hosts.addAll(master.getServerManager().getOnlineServers().keySet());
			}
			if (info.TASK_COMMAND == null || info.TASK_COMMAND.equals("")) {
				info.TASK_COMMAND = StringUtils.join(" ", info.getCommand());
			}
			if (info.SUBMIT_TIME.getTime() < 1000)
				info.SUBMIT_TIME = new Date(System.currentTimeMillis());
			con = getOutputConnection();
			access = new DataAccess(con);
			access.setQueryTimeout(60000);
			long row = access.queryForLong("select count(0) c from dimport_task_info where TASK_ID=?", info.TASK_ID);
			if (row > 0) {
				updateLog(info, false);
				return;
			}
			access.execNoQuerySql("insert into dimport_task_info(TASK_ID,TASK_NAME,TASK_COMMAND,SUBMIT_TIME,START_TIME"
					+ ",END_TIME,IS_ALL_SUCCESS,FILE_NUMBER,FILE_PATH,FILE_FILTER"
					+ ",HOSTS,HOST_SIZE)values(?,?,?,?,?,?,?,?,?,?,?,?)", info.TASK_ID, info.TASK_NAME,
					info.TASK_COMMAND, info.SUBMIT_TIME, info.START_TIME, info.END_TIME, info.IS_ALL_SUCCESS,
					info.FILE_NUMBER, info.FILE_PATH, info.FILE_FILTER,
					(info.hosts == null ? "" : StringUtils.join(",", info.hosts)), hosts.size());
		} catch (Exception e) {
			LOG.error("writeLog TaskInfo:" + info, e);
		} finally {
			if (con != null) {
				try {
					con.close();
				} catch (SQLException e) {
				}
			}
		}
	}

	// 直接 写数据库
	public void updateLog(TaskInfo info) {
		updateLog(info, true);
	}

	public void updateLog(TaskInfo info, boolean check) {
		DataAccess access = null;
		Connection con = null;
		try {
			if (LOG.isDebugEnabled()) {
				LOG.debug("更新任务日志： " + info);
			}
			con = getOutputConnection();
			access = new DataAccess(con);
			access.setQueryTimeout(60000);
			if (check) {
				long row = access
						.queryForLong("select count(0) c from dimport_task_info where TASK_ID=?", info.TASK_ID);
				if (row == 0) {
					writeLog(info);
					return;
				}
			}
			if (info.hosts != null && info.hosts.size() > 0) {
				info.HOST_SIZE = info.hosts.size();
			}
			access.execNoQuerySql("update dimport_task_info set "
					+ "START_TIME=?,END_TIME=?,IS_ALL_SUCCESS=?,FILE_NUMBER=?"
					+ ",HOSTS=?,HOST_SIZE=?  where TASK_ID=?", info.START_TIME, info.END_TIME, info.IS_ALL_SUCCESS,
					info.FILE_NUMBER, (info.hosts == null ? "" : StringUtils.join(",", info.hosts)), info.HOST_SIZE,
					info.TASK_ID);
		} catch (Exception e) {
			LOG.error("updateLog TaskInfo:" + info, e);
		} finally {
			if (con != null) {
				try {
					con.close();
				} catch (SQLException e) {
				}
			}
		}
	}

	// 直接 写数据库
	public void writeLog(LogHostRunInfoPO info) {
		DataAccess access = null;
		Connection con = null;
		try {
			con = getOutputConnection();
			access = new DataAccess(con);
			if (LOG.isDebugEnabled()) {
				LOG.debug("添加执行日志： " + info);
			}
			access.setQueryTimeout(60000);
			access.execNoQuerySql("insert into dimport_task_host_run_info"
					+ "(RUN_LOG_ID,TASK_ID,HOST_NAME,FILE_NAME,FILE_SIZE,IS_RUN_SUCCESS"
					+ ",RETURN_CODE,ERROR_MSG,START_TIME,END_TIME,RUN_COMMAND )values (?,?,?,?,?,?,?,?,?,?,?) ",
					info.RUN_LOG_ID, info.TASK_ID, info.HOST_NAME, info.FILE_NAME, info.FILE_SIZE, info.IS_RUN_SUCCESS,
					info.RETURN_CODE, info.ERROR_MSG, info.START_TIME, info.END_TIME, info.RUN_COMMAND);
		} catch (Exception e) {
			LOG.error("writeLog runInfo:" + info, e);
		} finally {
			if (con != null) {
				try {
					con.close();
				} catch (SQLException e) {
				}
			}
		}
	}

	// 直接 写数据库
	public void updateLog(LogHostRunInfoPO info) {
		List<LogHostRunInfoPO> taskAllFile = new ArrayList();
		taskAllFile.add(info);
		updateLog(taskAllFile);
	}

	public long updateLog(List<LogHostRunInfoPO> taskAllFile) {
		Connection con = null;
		try {
			con = getOutputConnection();
			con = getOutputConnection();
			DataAccess access = new DataAccess(con);
			for (LogHostRunInfoPO info : taskAllFile) {
				if (LOG.isDebugEnabled()) {
					LOG.debug("更新执行日志： " + info);
				}
				access.execNoQuerySql("update dimport_task_host_run_info set  IS_RUN_SUCCESS=?,RETURN_CODE=?"
						+ " ,ERROR_MSG=?, START_TIME=?,END_TIME=?   where RUN_LOG_ID=? ", info.IS_RUN_SUCCESS,
						info.RETURN_CODE, info.ERROR_MSG, info.START_TIME, info.END_TIME, info.RUN_LOG_ID);
				// == null ? "" : info.ERROR_MSG
			}
			return taskAllFile.size();
		} catch (Exception e) {
			LOG.error("updateLog runInfo:" + taskAllFile, e);
			return 0;
		} finally {
			if (con != null) {
				try {
					con.close();
				} catch (SQLException e) {
				}
			}
		}

	}

	// 添加到缓存中，达到条件才写数据库
	public void addDetailLogMsg(LogHostRunLogPO detailPo) {
		if (LOG.isDebugEnabled()) {
			LOG.debug("添加任务详细日志： " + detailPo);
		}
		synchronized (bufferdLogs) {
			bufferdLogs.add(detailPo);
		}
	}

	class WriteLogThread extends HasThread {
		@Override
		public void run() {
			long time = System.currentTimeMillis();
			while (true) {
				try {
					List<LogHostRunLogPO> list = null;
					synchronized (bufferdLogs) {
						if (bufferdLogs.size() > flushLogLength || System.currentTimeMillis() - time > flushLogInterval) {
							list = new ArrayList<LogHostRunLogPO>(bufferdLogs);
							bufferdLogs.clear();
						}
					}
					if (list != null) {
						writeLog(list);
					}
				} catch (Exception e) {
					LOG.error(e);
				} finally {
					Utils.sleep(1000);
				}
			}
		}
	}

	void writeLog(List<LogHostRunLogPO> list) {
		if (list == null || list.size() == 0)
			return;
		Connection con = null;
		try {
			con = getOutputConnection();
			con.setAutoCommit(false);
			PreparedStatement statement = con.prepareStatement("insert into dimport_task_host_run_log"
					+ "(RUN_LOG_ID,TASK_ID,MSG,PRINT_TIME) values(?,?,?,?)");
			for (LogHostRunLogPO rpo : list) {
				statement.setObject(1, rpo.RUN_LOG_ID);
				statement.setObject(2, rpo.TASK_ID);
				statement.setObject(3, rpo.MSG);
				statement.setObject(4, rpo.START_TIME);
				statement.addBatch();
			}
			statement.executeBatch();
			con.commit();
		} catch (Exception e) {
			LOG.error("writeLog detailInfo:", e);
			if (con != null) {
				try {
					con.rollback();
				} catch (SQLException e1) {
				}
			}
			if (!bufferdLogs.equals(list))
				synchronized (bufferdLogs) {
					bufferdLogs.addAll(list);
				}
		} finally {
			if (con != null) {
				try {
					con.close();
				} catch (SQLException e) {
				}
			}
		}
	}

	public void writeBufferLogInAbort() {
		// 先写缓存的日志
		if (logLevel == 1) {
			synchronized (bufferdLogs) {
				writeLog(bufferdLogs);
				bufferdLogs.clear();
			}
		}
		// 最后写执行中的任务日志
		if (master.getTaskManager() != null && master.getTaskManager().taskInProgress.size() > 0) {
			for (String taskName : master.getTaskManager().taskInProgress.keySet()) {
				ProcessInfo pinfo = master.getTaskManager().taskInProgress.get(taskName);
				pinfo.task.END_TIME = new Date(System.currentTimeMillis());
				pinfo.task.IS_ALL_SUCCESS = 0;
				updateLog(pinfo.task);
			}
		}
	}
}
