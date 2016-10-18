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

import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.ery.dimport.task.LogPO.LogHostRunInfoPO;
import com.ery.dimport.task.TaskInfo;
import com.ery.server.conf.Configuration;
import com.ery.server.http.BaseServlet;

/**
 * The servlet responsible for rendering the index page of the master.
 */
public class MasterStatusServlet extends BaseServlet {
	private static final Log LOG = LogFactory.getLog(MasterStatusServlet.class);
	private static final long serialVersionUID = 1L;

	@Override
	public void doGet(HttpServletRequest request, HttpServletResponse response) throws IOException {
		DaemonMaster master = (DaemonMaster) getServletContext().getAttribute(DaemonMaster.MASTER);
		assert master != null : "No Master in context!";
		doCheckMaster(request, response);
		Configuration conf = master.getConfiguration();
		Map<String, HostInfo> servers = master.getBackMasterTracker().getOnlineServers();// backMasters
		Map<String, HostInfo> deadServs = master.getServerManager().getDeadServers();
		PrintWriter out = response.getWriter();
		response.setContentType("text/html");

		TaskManager assign = master.getTaskManager();

		out.println("<html>");
		out.println("<head>");
		out.println("<meta HTTP-EQUIV='REFRESH' content='5;url=/master'/>");
		out.println("<script type='text/javascript'> ");
		out.println("function ensureInt(n) {\n var isInt = /^\\d+$/.test(n);"
				+ "if (!isInt) {\n alert(\"'\" + n + \"' is not integer.\");}\n  return isInt;\n}");
		out.println("</script>");

		out.println("</head>");
		out.println("<body>");
		out.println("<center>Dimport Master Info</center><br/>");
		out.println("<hr/>");
		out.println("Active Master<br/>");
		out.println(master.getActiveMaster());
		out.println("<br/>");
		out.println("<a href='/order'>submit task</a>");
		out.println("<hr/>");
		out.println("Task Masters<br/>");
		List<String> server = new ArrayList<String>(servers.keySet());
		String servs[] = server.toArray(new String[0]);
		Arrays.sort(servs);
		for (String serv : servs) {
			out.println(servers.get(serv));
			out.println("=================servStatus=");
			out.println("<br/>");
		}

		out.println("<hr/>");
		out.println("dead Masters<br/>");
		if (deadServs.size() > 0) {
			for (String serv : deadServs.keySet()) {
				out.println(deadServs.get(serv));
				out.println("<br/>");
			}
		}

		out.println("<hr/>");
		// 运行的任务
		out.print("<B>ALL Running task</B>");
		out.print("<pre> TASK_ID");
		out.print(" TASK_NAME");
		out.print(" IS_ALL_SUCCESS");
		out.print(" FILE_PATH");
		out.print(" FILE_FILTER");
		out.print(" TASK_COMMAND");
		out.println("</pre> ");
		for (String taskId : assign.allTask.keySet()) {
			TaskInfo taskInfo = assign.allTask.get(taskId);
			out.print("<pre> " + taskInfo.toString().replaceAll("\n", "\\n"));
			out.println("</pre> <input type=button value='STOP' onclick='window.open( \"/order?order=stop&taskId=" +
					taskInfo.TASK_ID + "\")' />");
		}
		// allTaskInfo 可分析每主机运行任务，及状态
		out.println("<hr/>");
		out.println("<B>TASK HSOT FILE RUNNING INFO</B><hr/>");
		for (String taskId : assign.allTaskInfo.keySet()) {
			Map<String, List<LogHostRunInfoPO>> map = assign.allTaskInfo.get(taskId);
			TaskInfo taskInfo = assign.allTask.get(taskId);
			if (taskInfo == null)
				continue;
			int runEndSize = 0;
			int runningSize = 0;
			HashMap<String, String> hostSum = new HashMap<String, String>();

			List<String> mapHost = new ArrayList<String>(map.keySet());
			String mhosts[] = mapHost.toArray(new String[0]);
			Arrays.sort(mhosts);
			for (String host : mhosts) {
				boolean isRunEnd = true;
				int failedFileSize = 0;
				int sucFileSize = 0;
				int waitingFileSize = 0;
				int runFileSize = 0;
				List<LogHostRunInfoPO> files = map.get(host);
				for (LogHostRunInfoPO logHostRunInfoPO : files) {
					if (logHostRunInfoPO.IS_RUN_SUCCESS != 1 ||
							logHostRunInfoPO.END_TIME.getTime() < logHostRunInfoPO.START_TIME.getTime()) {
						isRunEnd = false;
					}
					if (logHostRunInfoPO.START_TIME.getTime() > 100000) {
						if (logHostRunInfoPO.RETURN_CODE == 0) {// runed suc
							sucFileSize++;
						} else if (logHostRunInfoPO.END_TIME.getTime() > logHostRunInfoPO.START_TIME.getTime()) {
							failedFileSize++;
						} else {// running
							runFileSize++;
						}
					} else if (logHostRunInfoPO.IS_RUN_SUCCESS != 1) {// wait
						waitingFileSize++;
					}
				}
				hostSum.put(host, "suc[" + sucFileSize + "],fail[" + failedFileSize + "],run[" + runFileSize +
						"],wait[" + waitingFileSize + "]");
				if (isRunEnd) {
					runEndSize++;
				}
				if (files.size() > sucFileSize + failedFileSize + waitingFileSize)
					runningSize++;
			}
			String waitHost = "";
			String thosts[] = taskInfo.hosts.toArray(new String[0]);
			Arrays.sort(thosts);
			for (String host : thosts) {
				if (!map.containsKey(host)) {
					waitHost += host + ",";
				}
			}
			out.println("<b>" + taskId + "(all[" + taskInfo.hosts.size() + "],rec[" + map.size() + "],end[" +
					runEndSize + "],running[" + runningSize + "],wait[" + waitHost + "])</b><pre>");
			for (String host : mhosts) {
				out.println("<b>" + host + "(" + hostSum.get(host) + ")</b>");
				List<LogHostRunInfoPO> files = map.get(host);
				for (LogHostRunInfoPO logHostRunInfoPO : files) {
					out.println(logHostRunInfoPO);
				}
			}
			out.println("</pre><hr/>");
		}
		out.println("</body></html>");
		out.flush();
		out.close();
	}
}
