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
import java.util.Date;
import java.util.Map;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.ery.dimport.DImportConstant;
import com.ery.dimport.task.TaskClient;
import com.ery.dimport.task.TaskInfo;
import com.ery.dimport.task.TaskInfo.TaskType;
import com.ery.server.conf.Configuration;
import com.ery.server.http.BaseServlet;
import com.ery.server.util.StringUtils;

/**
 * The servlet responsible for rendering the index page of the master.
 */
public class TaskOrderServlet extends BaseServlet {
	private static final Log LOG = LogFactory.getLog(TaskOrderServlet.class);
	private static final long serialVersionUID = 1L;

	@Override
	public void doGet(HttpServletRequest request, HttpServletResponse response) throws IOException {
		DaemonMaster master = (DaemonMaster) getServletContext().getAttribute(DaemonMaster.MASTER);
		assert master != null : "No Master in context!";
		doCheckMaster(request, response);
		Configuration conf = master.getConfiguration();
		Map<String, HostInfo> servers = master.getBackMasterTracker().getOnlineServers();// backMasters
		Map<String, HostInfo> deadServs = master.getServerManager().getDeadServers();

		TaskManager assign = master.getTaskManager();
		response.setContentType("text/html");
		PrintWriter out = response.getWriter();
		String order = request.getParameter("order");
		String errMsg = null;
		TaskInfo task = new TaskInfo();
		if (order != null) {
			String taskId = request.getParameter("taskId");
			if (taskId != null && !taskId.equals("")) {
				task = assign.allTask.get(taskId);
				task.taskType = TaskType.STOP;
				task.END_TIME = new Date(System.currentTimeMillis());
			} else {// 新任务
				task.taskType = TaskType.START;
				task.TASK_NAME = request.getParameter("TASK_NAME");
				task.FILE_PATH = request.getParameter("FILE_PATH");
				task.FILE_FILTER = request.getParameter("FILE_FILTER");
				task.cmd = request.getParameter("cmd");
				if (task.TASK_NAME == null || task.TASK_NAME.equals("")) {
					errMsg = "TASK_NAME is null";
				} else if (task.FILE_PATH == null || task.FILE_PATH.equals("")) {
					errMsg = "FILE_PATH is null";
				} else if (task.cmd == null || task.cmd.equals("")) {
					errMsg = "FILE_PATH is null";
				}

				String params = request.getParameter("params");
				if (params != null && !params.trim().equals("")) {
					params = params.replaceAll("\r\n", "\n");
					String tmp[] = params.split("\n");
					task.params = new ArrayList<String>();
					for (int i = 0; i < tmp.length; i++) {
						task.params.add(tmp[i]);
					}
				}
			}
			TaskClient client = null;
			try {
				if (errMsg == null) {
					client = new TaskClient(conf.get(DImportConstant.ZOOKEEPER_QUORUM), master.getZooKeeper().baseZNode);
					client.submitTaskInfo(task);
					// assign.submitTaskInfo(task);
				}
			} catch (IOException e) {
				e.printStackTrace(out);
			} finally {
				if (client != null)
					client.close();
			}
		}

		out.println("<html>");
		out.println("<head>");
		out.println("<script>");
		if (errMsg != null) {
			out.println("alert('" + errMsg + "');");
		}
		if (task.taskType == TaskType.STOP && task.TASK_ID != null) {
			out.println("window.close();");
		}
		out.println("</script>");
		out.println("</head>");
		out.println("<body><form action='/order' id=order  type=post >");
		out.println("<table>");
		out.println("<input id=order name=order type='hidden' value='start' style='display:none;' /> ");
		out.println("<tr><td>TASK_NAME</td><td><input id=TASK_NAME name=TASK_NAME type='text' value='" +
				((task.TASK_NAME != null) ? task.TASK_NAME : "") + "'/> </td>");
		out.println("<tr><td>FILE_PATH</td><td><input id=FILE_PATH name=FILE_PATH type='text'  value='" +
				((task.FILE_PATH != null) ? task.FILE_PATH : "") + "'/> </td>");
		out.println("<tr><td>FILE_FILTER</td><td><input id=FILE_FILTER name=FILE_FILTER type='text'  value='" +
				((task.FILE_FILTER != null) ? task.FILE_FILTER : "") + "'/> </td>");
		out.println("<tr><td>cmd</td><td><input id=cmd name=cmd type='text'  value='" +
				((task.cmd != null) ? task.cmd : "") + "'/> </td>");
		out.println("<tr><td>params</td><td><textarea id=params name=params  rows=5  >  " +
				(task.params != null ? StringUtils.join("\n", task.params) : "") + "</textArea> </td>");
		out.println("<tr><td colspan=2> <input  type='button' onclick='document.getElementById(\"order\").submit()' value='submit' /> </td>");
		out.println("</table></form>");
		out.println("</body></html>");
		out.flush();
		out.close();
	}
}
