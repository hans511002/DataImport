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
package com.ery.server.http;

import java.io.IOException;
import java.io.PrintWriter;

import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.ery.dimport.daemon.DaemonMaster;
import com.ery.server.conf.Configuration;

/**
 * The servlet responsible for rendering the index page of the master.
 */
public abstract class BaseServlet extends HttpServlet {
	private static final Log LOG = LogFactory.getLog(BaseServlet.class);
	private static final long serialVersionUID = 1L;

	// 获取编码类型
	public String getCharset() {
		return "UTF-8";
	}

	public void doGet(HttpServletRequest request, HttpServletResponse response) throws IOException {

	}

	public void doCheckMaster(HttpServletRequest request, HttpServletResponse response) throws IOException {
		DaemonMaster master = (DaemonMaster) getServletContext().getAttribute(DaemonMaster.MASTER);
		assert master != null : "No Master in context!";
		PrintWriter out = response.getWriter();
		response.setContentType("text/html");
		if (master.isInitialized()) {
			if (master.getServerManager() == null) {
				response.sendError(503, "Master not ready");
				return;
			}
		}
		// 跳转
		if (!master.isActiveMaster()) {
			if (master.getActiveMaster() == null) {
				out.println("启动中，稍后再试.");
				return;
			} else {
				response.sendRedirect("http://" + master.getActiveMaster().hostName + ":" +
						master.getActiveMaster().infoPort + "/master");
			}
		}
		Configuration conf = master.getConfiguration();

	}
}
