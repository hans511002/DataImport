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
import java.util.List;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.ery.dimport.DImportConstant;
import com.ery.server.conf.Configuration;
import com.ery.server.util.DNS;
import com.ery.server.util.ServerCommandLine;
import com.ery.server.util.StringUtils;
import com.ery.server.util.Strings;
import com.ery.server.util.VersionInfo;

public class DImportMasterStart extends ServerCommandLine {
	private static final Log LOG = LogFactory.getLog(DImportMasterStart.class);

	private static final String USAGE = "Usage: Master start|stop\n start  Start Master. \n"
			+ " stop stop a task .\n clear  Delete the master znode in ZooKeeper after a master crashes\n";

	private final Class<? extends DaemonMaster> masterClass;

	public DImportMasterStart(Class<? extends DaemonMaster> masterClass) {
		this.masterClass = masterClass;
	}

	protected String getUsage() {
		return USAGE;
	}

	public int run(String args[]) throws Exception {
		Options opt = new Options();// 参数解析 --param
		opt.addOption("debug", true, "debug type");// EStormConstant.DebugType
		CommandLine cmd;
		try {
			cmd = new GnuParser().parse(opt, args);
		} catch (ParseException e) {
			LOG.error("Could not parse: ", e);
			usage(null);
			return 1;
		}
		// if (test() > 0)
		// return 1;
		String debug = cmd.getOptionValue("debug");
		if (debug != null) {
			DImportConstant.DebugType = Integer.parseInt(debug);
		}
		List<String> remainingArgs = cmd.getArgList();
		if (remainingArgs.size() < 1) {
			usage(null);
			return 1;
		}
		String command = remainingArgs.get(0);
		if (command.equals("start")) {
			return startMaster();
		} else if (command.equals("stop")) {
			if (remainingArgs.size() == 1) {
				LOG.error("need taskId for stop order");
				usage("need taskId for stop order");
			}
			usage("当前未实现命令行停止task");
			String taskId = remainingArgs.get(1);
			return 0;
		}
		return 0;
	}

	public static HostInfo getMasterServerName(Configuration conf) throws IOException {
		String hostname = Strings.domainNamePointerToHostName(DNS.getDefaultHost(
				conf.get("dimport.master.dns.interface", "default"),
				conf.get("dimport.master.dns.nameserver", "default")));
		int _listenPort = conf.getInt(DImportConstant.DIMPORT_INFO_PORT_KEY, 3000);
		return new HostInfo(hostname, _listenPort, System.currentTimeMillis());
	}

	private int startMaster() {
		Configuration conf = getConf();
		try {
			logProcessInfo(getConf());
			DaemonMaster master = DaemonMaster.constructMaster(masterClass, conf);
			if (master.isStopped()) {
				LOG.info("Won't bring the Master up as a shutdown is requested");
				return 1;
			}
			master.start();
			master.join();
			if (master.isAborted())
				throw new RuntimeException("Dimport Master Aborted");
		} catch (Throwable t) {
			LOG.error("Dimport Master exiting", t);
			return 1;
		}
		return 0;
	}

	public static void main(String[] args) {
		VersionInfo.logVersion();
		StringUtils.startupShutdownMessage(DaemonMaster.class, args, LOG);
		new DImportMasterStart(DaemonMaster.class).doMain(args);
	}

}
