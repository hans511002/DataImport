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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.zookeeper.KeeperException;

import com.ery.server.zk.ZKUtil;
import com.ery.server.zk.ZooKeeperListener;
import com.ery.server.zk.ZooKeeperWatcher;

//监控备Master
public class ServerManager extends ZooKeeperListener {
	private static final Log LOG = LogFactory.getLog(ServerManager.class);
	private final Map<String, HostInfo> masterServers = new ConcurrentHashMap<String, HostInfo>();
	private DaemonMaster master;
	private volatile boolean clusterShutdown = false;

	public ServerManager(ZooKeeperWatcher watcher, DaemonMaster abortable) {
		super(watcher);
		this.master = abortable;
	}

	/**
	 * Starts the tracking of online RegionServers.
	 * 
	 * <p>
	 * All RSs will be tracked after this method is called.
	 * 
	 * @throws KeeperException
	 * @throws IOException
	 */
	public void start() throws KeeperException, IOException {
		watcher.registerListener(this);
		nodeChildrenChanged(watcher.dimportHostNode);
	}

	@Override
	public void nodeDeleted(String path) {
		if (path.startsWith(watcher.dimportHostNode + "/")) {
			String serverName = ZKUtil.getNodeName(path);
			LOG.info("Server ephemeral node " + path + " deleted, processing expiration [" + serverName + "]");
			if (serverName.equals(master.hostName) && this.master.getActiveMasterManager().haveActiveMaster() &&
					this.master.getActiveMasterManager().clusterHasActiveMaster.get()) {
				String backupZNode = ZKUtil.joinZNode(this.watcher.dimportHostNode, master.serverInfo.hostName);
				LOG.info("reRegister ZNode for " + backupZNode + " in   master directory");
				try {
					ZKUtil.createEphemeralNodeAndWatch(this.watcher, backupZNode, master.serverInfo.getBytes());
				} catch (KeeperException e) {
					this.masterServers.get(serverName).deadTime = System.currentTimeMillis();
					LOG.error("nodeDeleted " + path + " reRegister fail Host:" + serverName, e);
				} catch (IOException e) {
					this.masterServers.get(serverName).deadTime = System.currentTimeMillis();
					LOG.error("nodeDeleted " + path + " reRegister fail Host:" + serverName, e);
				}
			} else {
				if (this.masterServers.containsKey(serverName))
					this.masterServers.get(serverName).deadTime = System.currentTimeMillis();
			}
		}
	}

	@Override
	public void nodeChildrenChanged(String path) {
		if (path.equals(watcher.dimportHostNode)) {
			try {
				List<String> servers = ZKUtil.listChildrenAndWatchThem(watcher, watcher.dimportHostNode);
				synchronized (this.masterServers) {
					for (String n : servers) {
						if (!this.masterServers.containsKey(n) || this.masterServers.get(n).deadTime > 0) {
							byte[] data = ZKUtil.getDataAndWatch(watcher, watcher.dimportHostNode + "/" + n);
							HostInfo master = HostInfo.parseFrom(data);
							if (master != null)
								this.masterServers.put(master.hostName, master);
						}
					}
				}
			} catch (IOException e) {
				master.abort("Unexpected zk exception getting RS nodes", e);
			} catch (KeeperException e) {
				master.abort("Unexpected zk exception getting RS nodes", e);
			}
		}
	}

	public void nodeDataChanged(String path) {
		if (path.startsWith(watcher.dimportHostNode + "/")) {
			String serverName = ZKUtil.getNodeName(path);
			String backupZNode = ZKUtil.joinZNode(this.watcher.dimportHostNode, master.serverInfo.hostName);
			LOG.info("host node " + path + " data changed");
			try {
				if (ZKUtil.checkExists(watcher, backupZNode) >= 0) {
					byte[] data = ZKUtil.getDataAndWatch(watcher, path);
					HostInfo master = HostInfo.parseFrom(data);
					this.masterServers.put(master.hostName, master);
				} else {
					if (this.masterServers.containsKey(serverName))
						this.masterServers.get(serverName).deadTime = System.currentTimeMillis();
				}
			} catch (KeeperException e) {
				master.abort("Unexpected zk exception getting RS nodes", e);
			} catch (IOException e) {
				master.abort("Unexpected zk exception getting RS nodes", e);
			}
		}
	}

	/**
	 * Called when a new node has been created.
	 * 
	 * @param path
	 *            full path of the new node
	 */
	public void nodeCreated(String path) {
		nodeDataChanged(path);
	}

	public Map<String, HostInfo> getDeadServers() {
		Map<String, HostInfo> res = new HashMap<String, HostInfo>();
		for (String host : this.masterServers.keySet()) {
			if (this.masterServers.get(host).deadTime > 0)
				res.put(host, this.masterServers.get(host));
		}
		return res;
	}

	public Map<String, HostInfo> getOnlineServers() {
		Map<String, HostInfo> res = new HashMap<String, HostInfo>();
		for (String host : this.masterServers.keySet()) {
			if (this.masterServers.get(host).deadTime == 0)
				res.put(host, this.masterServers.get(host));
		}
		return res;
	}

	public synchronized boolean isServerDead(String serverName) {
		return serverName == null || !this.masterServers.containsKey(serverName) ||
				this.masterServers.get(serverName).deadTime > 0;
	}
}
