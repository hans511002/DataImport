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
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.zookeeper.KeeperException;

import com.ery.server.monitor.MonitoredTask;
import com.ery.server.zk.ZKUtil;
import com.ery.server.zk.ZooKeeperListener;
import com.ery.server.zk.ZooKeeperWatcher;

public class ActiveMasterManager extends ZooKeeperListener {
	private static final Log LOG = LogFactory.getLog(ActiveMasterManager.class);

	final AtomicBoolean clusterHasActiveMaster = new AtomicBoolean(false);
	final AtomicBoolean clusterShutDown = new AtomicBoolean(false);

	private final HostInfo hostInfo;
	private HostInfo activeMaster;
	private final DaemonMaster master;

	boolean isActiveMaster;

	/**
	 * @param watcher
	 * @param sn
	 *            ServerName
	 * @param master
	 *            In an instance of a Master.
	 */
	ActiveMasterManager(ZooKeeperWatcher watcher, HostInfo sn, DaemonMaster master) {
		super(watcher);
		this.hostInfo = sn;
		this.master = master;
	}

	@Override
	public void nodeCreated(String path) {
		handle(path);
	}

	@Override
	public void nodeDeleted(String path) {
		handle(path);
	}

	void handle(final String path) {
		if (path.equals(watcher.getMasterNode()) && !master.isStopped()) {
			handleMasterNodeChange();
		}
	}

	public boolean isActiveMaster() {
		return isActiveMaster;
	}

	public HostInfo getActiveMaster() {
		return this.activeMaster;
	}

	private void handleMasterNodeChange() {
		// Watch the node and check if it exists.
		try {
			synchronized (clusterHasActiveMaster) {
				if (ZKUtil.watchAndCheckExists(watcher, watcher.getMasterNode())) {
					// A master node exists, there is an active master
					LOG.debug("A master is now available");
					byte[] data = ZKUtil.getDataAndWatch(watcher, watcher.getMasterNode());
					activeMaster = HostInfo.parseFrom(data);
					clusterHasActiveMaster.set(true);
					if (activeMaster == null) {
						LOG.debug("No master available. Notifying waiting threads");
						clusterHasActiveMaster.set(false);
						// Notify any thread waiting to become the active master
						clusterHasActiveMaster.notifyAll();
					} else {
						isActiveMaster = hostInfo.equals(this.activeMaster);
					}
					// master.getServerManager().recordNewServer(activeMaster);
				} else {
					// Node is no longer there, cluster does not have an active
					// master
					LOG.debug("No master available. Notifying waiting threads");
					clusterHasActiveMaster.set(false);
					// Notify any thread waiting to become the active master
					clusterHasActiveMaster.notifyAll();
				}
			}
		} catch (KeeperException ke) {
			master.abort("Received an unexpected KeeperException, aborting", ke);
		} catch (IOException e) {
			master.abort("Received an unexpected KeeperException, aborting", e);
		}
	}

	/**
	 * Block until becoming the active master.
	 * 
	 * Method blocks until there is not another active master and our attempt to
	 * become the new active master is successful.
	 * 
	 * This also makes sure that we are watching the master znode so will be
	 * notified if another master dies.
	 * 
	 * @param startupStatus
	 * @return True if no issue becoming active master else false if another
	 *         master was running or if some other problem (zookeeper, stop flag
	 *         has been set on this Master)
	 */
	boolean blockUntilBecomingActiveMaster(MonitoredTask startupStatus) {
		String backupZNode = ZKUtil.joinZNode(this.watcher.dimportHostNode, this.hostInfo.hostName);
		while (true) {
			try {
				if (ZKUtil.checkExists(watcher, backupZNode) >= 0) {
					ZKUtil.deleteNodeRecursively(watcher, backupZNode);
				}
				LOG.info("Adding ZNode for " + backupZNode + " in backup master directory");
				ZKUtil.createEphemeralNodeAndWatch(this.watcher, backupZNode, this.hostInfo.getBytes());
				break;
			} catch (KeeperException ke) {
				master.abort("Received an unexpected KeeperException, aborting", ke);
				return false;
			} catch (IOException e) {
				master.abort("Received an unexpected IOException, aborting", e);
				return false;
			}
		}
		while (true) {
			startupStatus.setStatus("Trying to register in ZK as active master");
			try {
				// LOG.info("Adding ZNode for " + backupZNode +
				// " in backup master directory");
				// ZKUtil.createEphemeralNodeAndWatch(this.watcher, backupZNode,
				// this.hostInfo.getBytes());
				if (ZKUtil.createEphemeralNodeAndWatch(this.watcher, this.watcher.getMasterNode(),
						this.hostInfo.getBytes())) {
					startupStatus.setStatus("Successfully registered as active master.");
					this.clusterHasActiveMaster.set(true);
					handleMasterNodeChange();
					LOG.info("Registered Active Master=" + this.hostInfo);
					this.isActiveMaster = true;
					return true;
				}
				this.clusterHasActiveMaster.set(true);
				handleMasterNodeChange();
				String msg;
				byte[] bytes = ZKUtil.getDataAndWatch(this.watcher, this.watcher.getMasterNode());
				if (bytes == null) {
					msg = ("A master was detected, but went down before its address "
							+ "could be read.  Attempting to become the next active master");
				} else {
					HostInfo currentMaster = HostInfo.parseFrom(bytes);
					if (this.hostInfo.equals(currentMaster)) {
						msg = ("Current master has this master's address, " + currentMaster + "; master was restarted? Deleting node.");
						// Hurry along the expiration of the znode.
						ZKUtil.deleteNode(this.watcher, this.watcher.getMasterNode());
					} else {
						msg = "Another master is the active master, " + currentMaster +
								"; waiting to become the next active master";
					}
				}
				LOG.info(msg);
				startupStatus.setStatus(msg);
			} catch (KeeperException ke) {
				master.abort("Received an unexpected KeeperException, aborting", ke);
				return false;
			} catch (IOException e) {
				master.abort("Received an unexpected IOException, aborting", e);
				return false;
			}
			synchronized (this.clusterHasActiveMaster) {
				while (this.clusterHasActiveMaster.get() && !this.master.isStopped()) {
					try {
						this.clusterHasActiveMaster.wait();
					} catch (InterruptedException e) {
						// We expect to be interrupted when a master dies,
						// will fall out if so
						LOG.debug("Interrupted waiting for master to die", e);
					}
				}
				if (clusterShutDown.get()) {
					this.master.stop("Cluster went down before this master became active");
				}
				if (this.master.isStopped()) {
					return false;
				}
				// there is no active master so we can try to become active
				// master again
			}
		}
	}

	/**
	 * @return True if cluster has an active master.
	 */
	public boolean haveActiveMaster() {
		try {
			if (ZKUtil.checkExists(watcher, watcher.getMasterNode()) >= 0) {
				return true;
			}
		} catch (KeeperException ke) {
			LOG.info("Received an unexpected KeeperException when checking " + "isActiveMaster : " + ke);
		} catch (IOException e) {
			LOG.info("Received an unexpected KeeperException when checking " + "isActiveMaster : " + e);
		}
		return false;
	}

	// stop this master to delete activeMaster znode
	public void stop() {
		try {
			if (activeMaster != null && activeMaster.equals(this.hostInfo)) {
				ZKUtil.deleteNode(watcher, watcher.getMasterNode());
			}
		} catch (KeeperException e) {
			LOG.error(this.watcher.prefix("Error deleting our own master address node"), e);
		} catch (IOException e) {
			LOG.error(this.watcher.prefix("Error deleting our own master address node"), e);
		}
	}
}
