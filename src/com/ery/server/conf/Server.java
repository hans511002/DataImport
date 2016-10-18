package com.ery.server.conf;

import com.ery.server.zk.ZooKeeperWatcher;

public interface Server extends Abortable, Stoppable {
	/**
	 * Gets the configuration object for this server.
	 */
	Configuration getConfiguration();

	/**
	 * Gets the ZooKeeper instance for this server.
	 */
	ZooKeeperWatcher getZooKeeper();

	/**
	 * @return The unique server name for this server.
	 */
	String getHotName();
}
