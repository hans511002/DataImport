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
package com.ery.server.zk;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.data.Stat;

public class ZKAssign {
	private static final Log LOG = LogFactory.getLog(ZKAssign.class);

	public static String getNodeName(ZooKeeperWatcher zkw, String path) {
		return path.substring(path.lastIndexOf("/") + 1);
	}

	public static byte[] getData(ZooKeeperWatcher zkw, String pathOrRegionName) throws KeeperException, IOException {
		String node = getPath(zkw, pathOrRegionName);
		return ZKUtil.getDataAndWatch(zkw, node);
	}

	public static byte[] getDataAndWatch(ZooKeeperWatcher zkw, String pathOrRegionName, Stat stat)
			throws KeeperException, IOException {
		String node = getPath(zkw, pathOrRegionName);
		return ZKUtil.getDataAndWatch(zkw, node, stat);
	}

	public static byte[] getDataNoWatch(ZooKeeperWatcher zkw, String pathOrRegionName, Stat stat)
			throws KeeperException, IOException {
		String node = getPath(zkw, pathOrRegionName);
		return ZKUtil.getDataNoWatch(zkw, node, stat);
	}

	/**
	 * @param zkw
	 * @param pathOrRegionName
	 * @return Path to znode
	 */
	public static String getPath(final ZooKeeperWatcher zkw, final String pathOrRegionName) {
		return pathOrRegionName.startsWith("/") ? pathOrRegionName : getNodeName(zkw, pathOrRegionName);
	}

}
