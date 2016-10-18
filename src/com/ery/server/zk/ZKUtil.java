package com.ery.server.zk;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.zookeeper.AsyncCallback;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.NoNodeException;
import org.apache.zookeeper.Op;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Stat;
import org.apache.zookeeper.proto.CreateRequest;
import org.apache.zookeeper.proto.DeleteRequest;
import org.apache.zookeeper.proto.SetDataRequest;

import com.sun.xml.internal.ws.encoding.soap.DeserializationException;
import com.ery.dimport.DImportConstant;
import com.ery.server.conf.Configuration;
import com.ery.server.util.Bytes;
import com.ery.server.util.Threads;
import com.ery.server.zk.ZKUtil.ZKUtilOp.CreateAndFailSilent;
import com.ery.server.zk.ZKUtil.ZKUtilOp.DeleteNodeFailSilent;
import com.ery.server.zk.ZKUtil.ZKUtilOp.SetData;

public class ZKUtil {
	private static final Log LOG = LogFactory.getLog(ZKUtil.class);

	// TODO: Replace this with ZooKeeper constant when ZOOKEEPER-277 is
	// resolved.
	public static final char ZNODE_PATH_SEPARATOR = '/';
	private static int zkDumpConnectionTimeOut;

	/**
	 * Creates a new connection to ZooKeeper, pulling settings and ensemble
	 * config from the specified configuration object using methods from
	 * {@link ZKConfig}.
	 * 
	 * Sets the connection status monitoring watcher to the specified watcher.
	 * 
	 * @param conf
	 *            configuration to pull ensemble and other settings from
	 * @param watcher
	 *            watcher to monitor connection changes
	 * @return connection to zookeeper
	 * @throws IOException
	 *             if unable to connect to zk or config problem
	 */
	public static RecoverableZooKeeper connect(Configuration conf, Watcher watcher) throws IOException {
		String ensemble = conf.get(DImportConstant.ZOOKEEPER_QUORUM);
		return connect(conf, ensemble, watcher);
	}

	public static RecoverableZooKeeper connect(Configuration conf, String ensemble, Watcher watcher) throws IOException {
		return connect(conf, ensemble, watcher, null);
	}

	public static RecoverableZooKeeper connect(Configuration conf, String ensemble, Watcher watcher,
			final String identifier) throws IOException {
		if (ensemble == null) {
			throw new IOException("Unable to determine ZooKeeper ensemble");
		}
		int timeout = conf.getInt(DImportConstant.ZK_SESSION_TIMEOUT, DImportConstant.DEFAULT_ZK_SESSION_TIMEOUT);
		if (LOG.isTraceEnabled()) {
			LOG.trace(identifier + " opening connection to ZooKeeper ensemble=" + ensemble);
		}
		int retry = conf.getInt("zookeeper.recovery.retry", 5);
		int retryIntervalMillis = conf.getInt("zookeeper.recovery.retry.intervalmill", 1000);
		zkDumpConnectionTimeOut = conf.getInt("zookeeper.dump.connection.timeout", 1000);
		return new RecoverableZooKeeper(ensemble, timeout, watcher, retry, retryIntervalMillis, identifier);
	}

	/**
	 * Join the prefix znode name with the suffix znode name to generate a
	 * proper full znode name.
	 * 
	 * Assumes prefix does not end with slash and suffix does not begin with it.
	 * 
	 * @param prefix
	 *            beginning of znode name
	 * @param suffix
	 *            ending of znode name
	 * @return result of properly joining prefix with suffix
	 */
	public static String joinZNode(String prefix, String suffix) {
		return prefix + ZNODE_PATH_SEPARATOR + suffix;
	}

	/**
	 * Returns the full path of the immediate parent of the specified node.
	 * 
	 * @param node
	 *            path to get parent of
	 * @return parent of path, null if passed the root node or an invalid node
	 */
	public static String getParent(String node) {
		int idx = node.lastIndexOf(ZNODE_PATH_SEPARATOR);
		return idx <= 0 ? null : node.substring(0, idx);
	}

	/**
	 * Get the name of the current node from the specified fully-qualified path.
	 * 
	 * @param path
	 *            fully-qualified path
	 * @return name of the current node
	 */
	public static String getNodeName(String path) {
		return path.substring(path.lastIndexOf(ZNODE_PATH_SEPARATOR) + 1);
	}

	/**
	 * Get the key to the ZK ensemble for this configuration without adding a
	 * name at the end
	 * 
	 * @param conf
	 *            Configuration to use to build the key
	 * @return ensemble key without a name
	 */
	public static String getZooKeeperClusterKey(Configuration conf) {
		return getZooKeeperClusterKey(conf, null);
	}

	/**
	 * Get the key to the ZK ensemble for this configuration and append a name
	 * at the end
	 * 
	 * @param conf
	 *            Configuration to use to build the key
	 * @param name
	 *            Name that should be appended at the end if not empty or null
	 * @return ensemble key with a name (if any)
	 */
	public static String getZooKeeperClusterKey(Configuration conf, String name) {
		String ensemble = conf.get(DImportConstant.ZOOKEEPER_QUORUM.replaceAll("[\\t\\n\\x0B\\f\\r]", ""));
		StringBuilder builder = new StringBuilder(ensemble);
		builder.append(";");
		builder.append(conf.get(DImportConstant.ZOOKEEPER_BASE_ZNODE));
		if (name != null && !name.isEmpty()) {
			builder.append(",");
			builder.append(name);
		}
		return builder.toString();
	}

	/**
	 * Apply the settings in the given key to the given configuration, this is
	 * used to communicate with distant clusters
	 * 
	 * @param conf
	 *            configuration object to configure
	 * @param key
	 *            string that contains the 3 required configuratins
	 * @throws IOException
	 */
	public static void applyClusterKeyToConf(Configuration conf, String key) throws IOException {
		String[] parts = transformClusterKey(key);
		conf.set(DImportConstant.ZOOKEEPER_QUORUM, parts[0]);
		conf.set(DImportConstant.ZOOKEEPER_BASE_ZNODE, parts[1]);
	}

	/**
	 * Separate the given key into the three configurations it should contain:
	 * hbase.zookeeper.quorum, hbase.zookeeper.client.port and
	 * zookeeper.znode.parent
	 * 
	 * @param key
	 * @return the three configuration in the described order
	 * @throws IOException
	 */
	public static String[] transformClusterKey(String key) throws IOException {
		String[] parts = key.split(";");
		if (parts.length != 2) {
			throw new IOException("Cluster key invalid, the format should be:" + DImportConstant.ZOOKEEPER_QUORUM +
					";" + DImportConstant.ZOOKEEPER_BASE_ZNODE);
		}
		return parts;
	}

	//
	// Existence checks and watches
	//

	/**
	 * Watch the specified znode for delete/create/change events. The watcher is
	 * set whether or not the node exists. If the node already exists, the
	 * method returns true. If the node does not exist, the method returns
	 * false.
	 * 
	 * @param zkw
	 *            zk reference
	 * @param znode
	 *            path of node to watch
	 * @return true if znode exists, false if does not exist or error
	 * @throws KeeperException
	 *             if unexpected zookeeper exception
	 * @throws IOException
	 */
	public static boolean watchAndCheckExists(ZooKeeperWatcher zkw, String znode) throws KeeperException, IOException {
		try {
			Stat s = zkw.getRecoverableZooKeeper().exists(znode, zkw);
			boolean exists = s != null ? true : false;
			if (exists) {
				LOG.debug(zkw.prefix("Set watcher on existing znode=" + znode));
			} else {
				LOG.debug(zkw.prefix("Set watcher on znode that does not yet exist, " + znode));
			}
			return exists;
		} catch (KeeperException e) {
			LOG.warn(zkw.prefix("Unable to set watcher on znode " + znode), e);
			zkw.keeperException(e);
			return false;
		} catch (InterruptedException e) {
			LOG.warn(zkw.prefix("Unable to set watcher on znode " + znode), e);
			zkw.interruptedException(e);
			return false;
		}
	}

	/**
	 * Watch the specified znode, but only if exists. Useful when watching for
	 * deletions. Uses .getData() (and handles NoNodeException) instead of
	 * .exists() to accomplish this, as .getData() will only set a watch if the
	 * znode exists.
	 * 
	 * @param zkw
	 *            zk reference
	 * @param znode
	 *            path of node to watch
	 * @return true if the watch is set, false if node does not exists
	 * @throws KeeperException
	 *             if unexpected zookeeper exception
	 */
	public static boolean setWatchIfNodeExists(ZooKeeperWatcher zkw, String znode) throws KeeperException, IOException {
		try {
			zkw.getRecoverableZooKeeper().getData(znode, true, null);
			return true;
		} catch (NoNodeException e) {
			return false;
		} catch (InterruptedException e) {
			LOG.warn(zkw.prefix("Unable to set watcher on znode " + znode), e);
			zkw.interruptedException(e);
			return false;
		}
	}

	/**
	 * Check if the specified node exists. Sets no watches.
	 * 
	 * @param zkw
	 *            zk reference
	 * @param znode
	 *            path of node to watch
	 * @return version of the node if it exists, -1 if does not exist
	 * @throws KeeperException
	 *             if unexpected zookeeper exception
	 */
	public static int checkExists(ZooKeeperWatcher zkw, String znode) throws KeeperException, IOException {
		try {
			Stat s = zkw.getRecoverableZooKeeper().exists(znode, null);
			return s != null ? s.getVersion() : -1;
		} catch (KeeperException e) {
			LOG.warn(zkw.prefix("Unable to set watcher on znode (" + znode + ")"), e);
			zkw.keeperException(e);
			return -1;
		} catch (InterruptedException e) {
			LOG.warn(zkw.prefix("Unable to set watcher on znode (" + znode + ")"), e);
			zkw.interruptedException(e);
			return -1;
		}
	}

	//
	// Znode listings
	//

	/**
	 * Lists the children znodes of the specified znode. Also sets a watch on
	 * the specified znode which will capture a NodeDeleted event on the
	 * specified znode as well as NodeChildrenChanged if any children of the
	 * specified znode are created or deleted.
	 * 
	 * Returns null if the specified node does not exist. Otherwise returns a
	 * list of children of the specified node. If the node exists but it has no
	 * children, an empty list will be returned.
	 * 
	 * @param zkw
	 *            zk reference
	 * @param znode
	 *            path of node to list and watch children of
	 * @return list of children of the specified node, an empty list if the node
	 *         exists but has no children, and null if the node does not exist
	 * @throws KeeperException
	 *             if unexpected zookeeper exception
	 */
	public static List<String> listChildrenAndWatchForNewChildren(ZooKeeperWatcher zkw, String znode)
			throws KeeperException, IOException {
		try {
			List<String> children = zkw.getRecoverableZooKeeper().getChildren(znode, zkw);
			return children;
		} catch (KeeperException.NoNodeException ke) {
			LOG.debug(zkw.prefix("Unable to list children of znode " + znode + " " +
					"because node does not exist (not an error)"));
			return null;
		} catch (KeeperException e) {
			LOG.warn(zkw.prefix("Unable to list children of znode " + znode + " "), e);
			zkw.keeperException(e);
			return null;
		} catch (InterruptedException e) {
			LOG.warn(zkw.prefix("Unable to list children of znode " + znode + " "), e);
			zkw.interruptedException(e);
			return null;
		}
	}

	/**
	 * List all the children of the specified znode, setting a watch for
	 * children changes and also setting a watch on every individual child in
	 * order to get the NodeCreated and NodeDeleted events.
	 * 
	 * @param zkw
	 *            zookeeper reference
	 * @param znode
	 *            node to get children of and watch
	 * @return list of znode names, null if the node doesn't exist
	 * @throws KeeperException
	 */
	public static List<String> listChildrenAndWatchThem(ZooKeeperWatcher zkw, String znode) throws KeeperException,
			IOException {
		List<String> children = listChildrenAndWatchForNewChildren(zkw, znode);
		if (children == null) {
			return null;
		}
		for (String child : children) {
			watchAndCheckExists(zkw, joinZNode(znode, child));
		}
		return children;
	}

	/**
	 * Lists the children of the specified znode without setting any watches.
	 * 
	 * Sets no watches at all, this method is best effort.
	 * 
	 * Returns an empty list if the node has no children. Returns null if the
	 * parent node itself does not exist.
	 * 
	 * @param zkw
	 *            zookeeper reference
	 * @param znode
	 *            node to get children
	 * @return list of data of children of specified znode, empty if no
	 *         children, null if parent does not exist
	 * @throws KeeperException
	 *             if unexpected zookeeper exception
	 */
	public static List<String> listChildrenNoWatch(ZooKeeperWatcher zkw, String znode) throws KeeperException,
			IOException {
		List<String> children = null;
		try {
			// List the children without watching
			children = zkw.getRecoverableZooKeeper().getChildren(znode, null);
		} catch (KeeperException.NoNodeException nne) {
			return null;
		} catch (InterruptedException ie) {
			zkw.interruptedException(ie);
		}
		return children;
	}

	/**
	 * Simple class to hold a node path and node data.
	 * 
	 * @deprecated Unused
	 */
	@Deprecated
	public static class NodeAndData {
		private String node;
		private byte[] data;

		public NodeAndData(String node, byte[] data) {
			this.node = node;
			this.data = data;
		}

		public String getNode() {
			return node;
		}

		public byte[] getData() {
			return data;
		}

		@Override
		public String toString() {
			return node;
		}

		public boolean isEmpty() {
			return (data.length == 0);
		}
	}

	/**
	 * Checks if the specified znode has any children. Sets no watches.
	 * 
	 * Returns true if the node exists and has children. Returns false if the
	 * node does not exist or if the node does not have any children.
	 * 
	 * Used during master initialization to determine if the master is a
	 * failed-over-to master or the first master during initial cluster startup.
	 * If the directory for regionserver ephemeral nodes is empty then this is a
	 * cluster startup, if not then it is not cluster startup.
	 * 
	 * @param zkw
	 *            zk reference
	 * @param znode
	 *            path of node to check for children of
	 * @return true if node has children, false if not or node does not exist
	 * @throws KeeperException
	 *             if unexpected zookeeper exception
	 */
	public static boolean nodeHasChildren(ZooKeeperWatcher zkw, String znode) throws KeeperException, IOException {
		try {
			return !zkw.getRecoverableZooKeeper().getChildren(znode, null).isEmpty();
		} catch (KeeperException.NoNodeException ke) {
			LOG.debug(zkw.prefix("Unable to list children of znode " + znode + " " +
					"because node does not exist (not an error)"));
			return false;
		} catch (KeeperException e) {
			LOG.warn(zkw.prefix("Unable to list children of znode " + znode), e);
			zkw.keeperException(e);
			return false;
		} catch (InterruptedException e) {
			LOG.warn(zkw.prefix("Unable to list children of znode " + znode), e);
			zkw.interruptedException(e);
			return false;
		}
	}

	/**
	 * Get the number of children of the specified node.
	 * 
	 * If the node does not exist or has no children, returns 0.
	 * 
	 * Sets no watches at all.
	 * 
	 * @param zkw
	 *            zk reference
	 * @param znode
	 *            path of node to count children of
	 * @return number of children of specified node, 0 if none or parent does
	 *         not exist
	 * @throws KeeperException
	 *             if unexpected zookeeper exception
	 */
	public static int getNumberOfChildren(ZooKeeperWatcher zkw, String znode) throws KeeperException, IOException {
		try {
			Stat stat = zkw.getRecoverableZooKeeper().exists(znode, null);
			return stat == null ? 0 : stat.getNumChildren();
		} catch (KeeperException e) {
			LOG.warn(zkw.prefix("Unable to get children of node " + znode));
			zkw.keeperException(e);
		} catch (InterruptedException e) {
			zkw.interruptedException(e);
		}
		return 0;
	}

	//
	// Data retrieval
	//

	/**
	 * Get znode data. Does not set a watcher.
	 * 
	 * @return ZNode data, null if the node does not exist or if there is an
	 *         error.
	 * @throws KeeperException
	 */
	public static byte[] getData(ZooKeeperWatcher zkw, String znode) throws IOException, KeeperException {
		try {
			byte[] data = zkw.getRecoverableZooKeeper().getData(znode, null, null);
			logRetrievedMsg(zkw, znode, data, false);
			return data;
		} catch (KeeperException.NoNodeException e) {
			LOG.debug(zkw.prefix("Unable to get data of znode " + znode + " " +
					"because node does not exist (not an error)"));
			return null;
		} catch (KeeperException e) {
			LOG.warn(zkw.prefix("Unable to get data of znode " + znode), e);
			zkw.keeperException(e);
			return null;
		} catch (InterruptedException e) {
			LOG.warn(zkw.prefix("Unable to get data of znode " + znode), e);
			zkw.interruptedException(e);
			return null;
		}
	}

	/**
	 * Get the data at the specified znode and set a watch.
	 * 
	 * Returns the data and sets a watch if the node exists. Returns null and no
	 * watch is set if the node does not exist or there is an exception.
	 * 
	 * @param zkw
	 *            zk reference
	 * @param znode
	 *            path of node
	 * @return data of the specified znode, or null
	 * @throws KeeperException
	 *             if unexpected zookeeper exception
	 */
	public static byte[] getDataAndWatch(ZooKeeperWatcher zkw, String znode) throws KeeperException, IOException {
		return getDataInternal(zkw, znode, null, true);
	}

	/**
	 * Get the data at the specified znode and set a watch.
	 * 
	 * Returns the data and sets a watch if the node exists. Returns null and no
	 * watch is set if the node does not exist or there is an exception.
	 * 
	 * @param zkw
	 *            zk reference
	 * @param znode
	 *            path of node
	 * @param stat
	 *            object to populate the version of the znode
	 * @return data of the specified znode, or null
	 * @throws KeeperException
	 *             if unexpected zookeeper exception
	 */
	public static byte[] getDataAndWatch(ZooKeeperWatcher zkw, String znode, Stat stat) throws KeeperException,
			IOException {
		return getDataInternal(zkw, znode, stat, true);
	}

	private static byte[] getDataInternal(ZooKeeperWatcher zkw, String znode, Stat stat, boolean watcherSet)
			throws KeeperException, IOException {
		try {
			byte[] data = zkw.getRecoverableZooKeeper().getData(znode, zkw, stat);
			logRetrievedMsg(zkw, znode, data, watcherSet);
			return data;
		} catch (KeeperException.NoNodeException e) {
			// This log can get pretty annoying when we cycle on 100ms waits.
			// Enable trace if you really want to see it.
			LOG.trace(zkw.prefix("Unable to get data of znode " + znode + " " +
					"because node does not exist (not an error)"));
			return null;
		} catch (KeeperException e) {
			LOG.warn(zkw.prefix("Unable to get data of znode " + znode), e);
			zkw.keeperException(e);
			return null;
		} catch (InterruptedException e) {
			LOG.warn(zkw.prefix("Unable to get data of znode " + znode), e);
			zkw.interruptedException(e);
			return null;
		}
	}

	/**
	 * Get the data at the specified znode without setting a watch.
	 * 
	 * Returns the data if the node exists. Returns null if the node does not
	 * exist.
	 * 
	 * Sets the stats of the node in the passed Stat object. Pass a null stat if
	 * not interested.
	 * 
	 * @param zkw
	 *            zk reference
	 * @param znode
	 *            path of node
	 * @param stat
	 *            node status to get if node exists
	 * @return data of the specified znode, or null if node does not exist
	 * @throws KeeperException
	 *             if unexpected zookeeper exception
	 */
	public static byte[] getDataNoWatch(ZooKeeperWatcher zkw, String znode, Stat stat) throws KeeperException,
			IOException {
		try {
			byte[] data = zkw.getRecoverableZooKeeper().getData(znode, null, stat);
			logRetrievedMsg(zkw, znode, data, false);
			return data;
		} catch (KeeperException.NoNodeException e) {
			LOG.debug(zkw.prefix("Unable to get data of znode " + znode + " " +
					"because node does not exist (not necessarily an error)"));
			return null;
		} catch (KeeperException e) {
			LOG.warn(zkw.prefix("Unable to get data of znode " + znode), e);
			zkw.keeperException(e);
			return null;
		} catch (InterruptedException e) {
			LOG.warn(zkw.prefix("Unable to get data of znode " + znode), e);
			zkw.interruptedException(e);
			return null;
		}
	}

	/**
	 * Returns the date of child znodes of the specified znode. Also sets a
	 * watch on the specified znode which will capture a NodeDeleted event on
	 * the specified znode as well as NodeChildrenChanged if any children of the
	 * specified znode are created or deleted.
	 * 
	 * Returns null if the specified node does not exist. Otherwise returns a
	 * list of children of the specified node. If the node exists but it has no
	 * children, an empty list will be returned.
	 * 
	 * @param zkw
	 *            zk reference
	 * @param baseNode
	 *            path of node to list and watch children of
	 * @return list of data of children of the specified node, an empty list if
	 *         the node exists but has no children, and null if the node does
	 *         not exist
	 * @throws KeeperException
	 *             if unexpected zookeeper exception
	 * @deprecated Unused
	 */
	public static List<NodeAndData> getChildDataAndWatchForNewChildren(ZooKeeperWatcher zkw, String baseNode)
			throws KeeperException, IOException {
		List<String> nodes = ZKUtil.listChildrenAndWatchForNewChildren(zkw, baseNode);
		List<NodeAndData> newNodes = new ArrayList<NodeAndData>();
		if (nodes != null) {
			for (String node : nodes) {
				String nodePath = ZKUtil.joinZNode(baseNode, node);
				byte[] data = ZKUtil.getDataAndWatch(zkw, nodePath);
				newNodes.add(new NodeAndData(nodePath, data));
			}
		}
		return newNodes;
	}

	/**
	 * Update the data of an existing node with the expected version to have the
	 * specified data.
	 * 
	 * Throws an exception if there is a version mismatch or some other problem.
	 * 
	 * Sets no watches under any conditions.
	 * 
	 * @param zkw
	 *            zk reference
	 * @param znode
	 * @param data
	 * @param expectedVersion
	 * @throws KeeperException
	 *             if unexpected zookeeper exception
	 * @throws KeeperException.BadVersionException
	 *             if version mismatch
	 * @deprecated Unused
	 */
	public static void updateExistingNodeData(ZooKeeperWatcher zkw, String znode, byte[] data, int expectedVersion)
			throws KeeperException, IOException {
		try {
			zkw.getRecoverableZooKeeper().setData(znode, data, expectedVersion);
		} catch (InterruptedException ie) {
			zkw.interruptedException(ie);
		}
	}

	//
	// Data setting
	//

	/**
	 * Sets the data of the existing znode to be the specified data. Ensures
	 * that the current data has the specified expected version.
	 * 
	 * <p>
	 * If the node does not exist, a {@link NoNodeException} will be thrown.
	 * 
	 * <p>
	 * If their is a version mismatch, method returns null.
	 * 
	 * <p>
	 * No watches are set but setting data will trigger other watchers of this
	 * node.
	 * 
	 * <p>
	 * If there is another problem, a KeeperException will be thrown.
	 * 
	 * @param zkw
	 *            zk reference
	 * @param znode
	 *            path of node
	 * @param data
	 *            data to set for node
	 * @param expectedVersion
	 *            version expected when setting data
	 * @return true if data set, false if version mismatch
	 * @throws KeeperException
	 *             if unexpected zookeeper exception
	 */
	public static boolean setData(ZooKeeperWatcher zkw, String znode, byte[] data, int expectedVersion)
			throws KeeperException, IOException, KeeperException.NoNodeException {
		try {
			return zkw.getRecoverableZooKeeper().setData(znode, data, expectedVersion) != null;
		} catch (InterruptedException e) {
			zkw.interruptedException(e);
			return false;
		}
	}

	/**
	 * Set data into node creating node if it doesn't yet exist. Does not set
	 * watch.
	 * 
	 * @param zkw
	 *            zk reference
	 * @param znode
	 *            path of node
	 * @param data
	 *            data to set for node
	 * @throws KeeperException
	 */
	public static void createSetData(final ZooKeeperWatcher zkw, final String znode, final byte[] data)
			throws KeeperException, IOException {
		if (checkExists(zkw, znode) == -1) {
			ZKUtil.createWithParents(zkw, znode, data);
		} else {
			ZKUtil.setData(zkw, znode, data);
		}
	}

	/**
	 * Sets the data of the existing znode to be the specified data. The node
	 * must exist but no checks are done on the existing data or version.
	 * 
	 * <p>
	 * If the node does not exist, a {@link NoNodeException} will be thrown.
	 * 
	 * <p>
	 * No watches are set but setting data will trigger other watchers of this
	 * node.
	 * 
	 * <p>
	 * If there is another problem, a KeeperException will be thrown.
	 * 
	 * @param zkw
	 *            zk reference
	 * @param znode
	 *            path of node
	 * @param data
	 *            data to set for node
	 * @throws KeeperException
	 *             if unexpected zookeeper exception
	 */
	public static void setData(ZooKeeperWatcher zkw, String znode, byte[] data) throws KeeperException,
			KeeperException.NoNodeException, IOException {
		setData(zkw, (SetData) ZKUtilOp.setData(znode, data));
	}

	private static void setData(ZooKeeperWatcher zkw, SetData setData) throws KeeperException,
			KeeperException.NoNodeException, IOException {
		SetDataRequest sd = (SetDataRequest) toZooKeeperOp(zkw, setData).toRequestRecord();
		setData(zkw, sd.getPath(), sd.getData(), sd.getVersion());
	}

	/**
	 * Returns whether or not secure authentication is enabled (whether
	 * <code>hbase.security.authentication</code> is set to
	 * <code>kerberos</code>.
	 */
	public static boolean isSecureZooKeeper(Configuration conf) {
		// hbase shell need to use:
		// -Djava.security.auth.login.config=user-jaas.conf
		// since each user has a different jaas.conf
		if (System.getProperty("java.security.auth.login.config") != null)
			return true;

		// Master & RSs uses hbase.zookeeper.client.*
		return ("kerberos".equalsIgnoreCase(conf.get("hbase.security.authentication")) && conf
				.get("hbase.zookeeper.client.keytab.file") != null);
	}

	private static ArrayList<ACL> createACL(ZooKeeperWatcher zkw, String node) {
		if (isSecureZooKeeper(zkw.getConfiguration())) {
			// Certain znodes are accessed directly by the client,
			// so they must be readable by non-authenticated clients
			if ((node.equals(zkw.baseZNode) == true) || (node.startsWith(zkw.dimportHostNode) == true) ||
					(node.equals(zkw.dimportMasterNode) == true) || (node.equals(zkw.dimportOrderNode) == true) ||
					(node.startsWith(zkw.dimportRunTaskNode) == true)) {
				return ZooKeeperWatcher.CREATOR_ALL_AND_WORLD_READABLE;
			}
			return Ids.CREATOR_ALL_ACL;
		} else {
			return Ids.OPEN_ACL_UNSAFE;
		}
	}

	//
	// Node creation
	//

	/**
	 * 
	 * Set the specified znode to be an ephemeral node carrying the specified
	 * data.
	 * 
	 * If the node is created successfully, a watcher is also set on the node.
	 * 
	 * If the node is not created successfully because it already exists, this
	 * method will also set a watcher on the node.
	 * 
	 * If there is another problem, a KeeperException will be thrown.
	 * 
	 * @param zkw
	 *            zk reference
	 * @param znode
	 *            path of node
	 * @param data
	 *            data of node
	 * @return true if node created, false if not, watch set in both cases
	 * @throws KeeperException
	 *             if unexpected zookeeper exception
	 */
	public static boolean createEphemeralNodeAndWatch(ZooKeeperWatcher zkw, String znode, byte[] data)
			throws KeeperException, IOException {
		try {
			zkw.getRecoverableZooKeeper().create(znode, data, createACL(zkw, znode), CreateMode.EPHEMERAL);
		} catch (KeeperException.NodeExistsException nee) {
			if (!watchAndCheckExists(zkw, znode)) {
				// It did exist but now it doesn't, try again
				return createEphemeralNodeAndWatch(zkw, znode, data);
			}
			return false;
		} catch (InterruptedException e) {
			LOG.info("Interrupted", e);
			Thread.currentThread().interrupt();
		}
		return true;
	}

	/**
	 * Creates the specified znode to be a persistent node carrying the
	 * specified data.
	 * 
	 * Returns true if the node was successfully created, false if the node
	 * already existed.
	 * 
	 * If the node is created successfully, a watcher is also set on the node.
	 * 
	 * If the node is not created successfully because it already exists, this
	 * method will also set a watcher on the node but return false.
	 * 
	 * If there is another problem, a KeeperException will be thrown.
	 * 
	 * @param zkw
	 *            zk reference
	 * @param znode
	 *            path of node
	 * @param data
	 *            data of node
	 * @return true if node created, false if not, watch set in both cases
	 * @throws KeeperException
	 *             if unexpected zookeeper exception
	 */
	public static boolean createNodeIfNotExistsAndWatch(ZooKeeperWatcher zkw, String znode, byte[] data)
			throws KeeperException, IOException {
		try {
			zkw.getRecoverableZooKeeper().create(znode, data, createACL(zkw, znode), CreateMode.PERSISTENT);
		} catch (KeeperException.NodeExistsException nee) {
			try {
				zkw.getRecoverableZooKeeper().exists(znode, zkw);
			} catch (InterruptedException e) {
				zkw.interruptedException(e);
				return false;
			}
			return false;
		} catch (InterruptedException e) {
			zkw.interruptedException(e);
			return false;
		}
		return true;
	}

	/**
	 * Creates the specified znode with the specified data but does not watch
	 * it.
	 * 
	 * Returns the znode of the newly created node
	 * 
	 * If there is another problem, a KeeperException will be thrown.
	 * 
	 * @param zkw
	 *            zk reference
	 * @param znode
	 *            path of node
	 * @param data
	 *            data of node
	 * @param createMode
	 *            specifying whether the node to be created is ephemeral and/or
	 *            sequential
	 * @return true name of the newly created znode or null
	 * @throws KeeperException
	 *             if unexpected zookeeper exception
	 * @throws IOException
	 */
	public static String createNodeIfNotExistsNoWatch(ZooKeeperWatcher zkw, String znode, byte[] data,
			CreateMode createMode) throws KeeperException, IOException {

		String createdZNode = null;
		try {
			createdZNode = zkw.getRecoverableZooKeeper().create(znode, data, createACL(zkw, znode), createMode);
		} catch (KeeperException.NodeExistsException nee) {
			return znode;
		} catch (InterruptedException e) {
			zkw.interruptedException(e);
			return null;
		}
		return createdZNode;
	}

	/**
	 * Creates the specified node with the specified data and watches it.
	 * 
	 * <p>
	 * Throws an exception if the node already exists.
	 * 
	 * <p>
	 * The node created is persistent and open access.
	 * 
	 * <p>
	 * Returns the version number of the created node if successful.
	 * 
	 * @param zkw
	 *            zk reference
	 * @param znode
	 *            path of node to create
	 * @param data
	 *            data of node to create
	 * @return version of node created
	 * @throws KeeperException
	 *             if unexpected zookeeper exception
	 * @throws KeeperException.NodeExistsException
	 *             if node already exists
	 */
	public static int createAndWatch(ZooKeeperWatcher zkw, String znode, byte[] data) throws KeeperException,
			IOException, KeeperException.NodeExistsException {
		try {
			zkw.getRecoverableZooKeeper().create(znode, data, createACL(zkw, znode), CreateMode.PERSISTENT);
			Stat stat = zkw.getRecoverableZooKeeper().exists(znode, zkw);
			if (stat == null) {
				// Likely a race condition. Someone deleted the znode.
				throw KeeperException.create(KeeperException.Code.SYSTEMERROR,
						"ZK.exists returned null (i.e.: znode does not exist) for znode=" + znode);
			}
			return stat.getVersion();
		} catch (InterruptedException e) {
			zkw.interruptedException(e);
			return -1;
		}
	}

	/**
	 * Async creates the specified node with the specified data.
	 * 
	 * <p>
	 * Throws an exception if the node already exists.
	 * 
	 * <p>
	 * The node created is persistent and open access.
	 * 
	 * @param zkw
	 *            zk reference
	 * @param znode
	 *            path of node to create
	 * @param data
	 *            data of node to create
	 * @param cb
	 * @param ctx
	 * @throws KeeperException
	 *             if unexpected zookeeper exception
	 * @throws KeeperException.NodeExistsException
	 *             if node already exists
	 */
	public static void asyncCreate(ZooKeeperWatcher zkw, String znode, byte[] data,
			final AsyncCallback.StringCallback cb, final Object ctx) {
		zkw.getRecoverableZooKeeper().getZooKeeper()
				.create(znode, data, createACL(zkw, znode), CreateMode.PERSISTENT, cb, ctx);
	}

	/**
	 * Creates the specified node, iff the node does not exist. Does not set a
	 * watch and fails silently if the node already exists.
	 * 
	 * The node created is persistent and open access.
	 * 
	 * @param zkw
	 *            zk reference
	 * @param znode
	 *            path of node
	 * @throws KeeperException
	 *             if unexpected zookeeper exception
	 */
	public static void createAndFailSilent(ZooKeeperWatcher zkw, String znode) throws KeeperException, IOException {
		createAndFailSilent(zkw, znode, new byte[0]);
	}

	/**
	 * Creates the specified node containing specified data, iff the node does
	 * not exist. Does not set a watch and fails silently if the node already
	 * exists.
	 * 
	 * The node created is persistent and open access.
	 * 
	 * @param zkw
	 *            zk reference
	 * @param znode
	 *            path of node
	 * @param data
	 *            a byte array data to store in the znode
	 * @throws KeeperException
	 *             if unexpected zookeeper exception
	 */
	public static void createAndFailSilent(ZooKeeperWatcher zkw, String znode, byte[] data) throws KeeperException,
			IOException {
		createAndFailSilent(zkw, (CreateAndFailSilent) ZKUtilOp.createAndFailSilent(znode, data));
	}

	private static void createAndFailSilent(ZooKeeperWatcher zkw, CreateAndFailSilent cafs) throws KeeperException,
			IOException {
		CreateRequest create = (CreateRequest) toZooKeeperOp(zkw, cafs).toRequestRecord();
		String znode = create.getPath();
		try {
			RecoverableZooKeeper zk = zkw.getRecoverableZooKeeper();
			if (zk.exists(znode, false) == null) {
				zk.create(znode, create.getData(), create.getAcl(), CreateMode.fromFlag(create.getFlags()));
			}
		} catch (KeeperException.NodeExistsException nee) {
		} catch (KeeperException.NoAuthException nee) {
			try {
				if (null == zkw.getRecoverableZooKeeper().exists(znode, false)) {
					// If we failed to create the file and it does not already
					// exist.
					throw (nee);
				}
			} catch (InterruptedException ie) {
				zkw.interruptedException(ie);
			}

		} catch (InterruptedException ie) {
			zkw.interruptedException(ie);
		}
	}

	/**
	 * Creates the specified node and all parent nodes required for it to exist.
	 * 
	 * No watches are set and no errors are thrown if the node already exists.
	 * 
	 * The nodes created are persistent and open access.
	 * 
	 * @param zkw
	 *            zk reference
	 * @param znode
	 *            path of node
	 * @throws KeeperException
	 *             if unexpected zookeeper exception
	 * @throws IOException
	 */
	public static void createWithParents(ZooKeeperWatcher zkw, String znode) throws KeeperException, IOException {
		createWithParents(zkw, znode, new byte[0]);
	}

	/**
	 * Creates the specified node and all parent nodes required for it to exist.
	 * The creation of parent znodes is not atomic with the leafe znode creation
	 * but the data is written atomically when the leaf node is created.
	 * 
	 * No watches are set and no errors are thrown if the node already exists.
	 * 
	 * The nodes created are persistent and open access.
	 * 
	 * @param zkw
	 *            zk reference
	 * @param znode
	 *            path of node
	 * @throws KeeperException
	 *             if unexpected zookeeper exception
	 * @throws IOException
	 */
	public static void createWithParents(ZooKeeperWatcher zkw, String znode, byte[] data) throws KeeperException,
			IOException {
		try {
			if (znode == null) {
				return;
			}
			zkw.getRecoverableZooKeeper().create(znode, data, createACL(zkw, znode), CreateMode.PERSISTENT);
		} catch (KeeperException.NodeExistsException nee) {
			return;
		} catch (KeeperException.NoNodeException nne) {
			createWithParents(zkw, getParent(znode));
			createWithParents(zkw, znode, data);
		} catch (InterruptedException ie) {
			zkw.interruptedException(ie);
		}
	}

	//
	// Deletes
	//

	/**
	 * Delete the specified node. Sets no watches. Throws all exceptions.
	 * 
	 * @throws IOException
	 */
	public static void deleteNode(ZooKeeperWatcher zkw, String node) throws KeeperException, IOException {
		deleteNode(zkw, node, -1);
	}

	/**
	 * Delete the specified node with the specified version. Sets no watches.
	 * Throws all exceptions.
	 * 
	 * @throws IOException
	 */
	public static boolean deleteNode(ZooKeeperWatcher zkw, String node, int version) throws KeeperException,
			IOException {
		try {
			zkw.getRecoverableZooKeeper().delete(node, version);
			return true;
		} catch (KeeperException.BadVersionException bve) {
			return false;
		} catch (InterruptedException ie) {
			zkw.interruptedException(ie);
			return false;
		}
	}

	/**
	 * Deletes the specified node. Fails silent if the node does not exist.
	 * 
	 * @param zkw
	 * @param node
	 * @throws KeeperException
	 */
	public static void deleteNodeFailSilent(ZooKeeperWatcher zkw, String node) throws KeeperException, IOException {
		deleteNodeFailSilent(zkw, (DeleteNodeFailSilent) ZKUtilOp.deleteNodeFailSilent(node));
	}

	private static void deleteNodeFailSilent(ZooKeeperWatcher zkw, DeleteNodeFailSilent dnfs) throws KeeperException,
			IOException {
		DeleteRequest delete = (DeleteRequest) toZooKeeperOp(zkw, dnfs).toRequestRecord();
		try {
			zkw.getRecoverableZooKeeper().delete(delete.getPath(), delete.getVersion());
		} catch (KeeperException.NoNodeException nne) {
		} catch (InterruptedException ie) {
			zkw.interruptedException(ie);
		}
	}

	/**
	 * Delete the specified node and all of it's children.
	 * <p>
	 * If the node does not exist, just returns.
	 * <p>
	 * Sets no watches. Throws all exceptions besides dealing with deletion of
	 * children.
	 */
	public static void deleteNodeRecursively(ZooKeeperWatcher zkw, String node) throws KeeperException, IOException {
		try {
			List<String> children = ZKUtil.listChildrenNoWatch(zkw, node);
			// the node is already deleted, so we just finish
			if (children == null)
				return;

			if (!children.isEmpty()) {
				for (String child : children) {
					deleteNodeRecursively(zkw, joinZNode(node, child));
				}
			}
			zkw.getRecoverableZooKeeper().delete(node, -1);
		} catch (InterruptedException ie) {
			zkw.interruptedException(ie);
		}
	}

	/**
	 * Delete all the children of the specified node but not the node itself.
	 * 
	 * Sets no watches. Throws all exceptions besides dealing with deletion of
	 * children.
	 */
	public static void deleteChildrenRecursively(ZooKeeperWatcher zkw, String node) throws KeeperException, IOException {
		List<String> children = ZKUtil.listChildrenNoWatch(zkw, node);
		if (children == null || children.isEmpty())
			return;
		for (String child : children) {
			deleteNodeRecursively(zkw, joinZNode(node, child));
		}
	}

	/**
	 * Represents an action taken by ZKUtil, e.g. createAndFailSilent. These
	 * actions are higher-level than ZKOp actions, which represent individual
	 * actions in the ZooKeeper API, like create.
	 */
	public abstract static class ZKUtilOp {
		private String path;

		private ZKUtilOp(String path) {
			this.path = path;
		}

		/**
		 * @return a createAndFailSilent ZKUtilOp
		 */
		public static ZKUtilOp createAndFailSilent(String path, byte[] data) {
			return new CreateAndFailSilent(path, data);
		}

		/**
		 * @return a deleteNodeFailSilent ZKUtilOP
		 */
		public static ZKUtilOp deleteNodeFailSilent(String path) {
			return new DeleteNodeFailSilent(path);
		}

		/**
		 * @return a setData ZKUtilOp
		 */
		public static ZKUtilOp setData(String path, byte[] data) {
			return new SetData(path, data);
		}

		/**
		 * @return path to znode where the ZKOp will occur
		 */
		public String getPath() {
			return path;
		}

		/**
		 * ZKUtilOp representing createAndFailSilent in ZooKeeper (attempt to
		 * create node, ignore error if already exists)
		 */
		public static class CreateAndFailSilent extends ZKUtilOp {
			private byte[] data;

			private CreateAndFailSilent(String path, byte[] data) {
				super(path);
				this.data = data;
			}

			public byte[] getData() {
				return data;
			}

			@Override
			public boolean equals(Object o) {
				if (this == o)
					return true;
				if (!(o instanceof CreateAndFailSilent))
					return false;

				CreateAndFailSilent op = (CreateAndFailSilent) o;
				return getPath().equals(op.getPath()) && Arrays.equals(data, op.data);
			}

			@Override
			public int hashCode() {
				int ret = 17 + getPath().hashCode() * 31;
				return ret * 31 + Bytes.hashCode(data);
			}
		}

		/**
		 * ZKUtilOp representing deleteNodeFailSilent in ZooKeeper (attempt to
		 * delete node, ignore error if node doesn't exist)
		 */
		public static class DeleteNodeFailSilent extends ZKUtilOp {
			private DeleteNodeFailSilent(String path) {
				super(path);
			}

			@Override
			public boolean equals(Object o) {
				if (this == o)
					return true;
				if (!(o instanceof DeleteNodeFailSilent))
					return false;

				return super.equals(o);
			}

			@Override
			public int hashCode() {
				return getPath().hashCode();
			}
		}

		/**
		 * ZKUtilOp representing setData in ZooKeeper
		 */
		public static class SetData extends ZKUtilOp {
			private byte[] data;

			private SetData(String path, byte[] data) {
				super(path);
				this.data = data;
			}

			public byte[] getData() {
				return data;
			}

			@Override
			public boolean equals(Object o) {
				if (this == o)
					return true;
				if (!(o instanceof SetData))
					return false;

				SetData op = (SetData) o;
				return getPath().equals(op.getPath()) && Arrays.equals(data, op.data);
			}

			@Override
			public int hashCode() {
				int ret = getPath().hashCode();
				return ret * 31 + Bytes.hashCode(data);
			}
		}
	}

	/**
	 * Convert from ZKUtilOp to ZKOp
	 */
	private static Op toZooKeeperOp(ZooKeeperWatcher zkw, ZKUtilOp op) throws UnsupportedOperationException {
		if (op == null)
			return null;

		if (op instanceof CreateAndFailSilent) {
			CreateAndFailSilent cafs = (CreateAndFailSilent) op;
			return Op.create(cafs.getPath(), cafs.getData(), createACL(zkw, cafs.getPath()), CreateMode.PERSISTENT);
		} else if (op instanceof DeleteNodeFailSilent) {
			DeleteNodeFailSilent dnfs = (DeleteNodeFailSilent) op;
			return Op.delete(dnfs.getPath(), -1);
		} else if (op instanceof SetData) {
			SetData sd = (SetData) op;
			return Op.setData(sd.getPath(), sd.getData(), -1);
		} else {
			throw new UnsupportedOperationException("Unexpected ZKUtilOp type: " + op.getClass().getName());
		}
	}

	/**
	 * If hbase.zookeeper.useMulti is true, use ZooKeeper's multi-update
	 * functionality. Otherwise, run the list of operations sequentially.
	 * 
	 * If all of the following are true: - runSequentialOnMultiFailure is true -
	 * hbase.zookeeper.useMulti is true - on calling multi, we get a ZooKeeper
	 * exception that can be handled by a sequential call(*) Then: - we retry
	 * the operations one-by-one (sequentially)
	 * 
	 * Note *: an example is receiving a NodeExistsException from a "create"
	 * call. Without multi, a user could call "createAndFailSilent" to ensure
	 * that a node exists if they don't care who actually created the node (i.e.
	 * the NodeExistsException from ZooKeeper is caught). This will cause all
	 * operations in the multi to fail, however, because the NodeExistsException
	 * that zk.create throws will fail the multi transaction. In this case, if
	 * the previous conditions hold, the commands are run sequentially, which
	 * should result in the correct final state, but means that the operations
	 * will not run atomically.
	 * 
	 * @throws KeeperException
	 */
	public static void multiOrSequential(ZooKeeperWatcher zkw, List<ZKUtilOp> ops, boolean runSequentialOnMultiFailure)
			throws KeeperException, IOException {
		if (ops == null)
			return;
		boolean useMulti = zkw.getConfiguration().getBoolean(DImportConstant.ZOOKEEPER_USEMULTI, false);

		if (useMulti) {
			List<Op> zkOps = new LinkedList<Op>();
			for (ZKUtilOp op : ops) {
				zkOps.add(toZooKeeperOp(zkw, op));
			}
			try {
				zkw.getRecoverableZooKeeper().multi(zkOps);
			} catch (KeeperException ke) {
				switch (ke.code()) {
				case NODEEXISTS:
				case NONODE:
				case BADVERSION:
				case NOAUTH:
					// if we get an exception that could be solved by running
					// sequentially
					// (and the client asked us to), then break out and run
					// sequentially
					if (runSequentialOnMultiFailure) {
						LOG.info("On call to ZK.multi, received exception: " + ke.toString() + "." +
								"  Attempting to run operations sequentially because" +
								" runSequentialOnMultiFailure is: " + runSequentialOnMultiFailure + ".");
						processSequentially(zkw, ops);
						break;
					}
				default:
					throw ke;
				}
			} catch (InterruptedException ie) {
				zkw.interruptedException(ie);
			}
		} else {
			// run sequentially
			processSequentially(zkw, ops);
		}

	}

	private static void processSequentially(ZooKeeperWatcher zkw, List<ZKUtilOp> ops) throws KeeperException,
			IOException, NoNodeException {
		for (ZKUtilOp op : ops) {
			if (op instanceof CreateAndFailSilent) {
				createAndFailSilent(zkw, (CreateAndFailSilent) op);
			} else if (op instanceof DeleteNodeFailSilent) {
				deleteNodeFailSilent(zkw, (DeleteNodeFailSilent) op);
			} else if (op instanceof SetData) {
				setData(zkw, (SetData) op);
			} else {
				throw new UnsupportedOperationException("Unexpected ZKUtilOp type: " + op.getClass().getName());
			}
		}
	}

	private static void logRetrievedMsg(final ZooKeeperWatcher zkw, final String znode, final byte[] data,
			final boolean watcherSet) {
		if (!LOG.isTraceEnabled())
			return;
		// LOG.trace(zkw.prefix("Retrieved "
		// + ((data == null) ? 0 : data.length)
		// + " byte(s) of data from znode "
		// + znode
		// + (watcherSet ? " and set watcher; " : "; data=")
		// + (data == null ? "null" : data.length == 0 ? "empty" :
		// (znode.startsWith(zkw.stormBaseZNode) ? ZKAssign.(data) : // We
		// // should
		// // not
		// // be
		// // doing
		// // this
		// // reaching
		// // into
		// // another
		// // class
		// znode.startsWith(zkw.topologyZNode) ?
		// getServerNameOrEmptyString(data) : znode
		// .startsWith(zkw.backupMasterAddressesZNode) ?
		// getServerNameOrEmptyString(data) : StringUtils.abbreviate(
		// Bytes.toStringBinary(data), 32)))));

		// 需要根据节点序列数据
	}

	/**
	 * Waits for HBase installation's base (parent) znode to become available.
	 * 
	 * @throws IOException
	 *             on ZK errors
	 */
	public static void waitForBaseZNode(Configuration conf) throws IOException {
		LOG.info("Waiting until the base znode is available");
		String parentZNode = conf.get(DImportConstant.ZOOKEEPER_BASE_ZNODE,
				DImportConstant.DEFAULT_ZOOKEEPER_ZNODE_PARENT);
		ZooKeeper zk = new ZooKeeper(conf.get(DImportConstant.ZOOKEEPER_QUORUM), conf.getInt(
				DImportConstant.ZK_SESSION_TIMEOUT, DImportConstant.DEFAULT_ZK_SESSION_TIMEOUT), EmptyWatcher.instance);

		final int maxTimeMs = 10000;
		final int maxNumAttempts = maxTimeMs / DImportConstant.SOCKET_RETRY_WAIT_MS;

		KeeperException keeperEx = null;
		try {
			try {
				for (int attempt = 0; attempt < maxNumAttempts; ++attempt) {
					try {
						if (zk.exists(parentZNode, false) != null) {
							LOG.info("Parent znode exists: " + parentZNode);
							keeperEx = null;
							break;
						}
					} catch (KeeperException e) {
						keeperEx = e;
					}
					Threads.sleepWithoutInterrupt(DImportConstant.SOCKET_RETRY_WAIT_MS);
				}
			} finally {
				zk.close();
			}
		} catch (InterruptedException ex) {
			Thread.currentThread().interrupt();
		}

		if (keeperEx != null) {
			throw new IOException(keeperEx);
		}
	}

	public static byte[] blockUntilAvailable(final ZooKeeperWatcher zkw, final String znode, final long timeout)
			throws InterruptedException, IOException {
		if (timeout < 0)
			throw new IllegalArgumentException();
		if (zkw == null)
			throw new IllegalArgumentException();
		if (znode == null)
			throw new IllegalArgumentException();

		byte[] data = null;
		boolean finished = false;
		final long endTime = System.currentTimeMillis() + timeout;
		while (!finished) {
			try {
				data = ZKUtil.getData(zkw, znode);
			} catch (KeeperException e) {
				LOG.warn("Unexpected exception handling blockUntilAvailable", e);
			}
			if (data == null && (System.currentTimeMillis() + DImportConstant.SOCKET_RETRY_WAIT_MS < endTime)) {
				Thread.sleep(DImportConstant.SOCKET_RETRY_WAIT_MS);
			} else {
				finished = true;
			}
		}

		return data;
	}

	/**
	 * Convert a {@link DeserializationException} to a more palatable
	 * {@link KeeperException}. Used when can't let a
	 * {@link DeserializationException} out w/o changing public API.
	 * 
	 * @param e
	 *            Exception to convert
	 * @return Converted exception
	 */
	public static KeeperException convert(final DeserializationException e) {
		KeeperException ke = new KeeperException.DataInconsistencyException();
		ke.initCause(e);
		return ke;
	}

	/**
	 * Recursively print the current state of ZK (non-transactional)
	 * 
	 * @param root
	 *            name of the root directory in zk to print
	 * @throws IOException
	 * @throws KeeperException
	 */
	public static void logZKTree(ZooKeeperWatcher zkw, String root) throws IOException {
		if (!LOG.isDebugEnabled())
			return;
		LOG.debug("Current zk system:");
		String prefix = "|-";
		LOG.debug(prefix + root);
		try {
			logZKTree(zkw, root, prefix);
		} catch (KeeperException e) {
			throw new RuntimeException(e);
		}
	}

	/**
	 * Helper method to print the current state of the ZK tree.
	 * 
	 * @see #logZKTree(ZooKeeperWatcher, String)
	 * @throws KeeperException
	 *             if an unexpected exception occurs
	 */
	protected static void logZKTree(ZooKeeperWatcher zkw, String root, String prefix) throws KeeperException,
			IOException {
		List<String> children = ZKUtil.listChildrenNoWatch(zkw, root);
		if (children == null)
			return;
		for (String child : children) {
			LOG.debug(prefix + child);
			String node = ZKUtil.joinZNode(root.equals("/") ? "" : root, child);
			logZKTree(zkw, node, prefix + "---");
		}
	}

}
