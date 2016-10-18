package com.ery.server.zk;

import java.io.Serializable;
import java.util.Hashtable;

import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;

/**
 * * �ṩ����clientʹ�õ�watcher *
 * 
 * @author ransom *
 */
public class MultiWatcher implements Watcher, Serializable {
	private static final long serialVersionUID = 2192958127365249167L;
	private ZooKeeperServer serv = null;
	private Hashtable<String, String> events = new Hashtable<String, String>();
	private Hashtable<String, Watcher> proObj = new Hashtable<String, Watcher>();

	public MultiWatcher(ZooKeeperServer serv) {
		this.serv = serv;
	}

	public void addWatchHandle(String path, String className) {
		events.put(path, className);
	}

	public void process(String classStr, WatchedEvent event) throws ClassNotFoundException, InstantiationException, IllegalAccessException {
		Watcher obj = null;
		if (proObj.containsKey(classStr)) {
			obj = proObj.get(classStr);
		} else {
			Class<?> t = Class.forName(classStr);
			obj = (Watcher) t.newInstance();
		}
		obj.process(event);
	}

	public void process(WatchedEvent event) {
		if (events != null) {
			if (event.getPath() != null) {
				String outputStr = "path:" + event.getPath();
				outputStr += ",state:" + event.getState();
				outputStr += ",type:" + event.getType();
				System.out.println(outputStr);
			}
			// event.getType()== org.apache.zookeeper.Watcher.Event.EventType.
			if (event.getState() == org.apache.zookeeper.Watcher.Event.KeeperState.Expired
					|| event.getState() == org.apache.zookeeper.Watcher.Event.KeeperState.Disconnected)// AuthFailed)
				serv.connect();
			String path = event.getPath();
			if (path != null && path.equals("") == false) {

				String classStr = "";
				if (events.containsKey(path))
					classStr = events.get(path);
				else if (events.containsKey("*"))
					classStr = events.get("*");
				try {
					if (classStr == null || classStr.equals("")) {
						for (String p : events.keySet()) {
							if (path.startsWith(p)) {
								classStr = events.get(p);
								process(classStr, event);
							}
						}
					} else {
						process(classStr, event);
					}
					// java.lang.reflect.Method method = t.getMethod("process", WatchedEvent.class);
					// method.invoke(obj, event);
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
		}

	}
}
