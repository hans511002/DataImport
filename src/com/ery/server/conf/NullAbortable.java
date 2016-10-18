package com.ery.server.conf;

import java.io.Serializable;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.ery.server.zk.ZooKeeperWatcher;

/**
 * 用于错误日志记录
 * 
 * @author hans
 * 
 */
public class NullAbortable implements Server, Serializable {
	private static final Log LOG = LogFactory.getLog(NullAbortable.class);

	private static final long serialVersionUID = 7043134863928858819L;
	private Object obj = null;

	public void setObj(Object obj) {
		this.obj = obj;
	}

	@Override
	public void abort(String why, Throwable e) {
		if (obj != null)
			LOG.error(why + obj, e);
		else
			LOG.error(why, e);
	}

	@Override
	public boolean isAborted() {
		return false;
	}

	@Override
	public boolean isStopped() {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public void stop(String why) {
		if (obj != null)
			LOG.error(why + obj);
		else
			LOG.error(why);

	}

	@Override
	public Configuration getConfiguration() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public ZooKeeperWatcher getZooKeeper() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public String getHotName() {
		return null;
	}

}
