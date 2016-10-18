package com.ery.server.conf;

import java.util.Map;
import java.util.Properties;

public class HadoopConf extends org.apache.hadoop.conf.Configuration {
	static HadoopConf _conf = null;

	public HadoopConf() {
		super(true);
	}

	public static HadoopConf getConf(Configuration conf) {
		if (_conf == null) {
			_conf = new HadoopConf();
			Properties pro = conf.getProps();
			for (Map.Entry<Object, Object> key : pro.entrySet()) {
				_conf.set(key.getKey().toString(), key.getValue().toString());
			}
		}
		return _conf;
	}

	public static HadoopConf getConf(Configuration conf, String[] procStorePar) {
		if (_conf == null) {
			getConf(conf);
		}
		if (procStorePar != null) {
			for (String par : procStorePar) {
				String[] tmp = par.split("=");
				_conf.set(tmp[0], tmp[1]);
			}
		}
		return _conf;
	}
}
