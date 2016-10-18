package com.ery.dimport.daemon;

import java.io.Serializable;
import java.util.Date;

import com.ery.base.support.utils.Convert;
import com.ery.dimport.DImportConstant;

public class HostInfo implements Serializable {
	private static final long serialVersionUID = 3472416741736769079L;
	public long startTime;
	public long deadTime;
	public int infoPort;
	public String hostName;

	public HostInfo(String hostInfo) {
		String tmp[] = hostInfo.split(":");
		this.hostName = tmp[0];
		if (tmp.length > 1)
			this.infoPort = Convert.toInt(tmp[1], 0);
		if (tmp.length > 2)
			this.startTime = Convert.toLong(tmp[2], 0);
		if (tmp.length > 3)
			this.deadTime = Convert.toLong(tmp[3], 0);

	}

	public HostInfo(String hostName, int infoPort, long startTime) {
		this.hostName = hostName;
		this.infoPort = infoPort;
		this.startTime = startTime;
	}

	public String getHostPort() {
		return hostName + ":" + infoPort;
	}

	public byte[] getBytes() {
		return (hostName + ":" + infoPort + ":" + startTime + ":" + deadTime).getBytes();
	}

	public boolean equals(Object o) {
		if (o instanceof HostInfo) {
			HostInfo ot = (HostInfo) o;
			if (hostName.equals(ot.hostName) && this.infoPort == ot.infoPort) {
				return true;
			}
		}
		return false;
	}

	public String toString() {
		if (this.deadTime == 0) {
			return hostName + ":" + infoPort + "  " + DImportConstant.sdf.format(new Date(startTime));
		} else {
			return hostName + ":" + infoPort + "  " + DImportConstant.sdf.format(new Date(deadTime));
		}
	}

	public static HostInfo parseFrom(byte[] data) {
		if (data == null)
			return null;
		return new HostInfo(new String(data));
	}
}
