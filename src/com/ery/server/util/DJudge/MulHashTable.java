package com.ery.server.util.DJudge;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import com.ery.base.support.utils.Bytes;
import com.ery.server.util.ToolUtil;
import com.ery.server.util.DJudge.HashTable.FileHash;
import com.ery.server.util.DJudge.HashTable.HashNode;
import com.googlecode.aviator.AviatorEvaluator;
import com.googlecode.aviator.Expression;

public class MulHashTable {
	HashMap<Partition, JudgeHash> tables = new HashMap<Partition, JudgeHash>();
	HashMap<Partition, Partition> tableTypes = new HashMap<Partition, Partition>();
	long maxMemorySize;
	int maxListSize;
	String bufDir;
	String fileNameRule;
	int mulSize;
	String[] keys;
	String busTimeFiled;
	Expression expr = null;
	int hashSize;
	String partitionRule;

	int type = 2;// 1:分钟 2小时 3天
	int interval = 1;// 间隔

	/**
	 * 
	 * @param keys
	 * @param busTimeFiled
	 * @param partitionRule
	 *            类型：时长
	 * @param hashSize
	 * @param maxMemorySize
	 * @param maxListSize
	 * @param bufDir
	 * @param fileNameRule
	 * @param mulSize
	 */
	public MulHashTable(String[] keys, String busTimeFiled, String partitionRule, int hashSize, long maxMemorySize,
			int maxListSize, String bufDir, String fileNameRule, int mulSize) {
		this.keys = keys;
		this.busTimeFiled = busTimeFiled;
		this.partitionRule = partitionRule;
		this.hashSize = hashSize;
		this.bufDir = bufDir;
		this.fileNameRule = fileNameRule;
		this.mulSize = mulSize;
		this.maxMemorySize = maxMemorySize;
		this.maxListSize = maxListSize;
		if (this.mulSize <= 1)
			this.mulSize = 1;
		else if (this.mulSize >= 10)// 容易超出2G大小
			this.mulSize = 10;
		expr = AviatorEvaluator.compile(fileNameRule, true);
		if (this.partitionRule != null) {
			String tmp[] = this.partitionRule.split(":");
			type = Integer.parseInt(tmp[0]);
			if (type != 1 && type != 2 && type != 3) {
				type = 2;// 0:小时 1:分钟 2小时 3天
				interval = 1;// 间隔
			} else {
				if (tmp.length > 1) {
					interval = Integer.parseInt(tmp[1]);
				} else if (type == 1) {
					interval = 30;// 间隔
				} else if (type == 2 || type == 3) {
					interval = 1;// 间隔
				}
			}
		} else {
			type = 2;// 0:小时 1:分钟 2小时 3天
			interval = 1;// 间隔
		}
	}

	public static class Partition {
		String[] vals;
		long busStartTime;// 业务时间
		long busEndTime;// 业务时间
		long ctime;// 创建时间
		int type = 0;// 对应缓存类型，HashTable类型 1内存 0文件 2写入文件中

		public Partition(String[] vals, long stime, long etime) {
			this.vals = vals;
			this.busStartTime = stime;
			this.busEndTime = etime;
			this.ctime = System.currentTimeMillis();
		}

		public int hashCode() {
			int res = 0;
			for (String v : vals) {
				res = v.hashCode() | res;
			}
			return (int) (res + busStartTime);
		}

		public boolean equals(Object other) {
			if (other instanceof Partition) {
				Partition p = (Partition) other;
				if ((p.vals == null && this.vals != null) || (p.vals != null && this.vals == null))
					return false;
				if (p.vals.length != this.vals.length)
					return false;
				for (int i = 0; i < this.vals.length; i++) {
					if (!this.vals[i].equals(p.vals[i])) {
						return false;
					}
				}
				return this.busStartTime == p.busStartTime && this.busEndTime == p.busEndTime;
			} else {
				return false;
			}
		}
	}

	public Partition getPartition(String vals[], long busTime) {
		long stime = 0, etime = 0;
		switch (this.type) {
		case 1: {
			stime = (busTime) / (interval * 60000) * interval * 60000;
			etime = stime + interval * 60000;
			break;
		}
		case 2:
			stime = (busTime) / (interval * 3600000) * interval * 3600000;
			etime = stime + interval * 3600000;
			break;
		case 3:
			stime = (busTime) / (interval * 86400000) * interval * 86400000;
			etime = stime + interval * 86400000;
			break;
		}
		return new Partition(vals, stime, etime);
	}

	public synchronized Partition getMinTimeMemPartition() {
		Partition par = null;
		for (Partition p : this.tables.keySet()) {
			if (this.tables.get(p) instanceof HashTable) {
				if (par == null || p.busStartTime < par.busStartTime) {
					par = p;
				}
			}
		}
		return par;
	}

	public int getMemPartitionSize() {
		int res = 0;
		for (Partition p : this.tables.keySet()) {
			if (this.tables.get(p) instanceof HashTable) {
				res++;
			}
		}
		return res;
	}

	public synchronized Partition getMaxTimeMemPartition() {
		Partition par = null;
		for (Partition p : this.tables.keySet()) {
			if (this.tables.get(p) instanceof HashTable) {
				if (par == null || p.busStartTime > par.busStartTime) {
					par = p;
				}
			}
		}
		return par;
	}

	public JudgeHash getHashTable(Partition par, int hashSize) {
		synchronized (tables) {
			JudgeHash hash = this.tables.get(par);
			String fileName = null;
			if (hash == null) {
				int oldHs = hashSize;
				long memSize = (long) ((long) hashSize * HashTable.NodeSize * 1.4 + HashTable.HashHeadSize + hashSize *
						HashTable.jdkByte);
				long reserve = reserveMem(memSize);
				Partition max = getMaxTimeMemPartition();// 最大时间，最近时间
				if (reserve < memSize && max != null) {
					hashSize = ((HashTable) this.tables.get(max)).nodes.length;
					memSize = (long) ((long) hashSize * HashTable.NodeSize * 1.4 + HashTable.HashHeadSize + hashSize *
							HashTable.jdkByte);
				}
				reserve = reserveMem(memSize);
				if (reserve < memSize) {// 只能直接使用文件
					hashSize = oldHs;
					if (mulSize > 1) {
						memSize = (long) ((long) hashSize * Bytes.SIZEOF_INT * 4 + 0.2 * hashSize *
								(Bytes.SIZEOF_INT * 3 * mulSize + Bytes.SIZEOF_INT) + Bytes.SIZEOF_INT * 3);
						while (memSize > Integer.MAX_VALUE) {
							hashSize = (int) (hashSize / 1.1);
							memSize = (long) ((long) hashSize * Bytes.SIZEOF_INT * 4 + 0.2 * hashSize *
									(Bytes.SIZEOF_INT * 3 * mulSize + Bytes.SIZEOF_INT) + Bytes.SIZEOF_INT * 3);
						}
					} else {
						memSize = (long) ((long) hashSize * HashTable.NodeSize * 1.4 + Bytes.SIZEOF_INT * 3);
						while (memSize > Integer.MAX_VALUE) {
							hashSize = (int) (hashSize / 1.1);
							memSize = (long) ((long) hashSize * HashTable.NodeSize * 1.4 + Bytes.SIZEOF_INT * 3);
						}
					}
					fileName = getFileName(par);
					int reTry = 0;
					while (reTry++ < 3) {
						try {
							hash = new FileHash(fileName, hashSize, mulSize);
							par.type = 0;
							this.tables.put(par, hash);

							break;
						} catch (IOException e) {
							if (e.getCause() instanceof OutOfMemoryError) {
								System.out.println("OutOfMemoryError");
							}
							if (reTry == 2) {
								return null;
							}
						}
					}
				} else {
					hash = new HashTable(hashSize);
					par.type = 1;
					this.tables.put(par, hash);
				}
				tableTypes.put(par, par);
				System.out.println("新增hashTable  type=" + ((par.type == 1) ? "内存" : "文件 " + fileName));
			}
			return hash;
		}
	}

	public JudgeHash getHashTable(Partition par) {
		synchronized (tables) {
			JudgeHash hash = this.tables.get(par);
			return hash;
		}
	}

	public Partition getPartition(Partition par) {
		synchronized (tableTypes) {
			return tableTypes.get(par);
		}
	}

	public long checkMem() {
		long memSize = 0;
		for (Entry<Partition, JudgeHash> en : this.tables.entrySet()) {
			JudgeHash h = en.getValue();
			if (h instanceof HashTable) {
				memSize += ((HashTable) h).heapSize;
			}
		}
		return memSize;
	}

	// 反正保留大小的内存剩余大小，不足则释放内存表
	public long reserveMem(long reserve) {
		if (this.maxMemorySize > reserve) {
			long memSize = checkMem();
			if (this.maxMemorySize - memSize < reserve) {
				Partition lastPartition = getMinTimeMemPartition();
				if (lastPartition == null)
					return 0;
				JudgeHash hash = this.tables.get(lastPartition);
				HashTable ht = (HashTable) hash;
				synchronized (lastPartition) {
					String file = getFileName(lastPartition);
					int _mulSize = mulSize;
					boolean res = false;
					synchronized (lastPartition) {
						lastPartition.type = 2;// 写入文件中
						while (true) {
							try {
								ht.writeToFile(file, _mulSize);
								res = true;
								break;
							} catch (IOException e) {
								_mulSize--;
								if (_mulSize < 1)
									break;
							}
						}

						if (res) {
							HashTable.FileHash fh = null;
							int reTry = 0;
							while (reTry++ < 3) {
								try {
									fh = new HashTable.FileHash(file);
									break;
								} catch (IOException e) {
								}
							}
							if (fh != null) {
								synchronized (this.tables) {
									lastPartition.type = 1;
									this.tables.put(lastPartition, fh);
									tableTypes.put(lastPartition, lastPartition);
								}
							} else {
								synchronized (this.tables) {
									this.tables.remove(lastPartition);
									tableTypes.remove(lastPartition);
								}
							}
						} else {
							return this.maxMemorySize - memSize;
						}
					}
				}
			}
			memSize = this.maxMemorySize - checkMem();
			while (memSize < reserve) {
				memSize = this.reserveMem(reserve);
			}
			return memSize;
		} else {
			return 0;
		}
	}

	public synchronized Partition getMinTimePartition() {
		Partition par = null;
		for (Partition p : this.tables.keySet()) {
			if (par == null || p.busStartTime < par.busStartTime) {
				par = p;
			}
		}
		return par;
	}

	public synchronized Partition getMaxTimePartition() {
		Partition par = null;
		for (Partition p : this.tables.keySet()) {
			if (par == null || p.busStartTime > par.busStartTime) {
				par = p;
			}
		}
		return par;
	}

	public void checkListSize() {
		if (this.tables.size() > maxListSize) {
			Partition minp = getMinTimePartition();
			if (minp != null) {
				synchronized (tables) {
					JudgeHash h = tables.get(minp);
					synchronized (h) {
						h.close();
					}
					tables.remove(minp);
				}
			}
		}
	}

	public String getFileName(Partition par) {
		Map<String, Object> calcEnv = new HashMap<String, Object>();
		if (this.keys != null) {
			for (int i = 0; i < this.keys.length; i++) {
				calcEnv.put(this.keys[i], par.vals[i]);
			}
		}
		calcEnv.put("time", par.busStartTime);
		String fileName = this.expr.execute(calcEnv).toString();
		if (bufDir != null)
			return this.bufDir + File.separatorChar + fileName;
		else
			return fileName;
	}

	public boolean addNode(Partition par, HashNode node, int hashSize) throws IOException {
		JudgeHash hash = null;
		Partition op = getPartition(par);
		if (op == null) {
			op = par;
		}
		boolean res = false;
		synchronized (op) {
			hash = getHashTable(par, hashSize);
			synchronized (hash) {
				res = hash.addNode(node);
			}
		}
		return res;
	}

	public boolean addNode(Partition par, HashNode node) throws IOException {
		return addNode(par, node, this.hashSize);
	}

	public boolean contains(Partition par, HashNode node) throws IOException {
		JudgeHash hash = getHashTable(par);
		if (hash == null) {
			return false;
		}
		return hash.contains(node);
	}

	public void close(Partition par) {
		JudgeHash hash = getHashTable(par);
		if (hash != null) {
			hash.close();
			synchronized (tableTypes) {
				synchronized (tables) {
					this.tables.remove(par);
					tableTypes.remove(par);
				}
			}
		}
	}

	public boolean addNode(JudgeHash hash, HashNode node) throws IOException {
		boolean res = false;
		synchronized (hash) {
			res = hash.addNode(node);
		}
		return res;
	}

	public boolean contains(JudgeHash hash, HashNode node) throws IOException {
		return hash.contains(node);
	}

	public void close(JudgeHash hash) {
		hash.close();
	}

	public static void main(String[] args) throws IOException {
		if (args.length > 0 && args[0].equals("-h")) {
			System.err.println("buildFile  build test data File");
			System.err.println("testFile    test   FileHash inited");
			System.err.println("fileTest/realTest xm/xg  2:1 3777777 read or real build data to test Xm or Xg Mem  ");
			return;
		}

		try {
			if (args.length > 1)
				Thread.currentThread().sleep(5000);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}

		java.text.SimpleDateFormat sdf = new SimpleDateFormat("yyyyMM");
		System.err.println("freeMemory:" + Runtime.getRuntime().freeMemory());
		System.err.println("totalMemory:" + Runtime.getRuntime().totalMemory());
		long useMem = (Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory());
		System.err.println("use:" + useMem);
		// 写1亿条测试数据
		String[] partitionKeys = new String[] { "latn", "month" };
		String[] hashKeys = new String[] { "nbr", "stime", "etime" };
		int[] partitionKeyIndex = new int[] { 0, 1 };
		int[] hashKeyIndex = new int[] { 2, 3, 4 };
		String testDataFile = "data.dat";
		File file = new File(testDataFile);
		if (args.length > 0) {
			if (args[0].equals("buildFile")) {
				file.delete();
				file.createNewFile();
				FileOutputStream out = new java.io.FileOutputStream(file);
				long time = System.nanoTime();
				String data[] = new String[5];// latn month nbr stime etime
				long now = System.currentTimeMillis();
				for (int i = 0; i < 100000000; i++) {
					data[0] = (815 + (int) (Integer.MAX_VALUE * Math.random()) % 21) + "";
					if (i % 20000 == 0) {
						now += 1000;
					}
					data[2] = ("13" + (int) (Integer.MAX_VALUE * Math.random()) % 1000000000) + "";
					long busTime = (now - 3600000 + (int) (Integer.MAX_VALUE * Math.random()) % 10000);
					data[3] = busTime + "";// 保障流水线产生数据
					data[4] = data[3] + (int) (Integer.MAX_VALUE * Math.random()) % 111;
					data[1] = sdf.format(new Date(busTime));
					out.write(data[0].getBytes());
					out.write(",".getBytes());
					out.write(data[1].getBytes());
					out.write(",".getBytes());
					out.write(data[2].getBytes());
					out.write(",".getBytes());
					out.write(data[3].getBytes());
					out.write(",".getBytes());
					out.write(data[4].getBytes());
					out.write("\n".getBytes());
					if (i % 1000000 == 0) {
						System.err.println(" 写入:" + i + "   用时：" + (System.nanoTime() - time) / 1000000 + "ms   ");
					}
				}
				out.flush();
				out.close();
				System.err.println(" 写入:100000000     用时：" + (System.nanoTime() - time) / 1000000 + "ms   ");
			} else if (args[0].equals("testFile")) {
				FileHash ft = new FileHash("testFileHash.dat", 7777, 5);
				ft.close();
				FileHash fh = new FileHash("testFileHash.dat");
				System.err.println("hashSize=" + fh.hashSize);
				System.err.println("hashSize=" + fh.mulSize);
				System.err.println("hashSize=" + fh.repSize);
			}
		}
		// 读取数据做过滤性能测试

		// 512m内存
		// 1G内存
		// 2G内存
		// 4G内存
		// 8G内存
		long time = System.nanoTime();
		if (args.length > 0) {
			long maxMemSize = 512 * 1024 * 1024;
			if (args.length > 1) {
				Long num = ToolUtil.toLong(args[1]);
				if (num != null && num > 0) {
					char c = args[1].charAt(args[1].length() - 1);
					if (c == 'm' || c == 'M') {
						maxMemSize = num * 1024 * 1024;
					} else if (c == 'g' || c == 'G') {
						maxMemSize = num * 1024 * 1024 * 1024;
					}
				}
			}
			System.out.println("限制使用内存大小" + maxMemSize + "进行性能测试");
			int hashSize = 3777777;
			String partitionRule = "2:1";
			if (args.length > 2) {
				partitionRule = args[2];
				if (args.length > 3)
					hashSize = Integer.parseInt(args[3]);
			}
			int rowCount = 0, repsize = 0;
			if (args[0].equals("fileTest")) {// 从文件读取测试
				MulHashTable mht512m = new MulHashTable(partitionKeys, "stime", partitionRule, hashSize, maxMemSize, 1,
						".", "month+'_'+latn+'_'+time+'.idx'", 5);
				BufferedReader reader = new BufferedReader(new FileReader(file));
				String line = null;
				while ((line = reader.readLine()) != null) {
					String[] fileds = line.split(",");
					StringBuffer sb = new StringBuffer(64);
					for (int index : hashKeyIndex) {
						sb.append(fileds[index]);
						sb.append(',');
					}
					long busTime = Long.parseLong(fileds[3]);
					fileds[0] = (Integer.parseInt(fileds[0]) + 815) + "";
					fileds[1] = sdf.format(new Date(busTime));

					String vals[] = new String[partitionKeyIndex.length];
					for (int i = 0; i < partitionKeyIndex.length; i++) {
						vals[i] = fileds[i];
					}
					byte[] bytes = sb.toString().getBytes();
					Partition par = mht512m.getPartition(vals, busTime);
					int h31 = HashTable.SPHash(bytes, 31), h17 = HashTable.SPHash(bytes, 17), h19 = HashTable.SPHash(
							bytes, 19);
					HashNode node = new HashNode(h31, h17, h19);
					if (!mht512m.addNode(par, node)) {
						repsize++;
						System.err.println("重复记录：" + line + " rowCount=" + rowCount);
					}
					if (rowCount % 200000 == 0) {
						System.err.println("判断:" + rowCount + "   用时：" + (System.nanoTime() - time) / 1000000 +
								"ms  重复数： " + repsize);
					}
					rowCount++;
				}
				System.err.println("判断:" + rowCount + "   用时：" + (System.nanoTime() - time) / 1000000 + "ms  重复数： " +
						repsize);
				reader.close();
				long _useMem = (Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory());
				System.err.println("ouse:" + useMem);
				System.err.println("cuse:" + _useMem);
				System.err.println("add use:" + (_useMem - useMem));
			} else if (args[0].equals("realTest")) {// 实时测试
				MulHashTable mht512m = new MulHashTable(partitionKeys, "stime", partitionRule, hashSize, maxMemSize, 1,
						".", "month+'_'+latn+'_'+time+'.idx'", 5);
				time = System.nanoTime();
				String data[] = new String[5];// latn month nbr stime etime
				long now = System.currentTimeMillis();
				for (int i = 0; i < 100000000; i++) {
					data[0] = (815 + (int) (Integer.MAX_VALUE * Math.random()) % 21) + "";
					if (i % 20000 == 0) {
						now += 1000;
					}
					data[2] = ("13" + (int) (Integer.MAX_VALUE * Math.random()) % 1000000000) + "";
					long busTime = (now - 3600000 + (int) (Integer.MAX_VALUE * Math.random()) % 10000);
					data[3] = busTime + "";// 保障流水线产生数据
					data[4] = data[3] + (int) (Integer.MAX_VALUE * Math.random()) % 111;
					data[1] = sdf.format(new Date(busTime));

					StringBuffer sb = new StringBuffer(64);
					for (int index : hashKeyIndex) {
						sb.append(data[index]);
						sb.append(',');
					}

					String vals[] = new String[partitionKeyIndex.length];
					for (int j = 0; j < partitionKeyIndex.length; j++) {
						vals[j] = data[j];
					}
					byte[] bytes = sb.toString().getBytes();
					Partition par = mht512m.getPartition(vals, busTime);
					int h31 = HashTable.SPHash(bytes, 31), h17 = HashTable.SPHash(bytes, 17), h19 = HashTable.SPHash(
							bytes, 19);
					HashNode node = new HashNode(h31, h17, h19);
					if (!mht512m.addNode(par, node)) {
						repsize++;
						System.err.println("重复记录：" + data + " rowCount=" + rowCount);
					}
					if (i % 1000000 == 0) {
						System.err.println(" 写入:" + i + "   用时：" + (System.nanoTime() - time) / 1000000 + "ms ");
					}
				}
				System.err.println(" 写入:100000000     用时：" + (System.nanoTime() - time) / 1000000 + "ms   ");
			}
		}
	}
}
