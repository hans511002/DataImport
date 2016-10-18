package com.ery.server.util.DJudge;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.BufferOverflowException;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;

import com.ery.server.util.Bytes;

public class HashTable extends HeapSize implements JudgeHash {

	HashNode[] nodes = null;
	int onceSize = 0;
	int duplicateSize = 0;
	long heapSize = 0;
	boolean autoExpand = false;

	public static final int HashHeadSize = jdkByte + jdkByte + RoundJdkByte(Integer.SIZE * 2 / Byte.SIZE)
			+ RoundJdkByte(Long.SIZE / Byte.SIZE) + RoundJdkByte(Byte.SIZE / Byte.SIZE);
	public static final int NodeSize = jdkByte + RoundJdkByte(Integer.SIZE * 3 / Byte.SIZE) + jdkByte;
	public static byte NullByteNode[] = new byte[Bytes.SIZEOF_INT * 4];
	static {
		for (int i = 0; i < NullByteNode.length; i++) {
			NullByteNode[i] = 0;
		}
	}

	public static int hash(byte[] bytes, int type) {
		switch (type) {
		case 0:
			return SPHash(bytes, 31);
		case 1:
			return SPHash(bytes, 17);
		case 2:
			return SPHash(bytes, 19);
		default:
			return SPHash(bytes, 31);
		}
	}

	public static int SPHash(byte[] bytes, int seed) {
		int hash = 0;
		int len = bytes.length;
		for (int i = 0; i < len; i++) {
			hash = (hash * seed) + bytes[i];
		}
		return hash;
	}

	public static class HashNode {
		public int HashCode1 = 0;
		public int HashCode2 = 0;
		public int HashCode3 = 0;
		public HashNode next = null;

		public HashNode(int h1, int h2, int h3) {
			this.HashCode1 = h1;
			this.HashCode2 = h2;
			this.HashCode3 = h3;
		}

		public HashNode() {
		}

		public boolean equals(Object Other) {
			if (Other instanceof HashNode) {
				HashNode _node = (HashNode) Other;
				return (HashCode1 == _node.HashCode1 && HashCode2 == _node.HashCode2 && HashCode3 == _node.HashCode3);
			} else {
				return false;
			}
		}

		public boolean equals(HashNode _node) {
			return (HashCode1 == _node.HashCode1 && HashCode2 == _node.HashCode2 && HashCode3 == _node.HashCode3);
		}

		public int hashCode() {
			return HashCode1;// | HashCode2 | HashCode3;
		}
	}

	public HashTable(int hashSize) {
		this(hashSize, false);
	}

	public HashTable(int hashSize, boolean autoExpand) {
		if (hashSize > 11) {
			nodes = new HashNode[hashSize];
		} else {
			nodes = new HashNode[11];
		}
		this.heapSize = fullSizeOf(this);
		if (this.heapSize == 0) {
			this.heapSize = HashHeadSize + nodes.length * jdkByte;
		}
	}

	public boolean contains(HashNode node) {
		int index = node.HashCode1 % this.nodes.length;
		index = index > 0 ? index : -index;
		if (nodes[index] == null) {
			return false;
		} else {
			HashNode _node = nodes[index];
			while (_node != null) {
				if (_node.equals(node)) {
					return true;
				} else {
					_node = _node.next;
				}
			}
			return false;
		}
	}

	public boolean reSize(int size) {
		if (size < this.nodes.length) {
			return false;
		}
		HashNode[] oldNodes = this.nodes;
		nodes = new HashNode[size];
		onceSize = 0;
		this.heapSize = fullSizeOf(this);
		if (this.heapSize == 0) {
			this.heapSize = HashHeadSize + nodes.length * jdkByte;
		}
		for (HashNode node : oldNodes) {
			if (node != null)
				this.addNode(node);
		}
		return true;
	}

	public boolean reSize(float coefficient) {
		if (coefficient < 0.1) {
			coefficient = 0.2f;
		}
		return reSize(this.nodes.length + (int) (this.nodes.length * coefficient));
	}

	public void clear() {
		this.nodes = new HashNode[11];
		onceSize = 0;
		this.heapSize = sizeOf(this);
		autoExpand = true;
	}

	public boolean setAutoExpand(boolean autoExpand) {
		boolean a = this.autoExpand;
		this.autoExpand = autoExpand;
		return a;
	}

	@Override
	public void close() {
		clear();
	}

	/**
	 * 添加节点，
	 * 
	 * @param node
	 * @return 是否添加成功
	 */
	@Override
	public boolean addNode(HashNode node) {
		if (autoExpand) {
			if ((onceSize + duplicateSize) > this.nodes.length * 1.1 && onceSize / this.nodes.length > 0.75) {
				this.reSize(0.2f);
			}
		}
		int index = node.HashCode1 % nodes.length;
		index = index > 0 ? index : -index;
		if (nodes[index] == null) {
			nodes[index] = node;
			onceSize++;
			heapSize += NodeSize;
			return true;
		} else {
			HashNode _node = nodes[index];
			HashNode pnode = null;
			while (_node != null) {
				if (_node.equals(node)) {
					return false;
				} else {
					pnode = _node;
					_node = _node.next;
				}
			}
			pnode.next = node;
			duplicateSize++;
			heapSize += NodeSize;
			return true;
		}
	}

	public String analysis() {
		StringBuffer sb = new StringBuffer();
		int[] dept = new int[100];
		int maxLevel = 0;
		for (int i = 0; i < this.nodes.length; i++) {
			int depth = 0;
			if (this.nodes[i] != null) {
				dept[0]++;
				HashNode pnode = this.nodes[i];
				while (pnode.next != null) {
					depth++;
					dept[depth]++;
					if (maxLevel < depth)
						maxLevel = depth;
					pnode = pnode.next;
				}
			}
		}
		sb.append("索引重复数：" + this.duplicateSize + "  占比：" + (double) this.duplicateSize / (this.duplicateSize + this.onceSize) + " 内存占用："
				+ this.heapSize + "\n");
		for (int i = 0; i < maxLevel; i++) {
			sb.append("level" + i + ":" + dept[i] + " 占比：" + ((double) dept[i] / (this.duplicateSize + this.onceSize)) + "\n");
		}
		return sb.toString();
	}

	// 需要控制文件大小小于2G
	public void writeToFile(String file, int mulSize) throws IOException {
		File fl = new File(file);
		fl.deleteOnExit();
		if (!fl.createNewFile()) {
			File fp = fl.getParentFile();
			if (fp != null && !fp.exists()) {
				if (fp.mkdirs()) {
					throw new IOException("创建文件目录失败:" + fp.getAbsolutePath());
				}
			}
			if (!fl.createNewFile())
				throw new IOException("创建文件失败:" + fl.getAbsolutePath());
		}
		if (fl.getFreeSpace() < this.heapSize) {
			throw new IOException("磁盘空间不足,需要：" + this.heapSize + " 剩余：" + fl.getFreeSpace());
		}
		int head = FileHash.head;
		mulSize = mulSize > 1 ? mulSize : 1;
		int mulNodeSize = Bytes.SIZEOF_INT * 3 * mulSize + Bytes.SIZEOF_INT;
		int fileHeadSize = head + HashTable.NullByteNode.length * this.nodes.length;
		int fileSize = fileHeadSize + this.duplicateSize * mulNodeSize;// 一定比多个组合存在大
		if (fileSize * 1.3 > Integer.MAX_VALUE) {
			fileSize = Integer.MAX_VALUE;
		} else {
			fileSize = (int) (fileSize * 1.3);
		}
		System.out.println("落地HashTable到文件 file=" + fl.getCanonicalPath() + "  mapFileSize=" + fileSize + " hashSize=" + this.nodes.length
				+ "  duplicateSize=" + this.duplicateSize);
		// 为了以可读可写的方式打开文件，这里使用RandomAccessFile来创建文件。
		RandomAccessFile rf = new RandomAccessFile(file, "rw");
		FileChannel fc = rf.getChannel();
		// 注意，文件通道的可读可写要建立在文件流本身可读写的基础之上
		MappedByteBuffer out = fc.map(FileChannel.MapMode.READ_WRITE, 0, fileSize);
		// for (int i = 0; i < fileSize; i++) {
		// out.put((byte) 0);
		// }
		out.position(0);
		out.putInt(this.nodes.length);
		out.putInt(mulSize);
		int repSize = 0;
		out.putInt(repSize);
		for (int i = 0; i < this.nodes.length; i++) {
			HashNode node = this.nodes[i];
			out.position(head + i * HashTable.NullByteNode.length);
			if (node == null) {
				// out.put(HashTable.NullByteNode);
			} else {
				out.putInt(node.HashCode1);
				out.putInt(node.HashCode2);
				out.putInt(node.HashCode3);
				if (node.next != null) {
					out.putInt(this.nodes.length + repSize);
				} else {
					out.putInt(0);
				}
				if (mulSize <= 1) {
					while (node.next != null) {
						node = node.next;
						long filePos = (fileHeadSize + HashTable.NullByteNode.length * repSize);
						if (filePos + NodeSize > Integer.MAX_VALUE) {
							throw new IOException("超出2G文件限制，无法正常落地到文件");
						}
						out.position((int) filePos);
						out.putInt(node.HashCode1);
						out.putInt(node.HashCode2);
						out.putInt(node.HashCode3);
						repSize++;
						if (node.next != null) {
							out.putInt(this.nodes.length + repSize);
						} else {
							out.putInt(0);
						}
					}
				} else {
					try {
						while (true) {
							long filePos = (fileHeadSize + mulNodeSize * repSize);
							if (filePos + NodeSize > Integer.MAX_VALUE) {
								throw new IOException("超出2G文件限制，无法正常落地到文件");
							}
							out.position((int) filePos);
							int[] nodeData = new int[mulSize * 3];
							int index = 0;
							while (node.next != null) {
								node = node.next;
								nodeData[index * 3] = node.HashCode1;
								nodeData[index * 3 + 1] = node.HashCode2;
								nodeData[index * 3 + 2] = node.HashCode3;
								index++;
								if (index >= mulSize)
									break;
							}
							if (index < mulSize) {
								for (int j = index * 3; j < nodeData.length; j++) {
									nodeData[j] = 0;
								}
								out.putInt(0);
							} else {
								out.putInt(this.nodes.length + repSize);
							}
							repSize++;
							for (int j = 0; j < nodeData.length; j++) {
								out.putInt(nodeData[j]);
							}
							if (node == null || node.next == null)
								break;
						}

					} catch (BufferOverflowException e) {
						System.out.println("out.position()=" + out.position());
						throw e;
					}
				}
			}
		}
		out.position(Bytes.SIZEOF_INT * 2);
		out.putInt(repSize);
		out.force();
		out.clear();
		fc.close();
		rf.close();

		//
		//
		// rf.write(Bytes.toBytes(this.nodes.length));// 写HashSize
		// rf.write(Bytes.toBytes(mulSize));// 单一节点数据大小
		//
		// for (int i = 0; i < this.nodes.length; i++) {
		// HashNode node = this.nodes[i];
		// if (node == null) {
		// fos.write(HashTable.NullByteNode);
		// } else {
		// HashNode pnode = node;
		// while (pnode.next != null) {
		// node = pnode.next;
		// fos.write(Bytes.toBytes(pnode.HashCode1));
		// fos.write(Bytes.toBytes(pnode.HashCode2));
		// fos.write(Bytes.toBytes(pnode.HashCode3));
		// pnode = node;
		// }
		// }
		// }
		// for (int i = 0; i < 10; i++) {
		// // 写入基本类型double数据
		// rf.writeDouble(i * 1.414);
		// }
		// rf.close();
		// rf = new RandomAccessFile("rtest.dat", "rw");
		// // 直接将文件指针移到第5个double数据后面
		// rf.seek(5 * 8);

	}

	public static class FileHashNode {
		public int HashCode1 = 0;
		public int HashCode2 = 0;
		public int HashCode3 = 0;
		public int next = 0;

		public FileHashNode() {
		}

		public void clear(boolean all) {
			if (all) {
				this.HashCode1 = HashCode2 = HashCode3 = next = 0;
			} else {
				this.HashCode1 = HashCode2 = HashCode3 = 0;
			}
		}

		public boolean equals(Object Other) {
			if (Other instanceof HashNode) {
				HashNode _node = (HashNode) Other;
				return (HashCode1 == _node.HashCode1 && HashCode2 == _node.HashCode2 && HashCode3 == _node.HashCode3);
			} else {
				return false;
			}
		}

		public boolean equals(HashNode _node) {
			return (HashCode1 == _node.HashCode1 && HashCode2 == _node.HashCode2 && HashCode3 == _node.HashCode3);
		}

		public int hashCode() {
			return HashCode1;// | HashCode2 | HashCode3;
		}
	}

	public static class FileHash implements JudgeHash {
		public static int head = Bytes.SIZEOF_INT * 3;
		int hashSize = 0;
		int fileHeadSize = 0;
		int mulSize = 1;
		int repSize;

		int nodeSize = 0;
		int mapFileSize = 0;
		File file;
		RandomAccessFile rf = null;
		FileChannel fc = null;
		MappedByteBuffer out = null;

		int calcRepSize(int fileSize) {
			int rsize = fileSize - fileHeadSize;
			if (mulSize <= 1) {
				return rsize / HashTable.NullByteNode.length;
			} else {
				return rsize / nodeSize;
			}
		}

		public FileHash(String fileName) throws IOException {
			this.file = new File(fileName);
			reOpen();
		}

		public FileHash(String fileName, int hashSize, int mulSize) throws IOException {
			this.file = new File(fileName);
			if (!this.file.exists())
				this.file.createNewFile();
			this.hashSize = hashSize;
			this.mulSize = mulSize;
			rf = new RandomAccessFile(this.file, "rw");
			fc = rf.getChannel();
			if (mulSize > 1) {
				nodeSize = Bytes.SIZEOF_INT * 3 * mulSize + Bytes.SIZEOF_INT;
			} else {
				nodeSize = HashTable.NullByteNode.length;
			}
			fileHeadSize = (int) (head + HashTable.NullByteNode.length * hashSize);
			if (mulSize > 1) {
				if (fileHeadSize + hashSize * 0.6 * nodeSize / mulSize < Integer.MAX_VALUE)
					mapFileSize = (int) (fileHeadSize + hashSize * 0.5 * nodeSize);
				else
					mapFileSize = Integer.MAX_VALUE;
			} else {
				if (fileHeadSize + hashSize * 0.5 * nodeSize < Integer.MAX_VALUE)
					mapFileSize = (int) (fileHeadSize + hashSize * 0.5 * nodeSize);
				else
					mapFileSize = Integer.MAX_VALUE;
			}
			try {
				out = fc.map(FileChannel.MapMode.READ_WRITE, 0, mapFileSize);
				out.position(0);
				out.putInt(hashSize);
				out.putInt(mulSize);
				out.putInt(0);
				out.position(mapFileSize - 1);
				out.put((byte) 0);
			} catch (IOException e) {
				if (e.getCause() instanceof OutOfMemoryError) {
					System.out.println("OutOfMemoryError  use RandomAccessFile");
					out = null;
					rf.seek(0);
					rf.writeInt(hashSize);
					rf.writeInt(mulSize);
					rf.writeInt(0);
					rf.seek(mapFileSize - 1);
					rf.write((byte) 0);
				} else {
					throw e;
				}
			}
		}

		int calcPostion(int index) {
			if (mulSize > 1) {
				if (index <= this.hashSize) {
					return head + HashTable.NullByteNode.length * index;
				} else {
					return head + HashTable.NullByteNode.length * hashSize + (index - hashSize) * nodeSize;
				}
			} else {
				return head + HashTable.NullByteNode.length * index;
			}
		}

		public boolean contains(HashNode node) throws IOException {
			int index = (int) (node.HashCode1 % this.hashSize);
			index = index > 0 ? index : -index;
			FileHashNode _node = new FileHashNode();
			if (out == null) {
				rf.seek(calcPostion(index));
				_node.HashCode1 = rf.readInt();// out.getInt();
				if (_node.HashCode1 == 0)
					return false;
				_node.HashCode2 = rf.readInt();// out.getInt();
				_node.HashCode3 = rf.readInt();// out.getInt();
				if (_node.equals(node)) {
					return true;
				}
				_node.next = rf.readInt();// out.getInt();
				if (_node.next == 0) {
					return false;
				}
				if (mulSize <= 1) {
					while (_node.next != 0) {
						rf.seek(calcPostion(_node.next));
						_node.clear(true);
						_node.HashCode1 = rf.readInt();// out.getInt();
						if (_node.HashCode1 == 0)
							return false;
						_node.HashCode2 = rf.readInt();// out.getInt();
						_node.HashCode3 = rf.readInt();// out.getInt();
						if (_node.equals(node)) {
							return true;
						}
						_node.next = rf.readInt();// out.getInt();
						if (_node.next == 0) {
							return false;
						}
					}
				} else {
					while (_node.next != 0) {
						rf.seek(calcPostion(_node.next));
						_node.next = out.getInt();
						for (int i = 0; i < this.mulSize; i++) {
							_node.clear(false);// 保留next
							_node.HashCode1 = rf.readInt();// out.getInt();
							if (_node.HashCode1 == 0)
								return false;
							_node.HashCode2 = rf.readInt();// out.getInt();
							_node.HashCode3 = rf.readInt();// out.getInt();
							if (_node.equals(node)) {
								return true;
							}
						}
						if (_node.next == 0) {
							return false;
						}
					}
				}
			} else {
				out.position(calcPostion(index));
				_node.HashCode1 = out.getInt();
				if (_node.HashCode1 == 0)
					return false;
				_node.HashCode2 = out.getInt();
				_node.HashCode3 = out.getInt();
				if (_node.equals(node)) {
					return true;
				}
				_node.next = out.getInt();
				if (_node.next == 0) {
					return false;
				}
				if (mulSize <= 1) {
					while (_node.next != 0) {
						out.position(calcPostion(_node.next));
						_node.clear(true);
						_node.HashCode1 = out.getInt();
						if (_node.HashCode1 == 0)
							return false;
						_node.HashCode2 = out.getInt();
						_node.HashCode3 = out.getInt();
						if (_node.equals(node)) {
							return true;
						}
						_node.next = out.getInt();
						if (_node.next == 0) {
							return false;
						}
					}
				} else {
					while (_node.next != 0) {
						out.position(calcPostion(_node.next));
						_node.next = out.getInt();
						for (int i = 0; i < this.mulSize; i++) {
							_node.clear(false);// 保留next
							_node.HashCode1 = out.getInt();
							if (_node.HashCode1 == 0)
								return false;
							_node.HashCode2 = out.getInt();
							_node.HashCode3 = out.getInt();
							if (_node.equals(node)) {
								return true;
							}
						}
						if (_node.next == 0) {
							return false;
						}
					}
				}

			}
			return false;
		}

		synchronized void reMap(int mapFileSize) throws IOException {
			close();
			rf = new RandomAccessFile(this.file, "rw");
			fc = rf.getChannel();
			this.mapFileSize = mapFileSize;
			try {
				out = fc.map(FileChannel.MapMode.READ_WRITE, 0, mapFileSize);
			} catch (IOException e) {
				if (e.getCause() instanceof OutOfMemoryError) {
					System.out.println("OutOfMemoryError  use RandomAccessFile");
					out = null;
				} else {
					throw e;
				}
			}
		}

		public synchronized void close() {
			if (out != null) {
				out.position(Bytes.SIZEOF_INT * 2);
				out.putInt(this.repSize);
				out.force();
			}
			try {
				fc.close();
			} catch (IOException e) {
			}
			try {
				rf.close();
			} catch (IOException e) {
			}
			out = null;
			fc = null;
			rf = null;
		}

		public synchronized void reOpen() throws IOException {
			rf = new RandomAccessFile(this.file, "rw");
			fc = rf.getChannel();
			if (mapFileSize == 0) {
				int fileSize = (int) rf.length();// 文件 真实大小
				if (fileSize * 1.3 > Integer.MAX_VALUE) {
					mapFileSize = Integer.MAX_VALUE;
				} else {
					mapFileSize = (int) (fileSize * 1.3);// 预留30%增长
				}
			}

			try {
				out = fc.map(FileChannel.MapMode.READ_WRITE, 0, mapFileSize);
				out.position(0);
				hashSize = out.getInt();
				mulSize = out.getInt();
				repSize = out.getInt();
			} catch (IOException e) {
				if (e.getCause() instanceof OutOfMemoryError) {
					System.out.println("OutOfMemoryError  use RandomAccessFile");
					out = null;
					rf.seek(0);
					hashSize = rf.readInt();
					mulSize = rf.readInt();
					repSize = rf.readInt();
				} else {
					throw e;
				}
			}
			fileHeadSize = (int) (head + HashTable.NullByteNode.length * hashSize);
			if (mulSize > 1) {
				nodeSize = Bytes.SIZEOF_INT * 3 * mulSize + Bytes.SIZEOF_INT;
			} else {
				nodeSize = HashTable.NullByteNode.length;
			}
		}

		public int getFileSize() {
			return fileHeadSize + nodeSize * repSize;// 文件有效数据大小
		}

		public int getMapFileSize() {
			return this.mapFileSize;// 文件映射大小
		}

		public int getHashSize() {
			return hashSize;
		}

		public int getFileHeadSize() {
			return fileHeadSize;
		}

		public int getMulSize() {
			return mulSize;
		}

		public int getRepSize() {
			return repSize;
		}

		public int getNodeSize() {
			return nodeSize;
		}

		public File getFile() {
			return file;
		}

		/**
		 * 添加节点，
		 * 
		 * @param node
		 * @return 是否添加成功
		 * @throws IOException
		 */
		@Override
		public synchronized boolean addNode(HashNode node) throws IOException {
			int index = (int) (node.HashCode1 % this.hashSize);
			index = index > 0 ? index : -index;
			FileHashNode _node = new FileHashNode();
			int startIndex = calcPostion(index);
			if (out == null) {
				rf.seek(startIndex);
				_node.HashCode1 = rf.readInt();// out.getInt();
				if (_node.HashCode1 == 0) {
					rf.seek(calcPostion(index));
					rf.writeInt(node.HashCode1);
					rf.writeInt(node.HashCode2);
					rf.writeInt(node.HashCode3);
					rf.writeInt(0);
					// out.force();//不需要刷新，只有一个写
					return true;
				}
				_node.HashCode2 = rf.readInt();// out.getInt();
				_node.HashCode3 = rf.readInt();// out.getInt();
				if (_node.equals(node)) {
					return false;// 存在返回
				}
				_node.next = rf.readInt();// out.getInt();
				if (_node.next == 0) {
					// 写文件尾
					long fileEnd = fileHeadSize + (long) nodeSize * repSize;
					_node.next = (int) fileEnd;
					rf.seek(startIndex + Bytes.SIZEOF_INT * 3);
					rf.writeInt(_node.next);
					rf.seek(_node.next);
					repSize++;
					if (mulSize <= 1) {
						rf.writeInt(node.HashCode1);
						rf.writeInt(node.HashCode2);
						rf.writeInt(node.HashCode3);
						rf.writeInt(0);
					} else {
						rf.writeInt(0);
						rf.writeInt(node.HashCode1);
						rf.writeInt(node.HashCode2);
						rf.writeInt(node.HashCode3);
						for (int j = 1; j < mulSize; j++) {
							rf.writeInt(0);
							rf.writeInt(0);
							rf.writeInt(0);
						}
					}
					return true;
				} else {
					if (mulSize <= 1) {
						while (_node.next != 0) {
							startIndex = calcPostion(_node.next);
							assert (startIndex > 0 && startIndex < Integer.MAX_VALUE);
							rf.seek(startIndex);
							_node.clear(true);
							_node.HashCode1 = rf.readInt();// out.getInt();
							_node.HashCode2 = rf.readInt();// out.getInt();
							_node.HashCode3 = rf.readInt();// out.getInt();
							if (_node.equals(node)) {
								return false;// 存在返回
							}
							_node.next = rf.readInt();// out.getInt();
						}
						// 写文件尾
						long fileEnd = fileHeadSize + (long) nodeSize * repSize;
						_node.next = this.hashSize + this.repSize;// (int) fileEnd;
						rf.seek(startIndex + Bytes.SIZEOF_INT * 3);
						rf.writeInt(_node.next);
						rf.seek((int) fileEnd);
						rf.writeInt(node.HashCode1);
						rf.writeInt(node.HashCode2);
						rf.writeInt(node.HashCode3);
						rf.writeInt(0);
						repSize++;
						return true;
					} else {
						while (_node.next != 0) {
							startIndex = calcPostion(_node.next);
							assert (startIndex > 0 && startIndex < Integer.MAX_VALUE);
							rf.seek(startIndex);
							_node.next = out.getInt();
							for (int i = 0; i < this.mulSize; i++) {
								_node.clear(false);// 保留next
								_node.HashCode1 = rf.readInt();// out.getInt();
								_node.HashCode2 = rf.readInt();// out.getInt();
								_node.HashCode3 = rf.readInt();// out.getInt();
								if (_node.equals(node)) {
									return false;// 存在返回
								}
								if (_node.HashCode1 == 0) {// 空节点
									rf.seek(startIndex + Bytes.SIZEOF_INT + i * Bytes.SIZEOF_INT * 3);
									rf.writeInt(node.HashCode1);
									rf.writeInt(node.HashCode2);
									rf.writeInt(node.HashCode3);
									return true;
								}
							}
						}
						// 写文件尾
						long fileEnd = fileHeadSize + (long) nodeSize * repSize;
						_node.next = this.hashSize + this.repSize;// (int) fileEnd;
						rf.seek(startIndex);
						rf.writeInt(_node.next);
						rf.seek((int) fileEnd);
						rf.writeInt(0);
						rf.writeInt(node.HashCode1);
						rf.writeInt(node.HashCode2);
						rf.writeInt(node.HashCode3);
						for (int j = 1; j < mulSize; j++) {
							rf.writeInt(0);
							rf.writeInt(0);
							rf.writeInt(0);
						}
						repSize++;
						return true;
					}
				}
			} else {
				out.position(startIndex);
				_node.HashCode1 = out.getInt();
				if (_node.HashCode1 == 0) {
					out.position(startIndex);
					out.putInt(node.HashCode1);
					out.putInt(node.HashCode2);
					out.putInt(node.HashCode3);
					out.putInt(0);
					// out.force();//不需要刷新，只有一个写
					return true;
				}
				_node.HashCode2 = out.getInt();
				_node.HashCode3 = out.getInt();
				if (_node.equals(node)) {
					return false;// 存在返回
				}
				_node.next = out.getInt();
				if (_node.next == 0) {
					// 写文件尾
					long fileEnd = fileHeadSize + (long) nodeSize * repSize;
					if (fileEnd + this.nodeSize > Integer.MAX_VALUE) {
						throw new IOException("文件大小大于最大文件大小");
					} else if (fileEnd + this.nodeSize > this.mapFileSize) {
						mapFileSize = (int) (fileEnd * 1.3 > Integer.MAX_VALUE ? Integer.MAX_VALUE : fileEnd * 1.3);
						this.reMap(mapFileSize);
					}
					_node.next = this.hashSize + this.repSize;
					out.position(startIndex + Bytes.SIZEOF_INT * 3);
					out.putInt(_node.next);
					out.position((int) fileEnd);
					repSize++;
					if (mulSize <= 1) {
						out.putInt(node.HashCode1);
						out.putInt(node.HashCode2);
						out.putInt(node.HashCode3);
						out.putInt(0);
					} else {
						out.putInt(0);
						out.putInt(node.HashCode1);
						out.putInt(node.HashCode2);
						out.putInt(node.HashCode3);
						// out.position((int) fileEnd + this.nodeSize - 1);
						// out.put((byte) 0);
						for (int j = 1; j < mulSize; j++) {
							out.putInt(0);
							out.putInt(0);
							out.putInt(0);
						}
					}
					return true;
				} else {
					if (mulSize <= 1) {
						while (_node.next != 0) {
							startIndex = calcPostion(_node.next);
							assert (startIndex > 0 && startIndex < Integer.MAX_VALUE);
							out.position(startIndex);
							_node.clear(true);
							_node.HashCode1 = out.getInt();
							_node.HashCode2 = out.getInt();
							_node.HashCode3 = out.getInt();
							if (_node.equals(node)) {
								return false;// 存在返回
							}
							_node.next = out.getInt();
						}
						// 写文件尾
						long fileEnd = fileHeadSize + (long) nodeSize * repSize;
						if (fileEnd + this.nodeSize > Integer.MAX_VALUE) {
							throw new IOException("文件大小大于最大文件大小");
						} else if (fileEnd + this.nodeSize > this.mapFileSize) {
							mapFileSize = (int) (fileEnd * 1.3 > Integer.MAX_VALUE ? Integer.MAX_VALUE : fileEnd * 1.3);
							this.reMap(mapFileSize);
						}
						_node.next = this.hashSize + this.repSize;// (int) fileEnd;
						out.position(startIndex + Bytes.SIZEOF_INT * 3);
						out.putInt(_node.next);
						out.position((int) fileEnd);
						out.putInt(node.HashCode1);
						out.putInt(node.HashCode2);
						out.putInt(node.HashCode3);
						out.putInt(0);
						repSize++;
						return true;
					} else {
						while (_node.next != 0) {
							startIndex = calcPostion(_node.next);
							assert (startIndex > 0 && startIndex < Integer.MAX_VALUE);
							if (startIndex > this.mapFileSize) {
								System.err.println("startIndex=" + startIndex + "> mapFileSize=" + mapFileSize);
							}
							try {
								out.position(startIndex);
							} catch (Exception e) {
								e.printStackTrace();
								System.err.println("startIndex=" + startIndex + "> mapFileSize=" + mapFileSize + " _node.next="
										+ _node.next);
								throw new RuntimeException(e);
							}
							_node.next = out.getInt();
							for (int i = 0; i < this.mulSize; i++) {
								_node.clear(false);// 保留next
								_node.HashCode1 = out.getInt();
								_node.HashCode2 = out.getInt();
								_node.HashCode3 = out.getInt();
								if (_node.equals(node)) {
									return false;// 存在返回
								}
								if (_node.HashCode1 == 0) {// 空节点
									out.position(startIndex + Bytes.SIZEOF_INT + i * Bytes.SIZEOF_INT * 3);
									out.putInt(node.HashCode1);
									out.putInt(node.HashCode2);
									out.putInt(node.HashCode3);
									return true;
								}
							}
						}
						// 写文件尾
						long fileEnd = fileHeadSize + (long) nodeSize * repSize;
						if (fileEnd + this.nodeSize > Integer.MAX_VALUE) {
							throw new IOException("文件大小大于最大文件大小");
						} else if (fileEnd + this.nodeSize > this.mapFileSize) {
							mapFileSize = (int) (fileEnd * 1.3 > Integer.MAX_VALUE ? Integer.MAX_VALUE : fileEnd * 1.3);
							this.reMap(mapFileSize);
						}
						_node.next = this.hashSize + this.repSize;// (int) fileEnd;

						out.position(startIndex);

						out.putInt(_node.next);
						out.position((int) fileEnd);
						out.putInt(0);
						out.putInt(node.HashCode1);
						out.putInt(node.HashCode2);
						out.putInt(node.HashCode3);
						// out.position((int) fileEnd + this.nodeSize - 1);
						// out.put((byte) 0);
						for (int j = 1; j < mulSize; j++) {
							out.putInt(0);
							out.putInt(0);
							out.putInt(0);
						}
						repSize++;
						return true;
					}
				}
			}

		}

		@Override
		public void clear() {
			this.close();
		}
	}

	public static void main(String[] args) {
		System.err.println("freeMemory:" + Runtime.getRuntime().freeMemory());
		System.err.println("totalMemory:" + Runtime.getRuntime().totalMemory());
		long useMem = (Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory());
		System.err.println("use:" + useMem);

		System.out.println("NodeSize:" + HashTable.NodeSize);
		System.out.println("HashHeadSize:" + HashTable.HashHeadSize);
		HashTable ht = new HashTable(11111111);
		System.out.println("HashNode:" + sizeOf(new HashNode(0, 0, 0)));
		System.out.println("HashTable:" + sizeOf(ht));
		System.out.println("FULL HashNode:" + fullSizeOf(ht) + "       headSize=" + ht.heapSize);

		long time = System.nanoTime();
		int repSize = 0;
		for (int i = 0; i < 11111111; i++) {
			int h1 = (int) (Integer.MAX_VALUE * Math.random());
			int h2 = (int) (Integer.MAX_VALUE * Math.random());
			int h3 = (int) (Integer.MAX_VALUE * Math.random());
			if (!ht.addNode(new HashNode(h1, h2, h3))) {
				repSize++;
			}
			if (i % 1000000 == 0) {
				System.err.println(" 添加:" + i + " 失败：" + repSize + " 用时：" + (System.nanoTime() - time) / 1000000 + "ms   ");
			}
		}
		System.err.println(" 添加:10000000   失败：" + repSize + " 用时：" + (System.nanoTime() - time) / 1000000 + "ms   ");
		time = System.nanoTime();
		for (int i = 0; i < 11111111; i++) {
			int h1 = (int) (Integer.MAX_VALUE * Math.random());
			int h2 = (int) (Integer.MAX_VALUE * Math.random());
			int h3 = (int) (Integer.MAX_VALUE * Math.random());
			if (ht.contains(new HashNode(h1, h2, h3))) {
				repSize++;
			}
			if (i % 1000000 == 0) {
				System.err.println(" 判断:" + i + " 存在：" + repSize + " 用时：" + (System.nanoTime() - time) / 1000000 + "ms   ");
			}
		}
		System.err.println(" 判断:10000000   存在：" + repSize + " 用时：" + (System.nanoTime() - time) / 1000000 + "ms   ");

		System.out.println(ht.analysis());
		System.out.println("FULL HashNode:" + fullSizeOf(ht));
		long _useMem = (Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory());
		System.err.println("ouse:" + useMem);
		System.err.println("cuse:" + _useMem);
		System.err.println("add use:" + (_useMem - useMem));
	}

}
