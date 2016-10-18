package com.ery.server.util;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Field;
import java.net.URI;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Enumeration;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Set;

import javax.tools.JavaCompiler;
import javax.tools.JavaFileObject;
import javax.tools.SimpleJavaFileObject;
import javax.tools.ToolProvider;

@SuppressWarnings("all")
public class ClassUtils {

	private static DClassLoader classLoader = new DClassLoader();
	static {
		classLoader.setIgnoreConflict(true);
	}

	public static class DClassLoader extends URLClassLoader {

		boolean ignoreConflict = false;// 是否忽略冲突
		private Set<String> path = new HashSet<String>();

		public DClassLoader() {
			this(getSystemClassLoader());
		}

		public DClassLoader(ClassLoader parent) {
			super(new URL[] {}, parent);
		}

		/**
		 * 设置忽略冲突 （默认为false）
		 * 
		 * @param ignoreConflict true：忽略，与系统类发生冲突时，忽略冲突，返回系统类
		 *            false:不忽略，则当动态加载类时发现冲突会抛异常
		 */
		public void setIgnoreConflict(boolean ignoreConflict) {
			this.ignoreConflict = ignoreConflict;
		}

		/**
		 * 向加载器添加文件URL
		 * 
		 * @param urls
		 */
		public void addURL(URL... urls) {
			if (urls != null) {
				for (URL url : urls) {
					if (!path.contains(url.getPath())) {
						super.addURL(url);
						path.add(url.getPath());
					}
				}
			}
		}

		/**
		 * 向加载器添加文件（*.jar,*.zip,*.class)
		 * 
		 * @param files
		 * @throws java.io.IOException
		 */
		public void addFile(File... files) throws IOException {
			if (files != null) {
				for (File file : files) {
					if (file != null) {
						URL url = file.toURI().toURL();
						if (!path.contains(url.getPath())) {
							super.addURL(url);
							path.add(url.getPath());
						}
					}
				}
			}
		}

		/**
		 * 通过加载器载入一个类
		 * 
		 * @param name
		 * @return
		 * @throws Exception
		 */
		public Class<?> getClassByLoad(String name) throws Exception {
			Class<?> c = null;
			try {
				c = super.findSystemClass(name);
			} catch (ClassNotFoundException e) {
				return super.loadClass(name);
			}
			if (ignoreConflict) {
				// LOG.warn("需载入的类[" + name + "]与系统类冲突,忽略冲突,返回系统类！");
				return c;
			} else {
				throw new Exception("需载入的类[" + name + "]与系统类冲突!");
			}
		}
	}

	public static class JavaStringObject extends SimpleJavaFileObject {
		private String code;

		public JavaStringObject(String name, String code) {
			// super(URI.create("string:///" + name.replace('.', '/') +
			// Kind.SOURCE.extension), Kind.SOURCE);
			super(URI.create(name + ".java"), Kind.SOURCE);
			this.code = code;
		}

		@Override
		public CharSequence getCharContent(boolean ignoreEncodingErrors) throws IOException {
			return code;
		}
	}

	/**
	 * 获取一个class
	 * 
	 * @return
	 */
	public static Class getClassByJar(String jarName, String className) throws Exception {
		classLoader.addFile(new File(jarName));
		return classLoader.getClassByLoad(className);
	}

	/**
	 * 根据java代码获取一个类对象
	 * 
	 * @param javaCode 必须是完整的java文件代码【包名，类名，属性，方法……】
	 * @param className 包路径，类全名
	 * @return
	 * @throws Exception
	 */
	public static Class getClassByCode(String javaCode, String className) throws Exception {
		String[] arr = className.split(".");
		JavaCompiler javaCompiler = ToolProvider.getSystemJavaCompiler();
		JavaFileObject fileObject = new ClassUtils.JavaStringObject(arr[arr.length - 1], javaCode);
		JavaCompiler.CompilationTask task = javaCompiler.getTask(null, null, null,
				Arrays.asList("-d", ClassLoader.getSystemClassLoader().getResource("").getPath()), null,
				Arrays.asList(fileObject));
		boolean success = task.call();
		if (success) {
			return ClassLoader.getSystemClassLoader().loadClass(className);
		} else {
			throw new RuntimeException("编译失败");
		}
	}

	/**
	 * 获取一个类的所有字段，以及父类
	 * 
	 * @param clazz 类
	 * @param supClass 结束父类，如果不传则只取当前类
	 * @param hasPrivate 是否包含私有字段
	 * @return
	 */
	public static LinkedHashMap<String, Field> getAllFields(Class clazz, Class supClass, boolean hasPrivate) {
		LinkedHashMap<String, Field> fieldMap = new LinkedHashMap<String, Field>();
		Field[] fs = hasPrivate ? clazz.getDeclaredFields() : clazz.getFields();
		if (fs != null) {
			for (Field field : fs) {
				fieldMap.put(field.getName(), field);
			}
		}
		if (supClass.isAssignableFrom(clazz)) {
			for (clazz = clazz.getSuperclass();; clazz = clazz.getSuperclass()) {
				fs = hasPrivate ? clazz.getDeclaredFields() : clazz.getFields();
				if (fs != null) {
					for (Field field : fs) {
						if (!fieldMap.containsKey(field.getName())) {
							fieldMap.put(field.getName(), field);
						}
					}
				}
				if (clazz == supClass || clazz.equals(supClass)) {
					break;
				}
			}
		}
		return fieldMap;
	}

	public static List<Class> getAllClassByInterface(Class c) {
		String packageName = c.getPackage().getName();
		return getAllClassByInterface(c, packageName);
	}

	public static List<Class> getAllClassByInterface(Class c, String packageName) {
		// 返回结果
		List<Class> allClass = new ArrayList<Class>();
		// 如果不是一个接口，则不做处理
		try {
			// 获得当前包下以及子包下的所有类
			allClass = getClasses(packageName, c);
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		return allClass;
	}

	// 从一个包中查找出所有的类，在jar包中不能查找
	private static List<Class> getClasses(String packageName, Class c) throws ClassNotFoundException, IOException {
		ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
		// 用'/'代替'.'路径
		String path = packageName.replace('.', '/');
		Enumeration<URL> resources = classLoader.getResources(path);
		List<File> dirs = new ArrayList<File>();
		while (resources.hasMoreElements()) {
			URL resource = resources.nextElement();
			dirs.add(new File(java.net.URLDecoder.decode(resource.getFile())));
		}
		ArrayList<Class> classes = new ArrayList<Class>();
		for (File directory : dirs) {
			classes.addAll(findClasses(directory, packageName, c));
		}
		return classes;
	}

	private static List<Class> findClasses(File directory, String packageName, Class c) throws ClassNotFoundException {
		List<Class> classes = new ArrayList<Class>();
		if (!directory.exists()) {
			return classes;
		}
		File[] files = directory.listFiles();
		for (File file : files) {
			if (file.isDirectory()) {
				assert !file.getName().contains(".");
				classes.addAll(findClasses(file, packageName + "." + file.getName(), c));
			} else if (file.getName().endsWith(".class")) {
				Class clazz = Class.forName(packageName + '.'
						+ file.getName().substring(0, file.getName().length() - 6));
				if (!c.equals(clazz) && (c.isAssignableFrom(clazz) || c.isAssignableFrom(clazz))) {
					classes.add(clazz);
				}
			}
		}
		return classes;
	}
}