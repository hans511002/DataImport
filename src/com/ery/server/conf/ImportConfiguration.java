/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.ery.server.conf;

import java.util.Map.Entry;
import java.util.Properties;
import java.util.UUID;

import com.ery.dimport.DImportConstant;

/**
 * Utility to create Hadoop {@link Configuration}s that include
 * HIVE_SEQ_BUILD_INDEX-specific resources.
 */
public class ImportConfiguration {
	public static final String UUID_KEY = "dimport.conf.uuid";

	private ImportConfiguration() {
	} // singleton

	/*
	 * Configuration.hashCode() doesn't return values that correspond to a
	 * unique set of parameters. This is a workaround so that we can track
	 * instances of Configuration created by Nutch.
	 */
	private static void setUUID(Configuration conf) {
		UUID uuid = UUID.randomUUID();
		conf.set(UUID_KEY, uuid.toString());
	}

	/**
	 * Retrieve a Nutch UUID of this configuration object, or null if the
	 * configuration was created elsewhere.
	 * 
	 * @param conf
	 *            configuration instance
	 * @return uuid or null
	 */
	public static String getUUID(Configuration conf) {
		return conf.get(UUID_KEY);
	}

	/**
	 * Create a {@link Configuration} for Nutch. This will load the standard
	 * Nutch resources, <code>nutch-default.xml</code> and
	 * <code>nutch-site.xml</code> overrides.
	 */
	public static Configuration create() {
		Configuration conf = new Configuration();
		setUUID(conf);
		addDImportResources(conf);
		return conf;
	}

	/**
	 * Create a {@link Configuration} from supplied properties.
	 * 
	 * @param addResources
	 *            if true, then first <code>buildindex.xml</code>, and then will
	 *            be loaded prior to applying the properties. Otherwise these
	 *            resources won't be used.
	 * @param properties
	 *            a set of properties to define (or override)
	 */
	public static Configuration create(boolean addResources, Properties properties) {
		Configuration conf = new Configuration();
		setUUID(conf);
		if (addResources) {
			addDImportResources(conf);
		}
		for (Entry<Object, Object> e : properties.entrySet()) {
			conf.set(e.getKey().toString(), e.getValue().toString());
		}
		return conf;
	}

	/**
	 * Add the standard HIVE_SEQ_BUILD_INDEX resources to {@link Configuration}.
	 * 
	 * @param conf
	 *            Configuration object to which configuration is to be added.
	 */
	private static Configuration addDImportResources(Configuration conf) {
		conf.addResource("dimport-site.xml");
		String[] files = conf.getStrings(DImportConstant.RESOURCES_KEY);
		if (files != null) {
			for (String file : files)
				conf.addResource(file);
		}
		return conf;
	}
}
