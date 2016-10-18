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

package com.ery.server.executor;

/**
 * The following is a list of all executor types, both those that run in the master and those that run in the regionserver.
 */
public enum ExecutorType {

	// Top executor services
	TOPOLOGY_TRIGGER(1), //
	TOPOLOGY_SUBMIT(2), //
	TOPOLOGY_SHOWDOWN(3), //

	NODE_SUBMIT(20), //
	NODE_UPADTE(21), //
	NODE_MOVE(22), //
	//
	NODE_DATA_COLL(23), //
	;

	ExecutorType(int value) {
	}

	/**
	 * @param serverName
	 * @return Conflation of the executor type and the passed servername.
	 */
	String getExecutorName(String serverName) {
		return this.toString() + "-" + serverName.replace("%", "%%");
	}
}