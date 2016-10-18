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

public enum EventType {
	// Messages originating from RS (NOTE: there is NO direct communication from
	// RS to Master). These are a result of RS updates into ZK.
	// RS_ZK_REGION_CLOSING (1), // It is replaced by M_ZK_REGION_CLOSING(HBASE-4739)
	TOPOLOGY_TRIGGER(1, ExecutorType.TOPOLOGY_TRIGGER), //
	TOPOLOGY_SUBMIT(2, ExecutorType.TOPOLOGY_SUBMIT), //
	TOPOLOGY_SHOWDOWN(3, ExecutorType.TOPOLOGY_SHOWDOWN), //

	NODE_SUBMIT(20, ExecutorType.NODE_SUBMIT), //
	NODE_UPADTE(21, ExecutorType.NODE_UPADTE), //
	NODE_MOVE(22, ExecutorType.NODE_MOVE), //
	//
	NODE_DATA_COLL(23, ExecutorType.NODE_DATA_COLL) //
	;
	private final int code;
	private final ExecutorType executor;

	/**
	 * Constructor
	 */
	EventType(final int code, final ExecutorType executor) {
		this.code = code;
		this.executor = executor;
	}

	public int getCode() {
		return this.code;
	}

	public static EventType get(final int code) {
		// Is this going to be slow? Its used rare but still...
		for (EventType et : EventType.values()) {
			if (et.getCode() == code)
				return et;
		}
		throw new IllegalArgumentException("Unknown code " + code);
	}

	ExecutorType getExecutorServiceType() {
		return this.executor;
	}
}
