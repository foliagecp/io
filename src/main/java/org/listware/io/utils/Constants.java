/*
 *  Copyright 2023 NJWS Inc.
 *  Copyright 2022 Listware
 */

package org.listware.io.utils;

import java.util.Map;

public class Constants {
	private static final Map<String, String> env = System.getenv();

	public class Namespaces {
		// Internal java functions namespace
		public static final String INTERNAL = "system";
	}

	public static class Kafka {
		public static String Addr() {
			return env.get("KAFKA_ADDR");
		}
	}

	public static class Cmdb {
		public static String Addr() {
			return env.get("CMDB_ADDR");
		}

		public static int Port() {
			return Integer.valueOf(env.get("CMDB_PORT"));
		}
	}

}
