/* Copyright 2022 Listware */

package org.listware.io.utils;

public class Constants {
	public class Namespaces {
		// Internal java functions namespace
		public static final String INTERNAL = "system";
		// External proxy functions namespace
		public static final String EXTERNAL = "proxy";
	}

	public class Kafka {
		public static final String SERVER = "kafka:9092";
	}

	public class Cmdb {
		// FIXME from secrets
		public static final String ADDR = "cmdb.service.consul";
		public static final int PORT = 8529;
		public static final String USER = "root";
		public static final String PASSWORD = "password";
		public static final String DBNAME = "CMDBv2";

		public class Qdsl {
			public static final String ADDR = "qdsl.cmdb.service.consul";
			public static final int PORT = 31415;
		}
	}

}
