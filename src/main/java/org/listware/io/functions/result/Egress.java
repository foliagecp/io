/*
 *  Copyright 2023 NJWS Inc.
 *  Copyright 2022 Listware
 */

package org.listware.io.functions.result;

import org.apache.flink.statefun.sdk.io.EgressIdentifier;
import org.apache.flink.statefun.sdk.io.EgressSpec;
import org.apache.flink.statefun.sdk.kafka.KafkaEgressBuilder;
import org.apache.flink.statefun.sdk.reqreply.generated.TypedValue;
import org.listware.io.utils.Constants;
import org.listware.io.utils.KafkaEgressTypedValueSerializer;

public class Egress {
	public static final String EGRESS_NAME = "output.system";

	public static final EgressIdentifier<TypedValue> EGRESS = new EgressIdentifier<>(Constants.Namespaces.INTERNAL,
			EGRESS_NAME, TypedValue.class);

	public static EgressSpec<TypedValue> EGRESS_SPEC = KafkaEgressBuilder.forIdentifier(EGRESS)
			.withKafkaAddress(Constants.Kafka.Addr()).withSerializer(KafkaEgressTypedValueSerializer.class).build();

}
