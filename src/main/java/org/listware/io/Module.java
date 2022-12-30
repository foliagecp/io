/* Copyright 2022 Listware */

package org.listware.io;

import java.util.Map;

import org.apache.flink.statefun.sdk.spi.StatefulFunctionModule;
import org.listware.io.functions.egress.Egress;
import org.listware.io.router.IngressRouter;

import com.google.auto.service.AutoService;

@AutoService(StatefulFunctionModule.class)
public class Module implements StatefulFunctionModule {
	private IngressRouter ingressRouter = new IngressRouter();

	@Override
	public void configure(Map<String, String> globalConfiguration, Binder binder) {
		binder.bindIngress(IngressRouter.INGRESS_SPEC);
		binder.bindIngressRouter(IngressRouter.INGRESS, ingressRouter);

		binder.bindEgress(Egress.EGRESS_SPEC);
	}

}
