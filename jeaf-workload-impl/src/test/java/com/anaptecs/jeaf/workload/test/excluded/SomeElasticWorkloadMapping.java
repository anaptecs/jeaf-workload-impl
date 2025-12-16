/**
 * Copyright 2004 - 2020 anaptecs GmbH, Burgstr. 96, 72764 Reutlingen, Germany
 *
 * All rights reserved.
 */
package com.anaptecs.jeaf.workload.test.excluded;

import com.anaptecs.jeaf.workload.annotations.ElasticWorkloadConfig;
import com.anaptecs.jeaf.workload.annotations.EndpointsGroup;
import com.anaptecs.jeaf.workload.annotations.HTTPMethod;
import com.anaptecs.jeaf.workload.annotations.Resource;
import com.anaptecs.jeaf.workload.annotations.WorkloadMapping;

@ElasticWorkloadConfig(
    workloadMapping = @WorkloadMapping(
        endpoints = @EndpointsGroup(
            resources = { @Resource(path = "api/v3/elastic", httpMethods = { HTTPMethod.GET, HTTPMethod.POST }),
              @Resource(
                  path = "api/v3/veryelastic",
                  httpMethods = { HTTPMethod.GET, HTTPMethod.POST, HTTPMethod.PUT, HTTPMethod.DELETE }),
              @Resource(path = "api/v3/notsoelastic") }),
        pipeline = ElasticPipeline.class))
public interface SomeElasticWorkloadMapping {

}
