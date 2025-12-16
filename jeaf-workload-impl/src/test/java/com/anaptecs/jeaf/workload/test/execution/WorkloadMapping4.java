/**
 * Copyright 2004 - 2020 anaptecs GmbH, Burgstr. 96, 72764 Reutlingen, Germany
 *
 * All rights reserved.
 */
package com.anaptecs.jeaf.workload.test.execution;

import com.anaptecs.jeaf.workload.annotations.EndpointsGroup;
import com.anaptecs.jeaf.workload.annotations.HTTPMethod;
import com.anaptecs.jeaf.workload.annotations.Resource;
import com.anaptecs.jeaf.workload.annotations.StaticWorkloadConfig;
import com.anaptecs.jeaf.workload.annotations.WorkloadMapping;

@StaticWorkloadConfig(
    workloadMapping = @WorkloadMapping(
        pipeline = Pipeline2.class,
        endpoints = @EndpointsGroup(resources = @Resource(path = "api/v2/Object", httpMethods = HTTPMethod.GET))))
public interface WorkloadMapping4 {

}
