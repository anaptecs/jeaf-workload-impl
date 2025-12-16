/**
 * Copyright 2004 - 2020 anaptecs GmbH, Burgstr. 96, 72764 Reutlingen, Germany
 *
 * All rights reserved.
 */
package com.anaptecs.jeaf.workload.test;

import com.anaptecs.jeaf.workload.annotations.EndpointsGroup;
import com.anaptecs.jeaf.workload.annotations.StaticWorkloadConfig;
import com.anaptecs.jeaf.workload.annotations.WorkloadMapping;

@StaticWorkloadConfig(
    workloadMapping = @WorkloadMapping(
        endpoints = @EndpointsGroup(endpointURLs = "api/v2"),
        pipeline = FIFOPipeline.class))
public interface HighThroughput {

}
