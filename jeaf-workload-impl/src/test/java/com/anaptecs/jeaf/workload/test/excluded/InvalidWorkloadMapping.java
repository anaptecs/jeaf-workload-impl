/**
 * Copyright 2004 - 2020 anaptecs GmbH, Burgstr. 96, 72764 Reutlingen, Germany
 *
 * All rights reserved.
 */
package com.anaptecs.jeaf.workload.test.excluded;

import com.anaptecs.jeaf.workload.annotations.ElasticWorkloadConfig;
import com.anaptecs.jeaf.workload.annotations.EndpointsGroup;
import com.anaptecs.jeaf.workload.annotations.WorkloadMapping;

@ElasticWorkloadConfig(
    workloadMapping = @WorkloadMapping(pipeline = ElasticPipeline.class, endpoints = @EndpointsGroup()))
public interface InvalidWorkloadMapping {

}
