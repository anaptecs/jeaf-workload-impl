/**
 * Copyright 2004 - 2020 anaptecs GmbH, Burgstr. 96, 72764 Reutlingen, Germany
 *
 * All rights reserved.
 */
package com.anaptecs.jeaf.workload.test;

import com.anaptecs.jeaf.workload.annotations.PipelineConfig;
import com.anaptecs.jeaf.workload.annotations.QueueType;

@PipelineConfig(
    coreThreads = NoQueuingPipeline.MAX_THREADS,
    queueType = QueueType.NOT_QUEUED,
    maxLatency = NoQueuingPipeline.MAX_LATENCY)
public interface NoQueuingPipeline {
  public final int MAX_THREADS = 1;

  public final int MAX_LATENCY = 100;
}
