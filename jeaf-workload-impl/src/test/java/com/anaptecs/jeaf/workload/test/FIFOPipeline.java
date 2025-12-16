/**
 * Copyright 2004 - 2020 anaptecs GmbH, Burgstr. 96, 72764 Reutlingen, Germany
 *
 * All rights reserved.
 */
package com.anaptecs.jeaf.workload.test;

import com.anaptecs.jeaf.workload.annotations.PipelineConfig;
import com.anaptecs.jeaf.workload.annotations.QueueType;

@PipelineConfig(
    name = "FIFO-Pipeline",
    description = "FIFO-Pipeline is used as default for requests",
    defaultPipeline = true,
    coreThreads = FIFOPipeline.CORE_THREADS,
    maxThreads = FIFOPipeline.MAX_THREADS,
    queueType = QueueType.FIFO,
    maxLatency = FIFOPipeline.MAX_LATENCY,
    maxQueueDepth = FIFOPipeline.MAX_QUEUE_DEPTH)
public interface FIFOPipeline {
  public final int MAX_QUEUE_DEPTH = 40;

  public final int MAX_LATENCY = 1000;

  public final int CORE_THREADS = 2;

  public final int MAX_THREADS = 5;
}
