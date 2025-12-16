/**
 * Copyright 2004 - 2020 anaptecs GmbH, Burgstr. 96, 72764 Reutlingen, Germany
 *
 * All rights reserved.
 */
package com.anaptecs.jeaf.workload.impl;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import com.anaptecs.jeaf.tools.api.Tools;
import com.anaptecs.jeaf.workload.annotations.ElasticWorkloadConfig;
import com.anaptecs.jeaf.workload.annotations.PipelineConfig;
import com.anaptecs.jeaf.workload.annotations.QueueType;
import com.anaptecs.jeaf.workload.annotations.StaticWorkloadConfig;
import com.anaptecs.jeaf.workload.annotations.WorkloadMapping;
import com.anaptecs.jeaf.workload.api.RequestTypeKey;
import com.anaptecs.jeaf.workload.api.WorkloadErrorHandler;
import com.anaptecs.jeaf.xfun.api.checks.Assert;
import com.anaptecs.jeaf.xfun.api.checks.Check;

/**
 * Class implements a pipeline for JEAF's workload Management. Pipelines are one of the central parts of the workload
 * management. Each pipeline has its own thread pool and queuing strategy. Which pipeline is used for a concrete request
 * type can be configured using so called workload mappings. All those configurations are done using annotations:
 * {@link StaticWorkloadConfig}, {@link ElasticWorkloadConfig}, {@link WorkloadMapping} and {@link PipelineConfig}
 * 
 * @author JEAF Development Team
 */
public class Pipeline implements PipelineMBean {
  /**
   * ID of the pipeline. The class / interface that defines the pipeline is used as ID.
   */
  private final String pipelineID;

  /**
   * Name of the pipeline as it was configured. The attribute will always be a real string.
   */
  private final String name;

  /**
   * Reference to the pipeline configuration.
   */
  private final PipelineConfig pipelineConfig;

  /**
   * Executor represents the thread pool that is used to execute requests.
   */
  private final ThreadPoolExecutor threadPool;

  /**
   * Counter to track amount of rejected requests.
   */
  private long rejectionCounter = 0;

  /**
   * Counter to track amount of request that were rejected due to too high queue wait time
   */
  private long maxLatencyExceededCounter;

  /**
   * Initialize object.
   * 
   * @param pPipelineClass Class that defines the pipeline configuration. The parameter must not be null. This class is
   * also used as ID for the pipeline.
   * @param pPipelineConfig Configuration of the pipeline. The parameter must not be null.
   */
  public Pipeline( String pPipelineID, PipelineConfig pPipelineConfig ) {
    // Check parameters.
    Check.checkInvalidParameterNull(pPipelineID, "pPipelineID");
    Check.checkInvalidParameterNull(pPipelineConfig, "pPipelineConfig");

    // Resolve pipeline configuration values
    pipelineID = pPipelineID;

    // Resolve name of pipeline
    String lName = pPipelineConfig.name();
    if (Tools.getStringTools().isRealString(lName)) {
      name = lName;
    }
    else {
      name = pPipelineID;
    }

    // Resolve additional parameters from configuration.
    pipelineConfig = pPipelineConfig;

    // Create new thread pool for pipeline
    QueueType lQueueType = pPipelineConfig.queueType();

    BlockingQueue<Runnable> lRequestQueue;
    switch (lQueueType) {
      case NOT_QUEUED:
        lRequestQueue = new ArrayBlockingQueue<>(1, true);
        break;
      case FIFO:
        lRequestQueue = new ArrayBlockingQueue<>(pipelineConfig.maxQueueDepth(), true);
        break;
      case PRIORIZED:
      case FAIR_WEIGHTED:
        Assert.internalError("Not yet implemented.");
        lRequestQueue = null;
        break;
      default:
        Assert.unexpectedEnumLiteral(lQueueType);
        lRequestQueue = null;
    }
    // Create new thread pool
    int lMaxThreads = Math.max(pPipelineConfig.coreThreads(), pPipelineConfig.maxThreads());
    threadPool = new ThreadPoolExecutor(pPipelineConfig.coreThreads(), lMaxThreads,
        pPipelineConfig.maxThreadKeepAlive(), pPipelineConfig.timeUnit(), lRequestQueue);
  }

  /**
   * Method returns the ID of the pipeline.
   * 
   * @return {@link Class} ID of the pipeline.
   */
  public String getPipelineID( ) {
    return pipelineID;
  }

  /**
   * Method returns the name of the pipeline.
   * 
   * @return {@link String} Name of the pipeline. The method never returns null.
   */
  @Override
  public String getName( ) {
    return name;
  }

  /**
   * Description of the pipeline.
   * 
   * @return {@link String} DDescription of the pipeline. The method may return null if no description was provided
   * through the configuration
   */
  @Override
  public String getDescription( ) {
    return pipelineConfig.description();
  }

  /**
   * Method returns information about the thread pool of this pipeline.
   * 
   * @return {@link PipelineInfo} Info about the thread pool of this pipeline. The method never returns null.
   */
  public PipelineInfo getPipelineInfo( ) {
    return new PipelineInfo(pipelineID, name, this.getDescription(), threadPool);
  }

  /**
   * Method can be used to hand over a request to the pipeline. Depending on the current load on the pipeline and its
   * queuing strategy the request may be executed directly, queued or rejected.
   * 
   * @param pRequestTypeKey Request type of the request. The parameter must not be null.
   * @param pErrorHandler Error handler that should be used to indicate problems during the execution of requests.
   * @param pCommand Runnable object representing the request that should be executed. The parameter must not be null.
   */
  public void execute( RequestTypeKey pRequestTypeKey, WorkloadErrorHandler pErrorHandler, Runnable pCommand ) {
    // Create new executor for the request.
    CommandExecutor lCommandExecutor = new CommandExecutor(pRequestTypeKey, pCommand, this, pipelineConfig.maxLatency(),
        pipelineConfig.timeUnit(), pErrorHandler);

    // Hand over request to executor.
    try {
      threadPool.execute(lCommandExecutor);
    }
    catch (RejectedExecutionException e) {
      rejectionCounter++;
      pErrorHandler.requestRejected(pRequestTypeKey, e);
    }
  }

  // Implements methods for MBean
  @Override
  public int getPoolSize( ) {
    return threadPool.getPoolSize();
  }

  @Override
  public int getCorePoolSize( ) {
    return threadPool.getCorePoolSize();
  }

  @Override
  public int getMaximumPoolSize( ) {
    return threadPool.getMaximumPoolSize();
  }

  @Override
  public int getLargestPoolSize( ) {
    return threadPool.getMaximumPoolSize();
  }

  @Override
  public int getActiveCount( ) {
    return threadPool.getActiveCount();
  }

  @Override
  public long getKeepAliveTime( ) {
    return threadPool.getKeepAliveTime(TimeUnit.MILLISECONDS);
  }

  @Override
  public int getRemainingQueueCapacity( ) {
    return threadPool.getQueue().remainingCapacity();
  }

  @Override
  public int getQueueSize( ) {
    return threadPool.getQueue().size();
  }

  @Override
  public long getCompletedTaskCount( ) {
    return threadPool.getCompletedTaskCount();
  }

  @Override
  public long getTaskCount( ) {
    return threadPool.getTaskCount();
  }

  @Override
  public long getRejectedTaskCount( ) {
    return rejectionCounter;
  }

  @Override
  public long getMaxLatencyExceededCounter( ) {
    return maxLatencyExceededCounter;
  }

  public void incrementMaxLatencyExceededCounter( ) {
    maxLatencyExceededCounter++;
  }
}
