/**
 * Copyright 2004 - 2020 anaptecs GmbH, Burgstr. 96, 72764 Reutlingen, Germany
 *
 * All rights reserved.
 */
package com.anaptecs.jeaf.workload.impl;

import java.util.concurrent.ThreadPoolExecutor;

import com.anaptecs.jeaf.xfun.api.checks.Check;

/**
 * Class provides information about the current state of the thread pool of a pipeline-
 * 
 * @author JEAF Development Team
 */
public class PipelineInfo {
  /**
   * ID of the pipeline. The class / interface that defines the pipeline is used as ID.
   */
  private final String pipelineID;

  /**
   * Name of the pipeline as it was configured. The attribute will always be a real string.
   */
  private final String name;

  /**
   * Description of the pipeline. The attribute may be null.
   */
  private final String description;

  /**
   * Reference to thread pool that actually hold the information.
   */
  private final ThreadPoolExecutor threadPool;

  /**
   * Initialize object.
   * 
   * @param pThreadPool Thread pool which contains the actual information.
   */
  public PipelineInfo( String pPipelineID, String pName, String pDescription, ThreadPoolExecutor pThreadPool ) {
    // Check parameter
    Check.checkInvalidParameterNull(pPipelineID, "pPipelineID");
    Check.checkInvalidParameterNull(pName, "pName");
    Check.checkInvalidParameterNull(pThreadPool, "pThreadPool");

    pipelineID = pPipelineID;
    name = pName;
    description = pDescription;
    threadPool = pThreadPool;
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
  public String getName( ) {
    return name;
  }

  /**
   * Description of the pipeline.
   * 
   * @return {@link String} DDescription of the pipeline. The method may return null if no description was provided
   * through the configuration
   */
  public String getDescription( ) {
    return description;
  }

  /**
   * @see ThreadPoolExecutor#getCorePoolSize()
   */
  public int getCorePoolSize( ) {
    return threadPool.getCorePoolSize();
  }

  /**
   * @see ThreadPoolExecutor#getActiveCount()
   */
  public int getActiveCount( ) {
    return threadPool.getActiveCount();
  }

  /**
   * @see ThreadPoolExecutor#getTaskCount()
   */
  public long getTaskCount( ) {
    return threadPool.getTaskCount();
  }

  /**
   * @see ThreadPoolExecutor#getCompletedTaskCount()
   */
  public long getCompletedTaskCount( ) {
    return threadPool.getCompletedTaskCount();
  }

  /**
   * @see ThreadPoolExecutor#getLargestPoolSize()
   */
  public int getLargestPoolSize( ) {
    return threadPool.getLargestPoolSize();
  }

  /**
   * @see ThreadPoolExecutor#getMaximumPoolSize()
   */
  public int getMaximumPoolSize( ) {
    return threadPool.getMaximumPoolSize();
  }

  /**
   * @see ThreadPoolExecutor#getPoolSize()
   */
  public int getPoolSize( ) {
    return threadPool.getPoolSize();
  }
}
