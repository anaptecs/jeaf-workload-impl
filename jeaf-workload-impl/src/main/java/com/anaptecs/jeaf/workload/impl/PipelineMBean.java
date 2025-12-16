/**
 * Copyright 2004 - 2021 anaptecs GmbH, Burgstr. 96, 72764 Reutlingen, Germany
 *
 * All rights reserved.
 */
package com.anaptecs.jeaf.workload.impl;

public interface PipelineMBean {
  String getName( );

  String getDescription( );

  int getPoolSize( );

  int getCorePoolSize( );

  int getMaximumPoolSize( );

  int getLargestPoolSize( );

  int getActiveCount( );

  long getKeepAliveTime( );

  int getRemainingQueueCapacity( );

  int getQueueSize( );

  long getCompletedTaskCount( );

  long getTaskCount( );

  long getRejectedTaskCount( );

  long getMaxLatencyExceededCounter( );
}
