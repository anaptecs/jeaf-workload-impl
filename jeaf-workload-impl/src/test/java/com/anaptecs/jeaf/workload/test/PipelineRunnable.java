/**
 * Copyright 2004 - 2020 anaptecs GmbH, Burgstr. 96, 72764 Reutlingen, Germany
 *
 * All rights reserved.
 */
package com.anaptecs.jeaf.workload.test;

import java.util.Date;

import com.anaptecs.jeaf.xfun.api.XFun;

/**
 * Runnable that will be executed during pipeline tests.
 */
public class PipelineRunnable implements Runnable {
  boolean executed = false;

  final long executionTime;

  Date finishTimestamp;

  public PipelineRunnable( long pExecutionTime ) {
    executionTime = pExecutionTime;
  }

  @Override
  public void run( ) {
    try {
      Thread.sleep(executionTime);
    }
    catch (InterruptedException e) {
    }
    executed = true;
    finishTimestamp = new Date();
    XFun.getTrace().info("Runnable executed");
  }
}
