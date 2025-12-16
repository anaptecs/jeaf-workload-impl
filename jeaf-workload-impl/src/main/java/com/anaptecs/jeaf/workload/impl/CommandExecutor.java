/**
 * Copyright 2004 - 2020 anaptecs GmbH, Burgstr. 96, 72764 Reutlingen, Germany
 *
 * All rights reserved.
 */
package com.anaptecs.jeaf.workload.impl;

import java.util.concurrent.TimeUnit;

import com.anaptecs.jeaf.tools.annotations.monitoring.PerformanceMonitoringSegment;
import com.anaptecs.jeaf.tools.api.Tools;
import com.anaptecs.jeaf.tools.api.monitoring.TimerSample;
import com.anaptecs.jeaf.workload.api.RequestTypeKey;
import com.anaptecs.jeaf.workload.api.WorkloadErrorHandler;
import com.anaptecs.jeaf.workload.api.WorkloadMessages;
import com.anaptecs.jeaf.workload.api.WorkloadSystemException;
import com.anaptecs.jeaf.xfun.api.XFun;
import com.anaptecs.jeaf.xfun.api.checks.Check;
import com.anaptecs.jeaf.xfun.api.errorhandling.JEAFSystemException;
import com.anaptecs.jeaf.xfun.api.messages.MessageID;

/**
 * Class implements a runnable for JEAF's Workload Management that ensures that ensures that a defined maximum latency
 * will not be exceeded. If the max latency is exceeded before the runnable will be executed then an exception will be
 * thrown instead of the execution of the request.
 * 
 * @author JEAF Development Team
 */
public class CommandExecutor implements Runnable {
  private static final String PREFIX = CommandExecutor.class.getName() + ".";

  private static final String PIPELINE_LATENCY_SUFFIX = ".PipelineLatency";

  private final TimerSample timerSample;

  private final Pipeline pipeline;

  /**
   * Request key that can be used to identify the request type.
   */
  private final RequestTypeKey requestTypeKey;

  /**
   * Command that should actually be executed.
   */
  private final Runnable command;

  /**
   * Nano time when the runnable was created.
   */
  private final long startNanos;

  /**
   * Max latency in nano seconds.
   */
  private final long maxNanoLatency;

  /**
   * Reference to error handler that needs to be called in case of exceptions during execution of the command.
   */
  private final WorkloadErrorHandler errorHandler;

  /**
   * Initialize object.
   * 
   * @param pRequestTypeKey Request type key that belongs to the request. The parameter must not be null.
   * @param pCommand Runnable object representing the request that should be executed. The parameter must not be null.
   * @param pMaxLatency Maximum latency that is accepted that the request will be delayed. If parameter is 0 or smaller
   * then latency will not be checked.
   * @param pTimeUnit Time unit of the maximum latency. The parameter must not be null.
   */
  public CommandExecutor( RequestTypeKey pRequestTypeKey, Runnable pCommand, Pipeline pPipeline, long pMaxLatency,
      TimeUnit pTimeUnit, WorkloadErrorHandler pErrorHandler ) {
    // Check parameter
    Check.checkInvalidParameterNull(pRequestTypeKey, "pRequestTypeKey");
    Check.checkInvalidParameterNull(pCommand, "pCommand");
    Check.checkInvalidParameterNull(pPipeline, "pPipeline");
    Check.checkInvalidParameterNull(pTimeUnit, "pTimeUnit");
    Check.checkInvalidParameterNull(pErrorHandler, "pErrorHandler");

    requestTypeKey = pRequestTypeKey;
    command = pCommand;
    pipeline = pPipeline;
    startNanos = System.nanoTime();
    maxNanoLatency = pTimeUnit.toNanos(pMaxLatency);
    errorHandler = pErrorHandler;

    // Create new timer sample to monitor pipeline latency
    StringBuilder lBuilder = new StringBuilder();
    lBuilder.append(PREFIX);
    lBuilder.append(pipeline.getPipelineID());
    lBuilder.append(PIPELINE_LATENCY_SUFFIX);

    timerSample = Tools.getMonitoringTools().newTimerSample(lBuilder.toString());
  }

  /**
   * Method to actually execute the passed request. Before the real request is executed this method checks that the
   * maximum latency is not exceeded. In this case the request will be rejected.
   * 
   * @throws JEAFSystemException in case that the maximum latency is exceeded before the request was executed.
   */
  @PerformanceMonitoringSegment
  @Override
  public void run( ) {
    // Check if max latency is exceeded.
    long lRealLatency = System.nanoTime() - startNanos;

    // Latency limits are NOT exceeded.
    if (maxNanoLatency <= 0 || lRealLatency <= maxNanoLatency) {
      long lLatencyMillis = TimeUnit.NANOSECONDS.toMillis(lRealLatency);

      // Trace some info
      MessageID lMessage = WorkloadMessages.EXECUTING_REQUEST;
      if (lMessage.isEnabled()) {
        XFun.getTrace().write(lMessage, requestTypeKey.getKey(), Long.toString(lLatencyMillis));
      }
      Tools.getMonitoringTools().recordTimerSample(timerSample);
      command.run();
    }
    // Execution of runnable will be rejected as the defined maximum latency was exceeded.
    else {
      long lDifference = TimeUnit.NANOSECONDS.toMillis(lRealLatency - maxNanoLatency);
      pipeline.incrementMaxLatencyExceededCounter();
      WorkloadSystemException lException =
          new WorkloadSystemException(WorkloadMessages.REJECTING_REQUEST_DUE_TO_LATENCY_LIMIT, requestTypeKey.getKey(),
              Long.toString(lRealLatency), Long.toString(maxNanoLatency), Long.toString(lDifference));
      errorHandler.maximumLatencyExceeded(requestTypeKey, lException);
    }
  }
}
