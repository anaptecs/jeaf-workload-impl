/**
 * Copyright 2004 - 2020 anaptecs GmbH, Burgstr. 96, 72764 Reutlingen, Germany
 *
 * All rights reserved.
 */
package com.anaptecs.jeaf.workload.test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.List;

import com.anaptecs.jeaf.workload.annotations.PipelineConfig;
import com.anaptecs.jeaf.workload.api.rest.RESTRequestTypeKey;
import com.anaptecs.jeaf.workload.impl.Pipeline;
import com.anaptecs.jeaf.workload.impl.PipelineInfo;
import com.anaptecs.jeaf.xfun.api.XFun;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;

@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class PipelineTests {
  @Test
  @Order(10)
  public void testNoQueuingPipeline( ) throws Exception {
    PipelineConfig lPipelineConfig = NoQueuingPipeline.class.getAnnotation(PipelineConfig.class);
    assertNotNull(lPipelineConfig, "@PipelineConfig annotation is missing.");

    Pipeline lPipeline = new Pipeline(NoQueuingPipeline.class.getName(), lPipelineConfig);
    assertEquals(NoQueuingPipeline.class.getName(), lPipeline.getPipelineID());
    assertEquals(NoQueuingPipeline.class.getName(), lPipeline.getName());
    assertEquals("", lPipeline.getDescription());

    int lExecutionTime = 50;
    PipelineRunnable lRunnable = new PipelineRunnable(lExecutionTime);
    RESTRequestTypeKey lRequestTypeKey = new RESTRequestTypeKey("api/v1/NoQueuing", "GET");

    // Execute command.
    WorkloadErrorHandlerImpl lErrorHandler = new WorkloadErrorHandlerImpl();
    lPipeline.execute(lRequestTypeKey, lErrorHandler, lRunnable);

    // Wait until command is executed.
    Thread.sleep(lExecutionTime + 200);
    XFun.getTrace().info("Checking execution of Runnable.");
    assertTrue(lRunnable.executed, "Runnable was not executed");
    assertTrue(lErrorHandler.noExceptions);

    // Try to execute 3 commands directly after each other. As we do not support queuing it is expected that the
    // third request will be rejected.
    PipelineRunnable lFirstRunnable = new PipelineRunnable(lExecutionTime);
    PipelineRunnable lSecondRunnable = new PipelineRunnable(lExecutionTime);
    PipelineRunnable lThirdRunnable = new PipelineRunnable(lExecutionTime);

    lErrorHandler.reset();
    lPipeline.execute(lRequestTypeKey, lErrorHandler, lFirstRunnable);
    lPipeline.execute(lRequestTypeKey, lErrorHandler, lSecondRunnable);
    lPipeline.execute(lRequestTypeKey, lErrorHandler, lThirdRunnable);
    assertEquals(lRequestTypeKey, lErrorHandler.requestTypeKey);
    assertNotNull(lErrorHandler.exception);
    assertTrue(lErrorHandler.requestRejected);

    Thread.sleep(lExecutionTime * 3);
    assertTrue(lFirstRunnable.executed, "First Runnable was not executed");
    assertTrue(lSecondRunnable.executed, "Second Runnable was not executed");
    assertTrue(lFirstRunnable.finishTimestamp.before(lSecondRunnable.finishTimestamp), "Executing order was not fair.");
    lErrorHandler.reset();

    // Test error handling in case that max latency is exceeded.
    lExecutionTime = NoQueuingPipeline.MAX_LATENCY * 2;
    lFirstRunnable = new PipelineRunnable(lExecutionTime);
    lSecondRunnable = new PipelineRunnable(lExecutionTime);
    lPipeline.execute(lRequestTypeKey, lErrorHandler, lFirstRunnable);
    lPipeline.execute(lRequestTypeKey, lErrorHandler, lSecondRunnable);
    Thread.sleep(lExecutionTime * 3);
    assertTrue(lFirstRunnable.executed, "First Runnable was not executed");
    assertFalse(lSecondRunnable.executed, "Runnable was executed");
    assertFalse(lErrorHandler.noExceptions);
    assertTrue(lErrorHandler.maximumLatencyExceeded);
  }

  @Test
  @Order(10)
  public void testFIFOPipeline( ) throws Exception {
    PipelineConfig lPipelineConfig = FIFOPipeline.class.getAnnotation(PipelineConfig.class);
    assertNotNull(lPipelineConfig, "@PipelineConfig annotation is missing.");

    Pipeline lPipeline = new Pipeline(FIFOPipeline.class.getName(), lPipelineConfig);
    assertEquals(FIFOPipeline.class.getName(), lPipeline.getPipelineID());
    assertEquals("FIFO-Pipeline", lPipeline.getName());
    assertEquals("FIFO-Pipeline is used as default for requests", lPipeline.getDescription());

    // Also check the created thread pool.
    PipelineInfo lThreadPoolInfo = lPipeline.getPipelineInfo();
    assertNotNull(lThreadPoolInfo);
    assertEquals(0, lThreadPoolInfo.getActiveCount());
    assertEquals(0, lThreadPoolInfo.getCompletedTaskCount());
    assertEquals(0, lThreadPoolInfo.getTaskCount());
    assertEquals(FIFOPipeline.CORE_THREADS, lThreadPoolInfo.getCorePoolSize());
    assertEquals(FIFOPipeline.MAX_THREADS, lThreadPoolInfo.getMaximumPoolSize());
    assertEquals(0, lThreadPoolInfo.getLargestPoolSize());
    assertEquals(0, lThreadPoolInfo.getPoolSize());

    int lExecutionTime = 50;
    PipelineRunnable lRunnable = new PipelineRunnable(lExecutionTime);
    RESTRequestTypeKey lRequestTypeKey = new RESTRequestTypeKey("api/v1/FIFOQueuing", "PUT");

    // Execute command.
    WorkloadErrorHandlerImpl lErrorHandler = new WorkloadErrorHandlerImpl();
    lPipeline.execute(lRequestTypeKey, lErrorHandler, lRunnable);

    // Check thread pool info during execution
    assertEquals(1, lThreadPoolInfo.getActiveCount());
    assertEquals(0, lThreadPoolInfo.getCompletedTaskCount());
    assertEquals(1, lThreadPoolInfo.getTaskCount());
    assertEquals(FIFOPipeline.CORE_THREADS, lThreadPoolInfo.getCorePoolSize());
    assertEquals(FIFOPipeline.MAX_THREADS, lThreadPoolInfo.getMaximumPoolSize());
    assertEquals(1, lThreadPoolInfo.getLargestPoolSize());
    assertEquals(1, lThreadPoolInfo.getPoolSize());

    // Wait until command is executed.
    Thread.sleep(lExecutionTime * 2);
    assertTrue(lRunnable.executed, "Runnable was not executed");
    assertTrue(lErrorHandler.noExceptions);
    lErrorHandler.reset();

    // Check thread pool info after first execution
    assertEquals(0, lThreadPoolInfo.getActiveCount());
    assertEquals(1, lThreadPoolInfo.getCompletedTaskCount());
    assertEquals(1, lThreadPoolInfo.getTaskCount());
    assertEquals(FIFOPipeline.CORE_THREADS, lThreadPoolInfo.getCorePoolSize());
    assertEquals(FIFOPipeline.MAX_THREADS, lThreadPoolInfo.getMaximumPoolSize());
    assertEquals(1, lThreadPoolInfo.getLargestPoolSize());
    assertEquals(1, lThreadPoolInfo.getPoolSize());

    // Try to fill the queue
    int lCount = 20;
    List<PipelineRunnable> lRunnables = new ArrayList<>(lCount);
    for (int i = 0; i < lCount; i++) {
      lRunnables.add(new PipelineRunnable(lExecutionTime));
    }

    // Pass runnables to pipeline.
    for (PipelineRunnable lPipelineRunnable : lRunnables) {
      lPipeline.execute(lRequestTypeKey, lErrorHandler, lPipelineRunnable);
    }

    // Check thread pool info
    Thread.sleep(10);
    assertEquals(FIFOPipeline.CORE_THREADS, lThreadPoolInfo.getActiveCount());
    assertEquals(1, lThreadPoolInfo.getCompletedTaskCount());
    assertEquals(1 + lCount, lThreadPoolInfo.getTaskCount());
    assertEquals(FIFOPipeline.CORE_THREADS, lThreadPoolInfo.getCorePoolSize());
    assertEquals(FIFOPipeline.MAX_THREADS, lThreadPoolInfo.getMaximumPoolSize());
    assertEquals(2, lThreadPoolInfo.getLargestPoolSize());
    assertEquals(FIFOPipeline.CORE_THREADS, lThreadPoolInfo.getPoolSize());

    // Calculate time tom wait.
    int lExpectedRuntime = lExecutionTime * lCount / FIFOPipeline.CORE_THREADS;
    System.out.println("Expected overall execution time: " + lExpectedRuntime);
    Thread.sleep(lExpectedRuntime + 200);
    assertTrue(lErrorHandler.noExceptions);

    // Check thread pool
    assertEquals(0, lThreadPoolInfo.getActiveCount());
    assertEquals(1 + lCount, lThreadPoolInfo.getCompletedTaskCount());
    assertEquals(1 + lCount, lThreadPoolInfo.getTaskCount());
    assertEquals(FIFOPipeline.CORE_THREADS, lThreadPoolInfo.getCorePoolSize());
    assertEquals(FIFOPipeline.MAX_THREADS, lThreadPoolInfo.getMaximumPoolSize());
    assertEquals(2, lThreadPoolInfo.getLargestPoolSize());
    assertEquals(FIFOPipeline.CORE_THREADS, lThreadPoolInfo.getPoolSize());
  }
}
