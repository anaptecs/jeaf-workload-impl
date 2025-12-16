/**
 * Copyright 2004 - 2020 anaptecs GmbH, Burgstr. 96, 72764 Reutlingen, Germany
 *
 * All rights reserved.
 */
package com.anaptecs.jeaf.workload.test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.util.concurrent.TimeUnit;

import com.anaptecs.jeaf.workload.annotations.PipelineConfig;
import com.anaptecs.jeaf.workload.api.rest.RESTRequestTypeKey;
import com.anaptecs.jeaf.workload.impl.CommandExecutor;
import com.anaptecs.jeaf.workload.impl.Pipeline;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;

@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class CommandExecutorTest {
  @Test
  @Order(10)
  public void testCommandExecutor( ) throws Exception {
    PipelineConfig lPipelineConfig = NoQueuingPipeline.class.getAnnotation(PipelineConfig.class);
    Pipeline lPipeline = new Pipeline(NoQueuingPipeline.class.getName(), lPipelineConfig);

    RESTRequestTypeKey lRequestTypeKey = new RESTRequestTypeKey("api/v2/do", "POST");
    WorkloadErrorHandlerImpl lErrorHandler = new WorkloadErrorHandlerImpl();
    TimeUnit lTimeUnit = TimeUnit.MILLISECONDS;
    int lMaxLatency = 10;
    CommandExecutor lRunnable = new CommandExecutor(lRequestTypeKey, new Runnable() {
      @Override
      public void run( ) {
      }
    }, lPipeline, lMaxLatency, lTimeUnit, lErrorHandler);

    // Run executable. This should be fine.
    lRunnable.run();

    // Let's wait a little and try again.
    Thread.sleep(lTimeUnit.toMillis(lMaxLatency + 1));

    // Check that max latency is handled correctly.
    lRunnable.run();
    assertEquals(lRequestTypeKey, lErrorHandler.requestTypeKey);
    assertNotNull(lErrorHandler.exception);
    assertTrue(lErrorHandler.maximumLatencyExceeded);
    assertTrue(
        lErrorHandler.exception.getMessage().contains("8002] Rejecting executing of request 'api/v2/do (POST)'."));

    // Try execution of request with no latency maximum.
    lRunnable = new CommandExecutor(lRequestTypeKey, new Runnable() {
      @Override
      public void run( ) {
      }
    }, lPipeline, 0, lTimeUnit, lErrorHandler);

    Thread.sleep(10);
    lRunnable.run();

    // Test error cases.
    try {
      new CommandExecutor(null, lRunnable, lPipeline, lMaxLatency, lTimeUnit, lErrorHandler);
      fail("Exception expected.");
    }
    catch (IllegalArgumentException e) {
      assertEquals("Check failed. pRequestTypeKey must not be NULL.", e.getMessage());
    }
    try {
      new CommandExecutor(lRequestTypeKey, null, lPipeline, lMaxLatency, lTimeUnit, lErrorHandler);
      fail("Exception expected.");
    }
    catch (IllegalArgumentException e) {
      assertEquals("Check failed. pCommand must not be NULL.", e.getMessage());
    }
    try {
      new CommandExecutor(lRequestTypeKey, lRunnable, lPipeline, -1, null, lErrorHandler);
      fail("Exception expected.");
    }
    catch (IllegalArgumentException e) {
      assertEquals("Check failed. pTimeUnit must not be NULL.", e.getMessage());
    }
  }
}