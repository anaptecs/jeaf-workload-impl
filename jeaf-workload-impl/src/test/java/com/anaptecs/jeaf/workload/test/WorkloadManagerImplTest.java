/**
 * Copyright 2004 - 2020 anaptecs GmbH, Burgstr. 96, 72764 Reutlingen, Germany
 *
 * All rights reserved.
 */
package com.anaptecs.jeaf.workload.test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import com.anaptecs.jeaf.workload.api.RequestType;
import com.anaptecs.jeaf.workload.api.RequestTypeKey;
import com.anaptecs.jeaf.workload.api.Workload;
import com.anaptecs.jeaf.workload.api.WorkloadManager;
import com.anaptecs.jeaf.workload.api.WorkloadMessages;
import com.anaptecs.jeaf.workload.api.WorkloadSystemException;
import com.anaptecs.jeaf.workload.api.rest.RESTRequestType;
import com.anaptecs.jeaf.workload.api.rest.RESTRequestTypeKey;
import com.anaptecs.jeaf.workload.impl.PipelineInfo;
import com.anaptecs.jeaf.workload.impl.WorkloadManagerImpl;
import com.anaptecs.jeaf.workload.test.excluded.DefaultPipeline1;
import com.anaptecs.jeaf.workload.test.excluded.DefaultPipeline2;
import com.anaptecs.jeaf.workload.test.excluded.ElasticPipeline;
import com.anaptecs.jeaf.workload.test.excluded.InvalidPipelineRef;
import com.anaptecs.jeaf.workload.test.excluded.InvalidWorkloadMapping;
import com.anaptecs.jeaf.workload.test.excluded.SomeElasticWorkloadMapping;
import com.anaptecs.jeaf.workload.test.excluded.SomeStaticWorkloadMapping;
import com.anaptecs.jeaf.workload.test.excluded.StaticPipeline;
import com.anaptecs.jeaf.workload.test.execution.DefaultPipeline;
import com.anaptecs.jeaf.workload.test.execution.Pipeline1;
import com.anaptecs.jeaf.workload.test.execution.Pipeline2;
import com.anaptecs.jeaf.workload.test.execution.WorkloadMapping1;
import com.anaptecs.jeaf.workload.test.execution.WorkloadMapping2;
import com.anaptecs.jeaf.workload.test.execution.WorkloadMapping3;
import com.anaptecs.jeaf.workload.test.execution.WorkloadMapping4;
import com.anaptecs.jeaf.xfun.api.XFun;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;

@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class WorkloadManagerImplTest {
  @Test
  @Order(10)
  public void testWorkloadManagerLoading( ) {
    // Good cases
    WorkloadManager lWorkloadManager = Workload.getWorkloadManager();
    assertEquals(WorkloadManagerImpl.class, lWorkloadManager.getClass());
    WorkloadManagerImpl lWorkloadManagerImpl = (WorkloadManagerImpl) lWorkloadManager;
    PipelineInfo lDefaultPipelineInfo = lWorkloadManagerImpl.getDefaultPipeline();
    assertNotNull(lDefaultPipelineInfo, "Default pipeline missing");
    assertEquals(lDefaultPipelineInfo.getName(), "FIFO-Pipeline");
    assertEquals(lDefaultPipelineInfo.getDescription(), "FIFO-Pipeline is used as default for requests");
    assertEquals(1, lWorkloadManagerImpl.getPipelines().size());

    // Check workload mappings.
    Map<RequestTypeKey, PipelineInfo> lWorkloadMappings = lWorkloadManagerImpl.getWorkloadMappings();
    assertNotNull(lWorkloadMappings, "Workload mappings missing.");
    assertEquals(1, lWorkloadMappings.size());
    RESTRequestTypeKey lRequestTypeKey = new RESTRequestTypeKey("api/v2", null);
    PipelineInfo lThreadPoolInfo = lWorkloadMappings.get(lRequestTypeKey);
    assertNotNull(lThreadPoolInfo, "Workload mapping missing");

    XFun.getTrace().info("Workload-Mappings: " + lWorkloadMappings);

    // Additional positive cases.
    List<Class<?>> lPipelineClasses = new ArrayList<>();
    lPipelineClasses.add(StaticPipeline.class);
    lPipelineClasses.add(com.anaptecs.jeaf.workload.test.excluded.ElasticPipeline.class);
    List<Class<?>> lStaticMappingClasses = new ArrayList<>();
    lStaticMappingClasses.add(SomeStaticWorkloadMapping.class);
    List<Class<?>> lElasticMappingClasses = new ArrayList<>();
    lElasticMappingClasses.add(SomeElasticWorkloadMapping.class);

    // Test loading of pipeline without default pipeline.
    lWorkloadManagerImpl = new WorkloadManagerImpl(lPipelineClasses, lStaticMappingClasses, lElasticMappingClasses);
    lDefaultPipelineInfo = lWorkloadManagerImpl.getDefaultPipeline();
    assertNull(lDefaultPipelineInfo);
    assertEquals(2, lWorkloadManagerImpl.getPipelines().size());
    PipelineInfo lPipeline = lWorkloadManagerImpl.getPipelines().get(0);

    lWorkloadMappings = lWorkloadManagerImpl.getWorkloadMappings();
    XFun.getTrace().info(lWorkloadMappings.toString());
    assertEquals(8, lWorkloadMappings.size());

    lPipeline = lWorkloadMappings.get(new RESTRequestTypeKey("api/v3/static", null));
    assertEquals(StaticPipeline.class.getName(), lPipeline.getPipelineID());
    lPipeline = lWorkloadMappings.get(new RESTRequestTypeKey("api/v3/elastic", "POST"));
    assertEquals(ElasticPipeline.class.getName(), lPipeline.getPipelineID());
    lPipeline = lWorkloadMappings.get(new RESTRequestTypeKey("api/v3/elastic", "GET"));
    assertEquals(ElasticPipeline.class.getName(), lPipeline.getPipelineID());
    lPipeline = lWorkloadMappings.get(new RESTRequestTypeKey("api/v3/veryelastic", "POST"));
    assertEquals(ElasticPipeline.class.getName(), lPipeline.getPipelineID());
    lPipeline = lWorkloadMappings.get(new RESTRequestTypeKey("api/v3/veryelastic", "DELETE"));
    assertEquals(ElasticPipeline.class.getName(), lPipeline.getPipelineID());
    lPipeline = lWorkloadMappings.get(new RESTRequestTypeKey("api/v3/veryelastic", "PUT"));
    assertEquals(ElasticPipeline.class.getName(), lPipeline.getPipelineID());
    lPipeline = lWorkloadMappings.get(new RESTRequestTypeKey("api/v3/veryelastic", "GET"));
    assertEquals(ElasticPipeline.class.getName(), lPipeline.getPipelineID());
    lPipeline = lWorkloadMappings.get(new RESTRequestTypeKey("api/v3/notsoelastic", null));
    assertEquals(ElasticPipeline.class.getName(), lPipeline.getPipelineID());

    // Test null handling
    lWorkloadManagerImpl = new WorkloadManagerImpl(lPipelineClasses, lStaticMappingClasses, null);
    assertEquals(1, lWorkloadManagerImpl.getPipelines().size());
    lPipeline = lWorkloadManagerImpl.getPipelines().get(0);
    assertEquals(StaticPipeline.class.getName(), lPipeline.getPipelineID());
    lWorkloadMappings = lWorkloadManagerImpl.getWorkloadMappings();
    assertEquals(1, lWorkloadMappings.size());
    lPipeline = lWorkloadMappings.get(new RESTRequestTypeKey("api/v3/static", null));
    assertEquals(StaticPipeline.class.getName(), lPipeline.getPipelineID());

    lWorkloadManagerImpl = new WorkloadManagerImpl(lPipelineClasses, null, lElasticMappingClasses);
    lDefaultPipelineInfo = lWorkloadManagerImpl.getDefaultPipeline();
    assertNull(lDefaultPipelineInfo);
    assertEquals(1, lWorkloadManagerImpl.getPipelines().size());
    lPipeline = lWorkloadManagerImpl.getPipelines().get(0);
    assertEquals(com.anaptecs.jeaf.workload.test.excluded.ElasticPipeline.class.getName(), lPipeline.getPipelineID());
    lWorkloadMappings = lWorkloadManagerImpl.getWorkloadMappings();
    assertEquals(7, lWorkloadMappings.size());
    lPipeline = lWorkloadMappings.get(new RESTRequestTypeKey("api/v3/elastic", null));
    assertNull(lPipeline);

    // Test handling of configuration errors

    // No workload mappings
    try {
      new WorkloadManagerImpl(lPipelineClasses, null, null);
      fail("Exception expected.");
    }
    catch (WorkloadSystemException e) {
      assertEquals(WorkloadMessages.NO_WORKLOAD_MAPPINGS_AVAILABLE, e.getErrorCode());
    }

    // Invalid workload mapping.
    try {
      List<Class<?>> lInvalidRef = new ArrayList<>();
      lInvalidRef.add(InvalidPipelineRef.class);
      new WorkloadManagerImpl(lPipelineClasses, lInvalidRef, null);
      fail("Exception expected.");
    }
    catch (WorkloadSystemException e) {
      assertEquals(WorkloadMessages.NOT_EXISTING_PIPELINE_CONFIGURED, e.getErrorCode());
    }

    try {
      List<Class<?>> lInvalidMapping = new ArrayList<>();
      lInvalidMapping.add(InvalidWorkloadMapping.class);
      new WorkloadManagerImpl(lPipelineClasses, null, lInvalidMapping);
      fail("Exception expected.");
    }
    catch (WorkloadSystemException e) {
      assertEquals(WorkloadMessages.NO_WORKLOAD_MAPPINGS_AVAILABLE, e.getErrorCode());
    }

    // No pipelines.
    try {
      new WorkloadManagerImpl(null, null, null);
      fail("Exception expected.");
    }
    catch (WorkloadSystemException e) {
      assertEquals(WorkloadMessages.NO_PIPELINES_CONFIGURED, e.getErrorCode());
    }

    try {
      lPipelineClasses.clear();
      lPipelineClasses.add(String.class);
      new WorkloadManagerImpl(lPipelineClasses, null, null);
      fail("Exception expected.");
    }
    catch (WorkloadSystemException e) {
      assertEquals(WorkloadMessages.NO_PIPELINES_CONFIGURED, e.getErrorCode());
    }

    // Multiple default pipelines.
    try {
      lPipelineClasses.clear();
      lPipelineClasses.add(DefaultPipeline1.class);
      lPipelineClasses.add(DefaultPipeline2.class);
      new WorkloadManagerImpl(lPipelineClasses, null, null);
      fail("Exception expected.");
    }
    catch (WorkloadSystemException e) {
      assertEquals(WorkloadMessages.MULTIPLE_DEFAULT_PIPELINES, e.getErrorCode());
    }

    // Test default pipeline only.
    lPipelineClasses.clear();
    lPipelineClasses.add(DefaultPipeline1.class);
    lWorkloadManagerImpl = new WorkloadManagerImpl(lPipelineClasses, null, null);
    assertEquals(1, lWorkloadManagerImpl.getPipelines().size());
    assertNotNull(lWorkloadManagerImpl.getDefaultPipeline());
  }

  @Test
  @Order(20)
  public void testWorkloadManagerLoadingFromYAML( ) throws Exception {
    // Try to create workload manager with no configuration at all.
    try {
      new WorkloadManagerImpl(null, null, null);
    }
    catch (WorkloadSystemException e) {
      assertEquals(WorkloadMessages.NO_PIPELINES_CONFIGURED, e.getErrorCode());
    }

    // Load Workload manager with YAML configuration only.
    try {
      System.setProperty(WorkloadManagerImpl.YAML_CONFIG_PROPERTY_NAME,
          "./src/test/resources/PipelineConfiguration.yaml");
      WorkloadManagerImpl lWorkloadManager = new WorkloadManagerImpl(null, null, null);
      PipelineInfo lDefaultPipeline = lWorkloadManager.getDefaultPipeline();
      assertNotNull(lDefaultPipeline);
      assertEquals("Pipeline1", lDefaultPipeline.getPipelineID());

      List<PipelineInfo> lPipelines = lWorkloadManager.getPipelines();
      assertEquals(2, lPipelines.size());

      PipelineInfo lMinimalPipeline;
      if (lPipelines.get(0).getPipelineID().equals(lDefaultPipeline.getPipelineID())) {
        lMinimalPipeline = lPipelines.get(1);
      }
      else {
        lMinimalPipeline = lPipelines.get(0);
      }
      assertEquals("MinimalPipeline", lMinimalPipeline.getPipelineID());

      RequestType lRequestType = new RESTRequestType("api/v3/xyz", "POST");
      WorkloadErrorHandlerImpl lErrorHandler = new WorkloadErrorHandlerImpl();
      int lExecutionTime = 50;
      PipelineRunnable lRunnable = new PipelineRunnable(lExecutionTime);
      lWorkloadManager.execute(lRequestType, lErrorHandler, lRunnable);
      Thread.sleep(lExecutionTime * 2);

      assertTrue(lErrorHandler.noExceptions);
      assertTrue(lRunnable.executed);
      assertEquals(0, lDefaultPipeline.getCompletedTaskCount());
      assertEquals(1, lMinimalPipeline.getCompletedTaskCount());
    }
    finally {
      System.getProperties().remove(WorkloadManagerImpl.YAML_CONFIG_PROPERTY_NAME);
    }
  }

  @Test
  @Order(30)
  public void testWorkloadExecution( ) throws Exception {
    List<Class<?>> lPipelineClasses = new ArrayList<>();
    lPipelineClasses.add(Pipeline1.class);
    lPipelineClasses.add(Pipeline2.class);
    List<Class<?>> lWorkloadMappingClasses = new ArrayList<>();
    lWorkloadMappingClasses.add(WorkloadMapping1.class);
    lWorkloadMappingClasses.add(WorkloadMapping2.class);
    lWorkloadMappingClasses.add(WorkloadMapping3.class);
    lWorkloadMappingClasses.add(WorkloadMapping4.class);

    WorkloadManagerImpl lWorkloadManagerImpl = new WorkloadManagerImpl(lPipelineClasses, lWorkloadMappingClasses, null);
    assertNull(lWorkloadManagerImpl.getDefaultPipeline());
    PipelineInfo lPipeline1 = null;
    PipelineInfo lPipeline2 = null;
    for (PipelineInfo lNextPipeline : lWorkloadManagerImpl.getPipelines()) {
      if (Pipeline1.class.getName().equals(lNextPipeline.getPipelineID())) {
        lPipeline1 = lNextPipeline;
      }
      if (Pipeline2.class.getName().equals(lNextPipeline.getPipelineID())) {
        lPipeline2 = lNextPipeline;
      }
    }
    assertNotNull(lPipeline1);
    assertEquals(0, lPipeline1.getCompletedTaskCount());
    assertNotNull(lPipeline2);
    assertEquals(0, lPipeline2.getCompletedTaskCount());

    // Now let's execute our first request.
    RequestType lRequestType = new RESTRequestType("api/v1/xyz", "POST");
    WorkloadErrorHandlerImpl lErrorHandler = new WorkloadErrorHandlerImpl();
    int lExecutionTime = 50;
    PipelineRunnable lRunnable = new PipelineRunnable(lExecutionTime);
    lWorkloadManagerImpl.execute(lRequestType, lErrorHandler, lRunnable);
    Thread.sleep(lExecutionTime * 2);

    // Check execution result.
    assertTrue(lErrorHandler.noExceptions);
    assertTrue(lRunnable.executed);
    assertEquals(1, lPipeline1.getCompletedTaskCount());
    assertEquals(0, lPipeline2.getCompletedTaskCount());

    // Execute next runnable
    lRequestType = new RESTRequestType("api/v1", "PUT");
    lRunnable = new PipelineRunnable(lExecutionTime);
    lWorkloadManagerImpl.execute(lRequestType, lErrorHandler, lRunnable);
    Thread.sleep(lExecutionTime * 2);

    // Check execution result.
    assertTrue(lErrorHandler.noExceptions);
    assertTrue(lRunnable.executed);
    assertEquals(2, lPipeline1.getCompletedTaskCount());
    assertEquals(0, lPipeline2.getCompletedTaskCount());

    // Execute next runnable through other pipeline
    lRequestType = new RESTRequestType("api/v2", "GET");
    lRunnable = new PipelineRunnable(lExecutionTime);
    lWorkloadManagerImpl.execute(lRequestType, lErrorHandler, lRunnable);
    Thread.sleep(lExecutionTime * 2);

    // Check execution result.
    assertTrue(lErrorHandler.noExceptions);
    assertTrue(lRunnable.executed);
    assertEquals(2, lPipeline1.getCompletedTaskCount());
    assertEquals(1, lPipeline2.getCompletedTaskCount());

    // Check error handling
    try {
      lRequestType = new RESTRequestType("api/v4711", "GET");
      lRunnable = new PipelineRunnable(lExecutionTime);
      lWorkloadManagerImpl.execute(lRequestType, lErrorHandler, lRunnable);
      fail("Exception expected.");
    }
    catch (WorkloadSystemException e) {
      assertEquals(WorkloadMessages.NO_PIPELINE_FOUND, e.getErrorCode());
    }

    // Check execution result.
    assertTrue(lErrorHandler.noExceptions);
    assertFalse(lRunnable.executed);
    assertEquals(2, lPipeline1.getCompletedTaskCount());
    assertEquals(1, lPipeline2.getCompletedTaskCount());

    // Check fallback to default pipeline.
    lPipelineClasses.add(DefaultPipeline.class);
    lWorkloadManagerImpl = new WorkloadManagerImpl(lPipelineClasses, lWorkloadMappingClasses, null);

    PipelineInfo lDefaultPipeline = null;
    for (PipelineInfo lNextPipeline : lWorkloadManagerImpl.getPipelines()) {
      if (Pipeline1.class.getName().equals(lNextPipeline.getPipelineID())) {
        lPipeline1 = lNextPipeline;
      }
      if (Pipeline2.class.getName().equals(lNextPipeline.getPipelineID())) {
        lPipeline2 = lNextPipeline;
      }
      if (DefaultPipeline.class.getName().equals(lNextPipeline.getPipelineID())) {
        lDefaultPipeline = lNextPipeline;
      }
    }
    assertNotNull(lPipeline1);
    assertEquals(0, lPipeline1.getCompletedTaskCount());
    assertNotNull(lPipeline2);
    assertEquals(0, lPipeline2.getCompletedTaskCount());
    assertNotNull(lDefaultPipeline);
    assertEquals(0, lDefaultPipeline.getCompletedTaskCount());

    // Try to execute call with no pipeline once again. This time it should haven been execute by the default pipeline.
    lRequestType = new RESTRequestType("api/v4711", "GET");
    lRunnable = new PipelineRunnable(lExecutionTime);
    lWorkloadManagerImpl.execute(lRequestType, lErrorHandler, lRunnable);
    Thread.sleep(lExecutionTime * 2);

    // Check status of pipelines.
    assertEquals(0, lPipeline1.getCompletedTaskCount());
    assertEquals(0, lPipeline2.getCompletedTaskCount());
    assertEquals(1, lDefaultPipeline.getCompletedTaskCount());
    assertTrue(lErrorHandler.noExceptions);
    assertTrue(lRunnable.executed);

    // Execute same task again.
    lRunnable = new PipelineRunnable(lExecutionTime);
    lWorkloadManagerImpl.execute(lRequestType, lErrorHandler, lRunnable);
    Thread.sleep(lExecutionTime * 2);

    // Check status of pipelines.
    assertEquals(0, lPipeline1.getCompletedTaskCount());
    assertEquals(0, lPipeline2.getCompletedTaskCount());
    assertEquals(2, lDefaultPipeline.getCompletedTaskCount());
    assertTrue(lErrorHandler.noExceptions);
    assertTrue(lRunnable.executed);

    // Ensure that default pipeline is not always used.
    lRequestType = new RESTRequestType("api/v2", "GET");
    lRunnable = new PipelineRunnable(lExecutionTime);
    lWorkloadManagerImpl.execute(lRequestType, lErrorHandler, lRunnable);
    Thread.sleep(lExecutionTime * 2);

    // Check execution result.
    assertTrue(lErrorHandler.noExceptions);
    assertTrue(lRunnable.executed);
    assertEquals(0, lPipeline1.getCompletedTaskCount());
    assertEquals(1, lPipeline2.getCompletedTaskCount());
    assertEquals(2, lDefaultPipeline.getCompletedTaskCount());

    lRunnable = new PipelineRunnable(lExecutionTime);
    lWorkloadManagerImpl.execute(lRequestType, lErrorHandler, lRunnable);
    Thread.sleep(lExecutionTime * 2);

    // Check execution result.
    assertTrue(lErrorHandler.noExceptions);
    assertTrue(lRunnable.executed);
    assertEquals(0, lPipeline1.getCompletedTaskCount());
    assertEquals(2, lPipeline2.getCompletedTaskCount());
    assertEquals(2, lDefaultPipeline.getCompletedTaskCount());
  }

}
