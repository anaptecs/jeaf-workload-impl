/**
 * Copyright 2004 - 2020 anaptecs GmbH, Burgstr. 96, 72764 Reutlingen, Germany
 *
 * All rights reserved.
 */
package com.anaptecs.jeaf.workload.test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.concurrent.TimeUnit;

import com.anaptecs.jeaf.workload.annotations.PipelineConfig;
import com.anaptecs.jeaf.workload.annotations.QueueType;
import com.anaptecs.jeaf.workload.api.RequestTypeKey;
import com.anaptecs.jeaf.workload.api.WorkloadMessages;
import com.anaptecs.jeaf.workload.impl.yaml.GenericRequestTypeKey;
import com.anaptecs.jeaf.workload.impl.yaml.PipelineConfigImpl;
import com.anaptecs.jeaf.workload.impl.yaml.PipelineConfiguration;
import com.anaptecs.jeaf.workload.impl.yaml.RESTRequestTypeKey;
import com.anaptecs.jeaf.xfun.api.errorhandling.JEAFSystemException;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;

@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class YAMLPipelineConfigurationTest {
  @Test
  @Order(10)
  public void testYAMLPipelineConfiguration( ) {
    // Load pipelines.
    List<PipelineConfigImpl> lPipelines =
        PipelineConfiguration.loadPipelineConfigurations("./src/test/resources/PipelineConfiguration.yaml");

    assertEquals(2, lPipelines.size());

    // Test first pipeline
    PipelineConfigImpl lPipeline = lPipelines.get(0);

    // Check common properties
    assertEquals("Pipeline1", lPipeline.getId());
    assertEquals("Pipeline 1", lPipeline.getName());
    assertEquals("Pipeline for long running requests", lPipeline.getDescription());
    assertEquals(true, lPipeline.isDefaultPipeline());

    // Check thread pool settings
    assertEquals(5, lPipeline.getCoreThreads());
    assertEquals(10, lPipeline.getMaxThreads());
    assertEquals(22222, lPipeline.getMaxThreadKeepAlive());
    assertEquals(7, lPipeline.getThreadPriority());

    // Check queue settings
    assertEquals(QueueType.FIFO, lPipeline.getQueueType());
    assertEquals(37, lPipeline.getMaxQueueDepth());
    assertEquals(5000000, lPipeline.getMaxLatency());
    assertEquals(TimeUnit.NANOSECONDS, lPipeline.getTimeUnit());

    // Check request type keys
    List<GenericRequestTypeKey> lGenericKeys = lPipeline.getGenericKeys();
    assertEquals(1, lGenericKeys.size());
    GenericRequestTypeKey lGenericKey = lGenericKeys.get(0);
    assertEquals("LongRunningRequests", lGenericKey.getKey());

    // Check REST request types
    List<RESTRequestTypeKey> lRESTKeys = lPipeline.getRestKeys();
    assertEquals(1, lRESTKeys.size());
    assertEquals("api/v4/resource", lRESTKeys.get(0).getEndpointURL());
    assertEquals("POST", lRESTKeys.get(0).getHttpMethod());

    List<RequestTypeKey> lAllKeys = lPipeline.getRequestTypeKeys();
    assertEquals(2, lAllKeys.size());
    assertEquals("LongRunningRequests", lAllKeys.get(0).getKey());
    assertEquals("api/v4/resource (POST)", lAllKeys.get(1).getKey());

    PipelineConfig lPipelineConfig = lPipeline.getPipelineConfig();
    assertEquals(PipelineConfig.class, lPipelineConfig.annotationType());
    assertEquals("Pipeline 1", lPipelineConfig.name());
    assertEquals("Pipeline for long running requests", lPipelineConfig.description());
    assertEquals(true, lPipelineConfig.defaultPipeline());
    assertEquals(5, lPipelineConfig.coreThreads());
    assertEquals(10, lPipelineConfig.maxThreads());
    assertEquals(22222, lPipelineConfig.maxThreadKeepAlive());
    assertEquals(7, lPipelineConfig.threadPriority());
    assertEquals(QueueType.FIFO, lPipelineConfig.queueType());
    assertEquals(37, lPipelineConfig.maxQueueDepth());
    assertEquals(5000000, lPipelineConfig.maxLatency());
    assertEquals(TimeUnit.NANOSECONDS, lPipelineConfig.timeUnit());
  }

  @Test
  @Order(20)
  public void testMinimumPipeline( ) {
    // Load pipelines.
    List<PipelineConfigImpl> lPipelines =
        PipelineConfiguration.loadPipelineConfigurations("./src/test/resources/PipelineConfiguration.yaml");

    assertEquals(2, lPipelines.size());

    // Test second pipeline with minimum configuration
    PipelineConfigImpl lMinimalPipeline = lPipelines.get(1);

    // Check common properties
    assertEquals("MinimalPipeline", lMinimalPipeline.getId());
    assertEquals("", lMinimalPipeline.getName());
    assertEquals("", lMinimalPipeline.getDescription());
    assertEquals(false, lMinimalPipeline.isDefaultPipeline());

    // Check thread pool settings
    assertEquals(10, lMinimalPipeline.getCoreThreads());
    assertEquals(-1, lMinimalPipeline.getMaxThreads());
    assertEquals(60000, lMinimalPipeline.getMaxThreadKeepAlive());
    assertEquals(5, lMinimalPipeline.getThreadPriority());

    // Check queue settings
    assertEquals(QueueType.FIFO, lMinimalPipeline.getQueueType());
    assertEquals(50, lMinimalPipeline.getMaxQueueDepth());
    assertEquals(-1, lMinimalPipeline.getMaxLatency());
    assertEquals(TimeUnit.MILLISECONDS, lMinimalPipeline.getTimeUnit());

    // Check request type keys
    List<GenericRequestTypeKey> lGenericKeys = lMinimalPipeline.getGenericKeys();
    assertEquals(0, lGenericKeys.size());

    // Check REST request types
    List<RESTRequestTypeKey> lRESTKeys = lMinimalPipeline.getRestKeys();
    assertEquals(1, lRESTKeys.size());

    PipelineConfig lPipelineConfig = lMinimalPipeline.getPipelineConfig();
    assertEquals(PipelineConfig.class, lPipelineConfig.annotationType());
    assertEquals("", lPipelineConfig.name());
    assertEquals("", lPipelineConfig.description());
    assertEquals(false, lPipelineConfig.defaultPipeline());
    assertEquals(10, lPipelineConfig.coreThreads());
    assertEquals(-1, lPipelineConfig.maxThreads());
    assertEquals(60000, lPipelineConfig.maxThreadKeepAlive());
    assertEquals(5, lPipelineConfig.threadPriority());
    assertEquals(QueueType.FIFO, lPipelineConfig.queueType());
    assertEquals(50, lPipelineConfig.maxQueueDepth());
    assertEquals(-1, lPipelineConfig.maxLatency());
    assertEquals(TimeUnit.MILLISECONDS, lPipelineConfig.timeUnit());

  }

  @Test
  @Order(30)
  public void testYAMLObjects( ) {
    PipelineConfigImpl lPipelineConfigImpl = new PipelineConfigImpl();
    GenericRequestTypeKey lGenericKey = new GenericRequestTypeKey();
    lPipelineConfigImpl.addGenericKey(lGenericKey);
    List<GenericRequestTypeKey> lGenericKeys = lPipelineConfigImpl.getGenericKeys();
    assertEquals(1, lGenericKeys.size());
    assertTrue(lGenericKeys.contains(lGenericKey));

    RESTRequestTypeKey lRESTKey = new RESTRequestTypeKey();
    lPipelineConfigImpl.addRestKey(lRESTKey);
    List<RESTRequestTypeKey> lRESTKeys = lPipelineConfigImpl.getRestKeys();
    assertEquals(1, lRESTKeys.size());
    assertTrue(lRESTKeys.contains(lRESTKey));

    PipelineConfiguration lPipelineConfiguration = new PipelineConfiguration();
    lPipelineConfiguration.addPipeline(lPipelineConfigImpl);
    List<PipelineConfigImpl> lPipelines = lPipelineConfiguration.getPipelines();
    assertEquals(1, lPipelines.size());
    assertTrue(lPipelines.contains(lPipelineConfigImpl));
  }

  @Test
  @Order(40)
  public void testErrorHanldingOnLoading( ) {
    try {
      PipelineConfiguration.loadPipelineConfigurations(null);
    }
    catch (IllegalArgumentException e) {
      assertEquals("Check failed. pFileName must not be NULL.", e.getMessage());
    }

    // Try to load configuration from a file that does not exist.
    try {
      PipelineConfiguration.loadPipelineConfigurations("./src/test/resources/MissingFile.yaml");
    }
    catch (JEAFSystemException e) {
      assertEquals(WorkloadMessages.UNABLE_TO_LOAD_YAML_CONFIG, e.getErrorCode());
    }
  }
}
