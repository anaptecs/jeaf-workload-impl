/**
 * Copyright 2004 - 2020 anaptecs GmbH, Burgstr. 96, 72764 Reutlingen, Germany
 *
 * All rights reserved.
 */
package com.anaptecs.jeaf.workload.impl.yaml;

import java.lang.annotation.Annotation;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import com.anaptecs.jeaf.workload.annotations.PipelineConfig;
import com.anaptecs.jeaf.workload.annotations.QueueType;
import com.anaptecs.jeaf.workload.api.RequestTypeKey;

public class PipelineConfigImpl {
  /**
   * ID of the pipeline
   */
  private String id;

  /**
   * {@link PipelineConfig#name()}
   */
  private String name;

  /**
   * {@link PipelineConfig#description()}
   */
  private String description;

  /**
   * {@link PipelineConfig#coreThreads()}
   */
  private int coreThreads;

  /**
   * {@link PipelineConfig#maxThreads()}
   */
  private int maxThreads;

  /**
   * {@link PipelineConfig#threadPriority()}
   */
  private int threadPriority;

  /**
   * {@link PipelineConfig#maxThreadKeepAlive()}
   */
  private int maxThreadKeepAlive;

  /**
   * {@link PipelineConfig#queueType()}
   */
  private QueueType queueType;

  /**
   * {@link PipelineConfig#maxQueueDepth()}
   */
  private int maxQueueDepth;

  /**
   * {@link PipelineConfig#maxLatency()}
   */
  private int maxLatency;

  /**
   * {@link PipelineConfig#timeUnit()}
   */
  private TimeUnit timeUnit;

  /**
   * {@link PipelineConfig#defaultPipeline()}
   */
  private boolean defaultPipeline;

  /**
   * List with all REST based request type key that should be processed by this pipeline.
   */
  private List<RESTRequestTypeKey> restKeys = new ArrayList<>();

  /**
   * List with all generic request types that should be processed by this pipeline.
   */
  private List<GenericRequestTypeKey> genericKeys = new ArrayList<>();

  /**
   * Initialize object. Therefore same default values are used as by annotation {@link PipelineConfig}
   */
  public PipelineConfigImpl( ) {
    // id and coreThreads are mandatory and do not have no a default value.
    name = "";
    description = "";
    maxThreads = -1;
    threadPriority = Thread.NORM_PRIORITY;
    maxThreadKeepAlive = 60000;
    queueType = QueueType.FIFO;
    maxQueueDepth = 50;
    maxLatency = -1;
    timeUnit = TimeUnit.MILLISECONDS;
    defaultPipeline = false;
  }

  public String getId( ) {
    return id;
  }

  public void setId( String pId ) {
    id = pId;
  }

  public String getName( ) {
    return name;
  }

  public void setName( String pName ) {
    name = pName;
  }

  public String getDescription( ) {
    return description;
  }

  public void setDescription( String pDescription ) {
    description = pDescription;
  }

  public int getCoreThreads( ) {
    return coreThreads;
  }

  public void setCoreThreads( int pCoreThreads ) {
    coreThreads = pCoreThreads;
  }

  public int getMaxThreads( ) {
    return maxThreads;
  }

  public void setMaxThreads( int pMaxThreads ) {
    maxThreads = pMaxThreads;
  }

  public int getThreadPriority( ) {
    return threadPriority;
  }

  public void setThreadPriority( int pThreadPriority ) {
    threadPriority = pThreadPriority;
  }

  public int getMaxThreadKeepAlive( ) {
    return maxThreadKeepAlive;
  }

  public void setMaxThreadKeepAlive( int pMaxThreadKeepAlive ) {
    maxThreadKeepAlive = pMaxThreadKeepAlive;
  }

  public QueueType getQueueType( ) {
    return queueType;
  }

  public void setQueueType( QueueType pQueueType ) {
    queueType = pQueueType;
  }

  public int getMaxQueueDepth( ) {
    return maxQueueDepth;
  }

  public void setMaxQueueDepth( int pMaxQueueDepth ) {
    maxQueueDepth = pMaxQueueDepth;
  }

  public int getMaxLatency( ) {
    return maxLatency;
  }

  public void setMaxLatency( int pMaxLatency ) {
    maxLatency = pMaxLatency;
  }

  public TimeUnit getTimeUnit( ) {
    return timeUnit;
  }

  public void setTimeUnit( TimeUnit pTimeUnit ) {
    timeUnit = pTimeUnit;
  }

  public boolean isDefaultPipeline( ) {
    return defaultPipeline;
  }

  public void setDefaultPipeline( boolean pDefaultPipeline ) {
    defaultPipeline = pDefaultPipeline;
  }

  public List<RESTRequestTypeKey> getRestKeys( ) {
    return restKeys;
  }

  public void setRestKeys( List<RESTRequestTypeKey> pRestKeys ) {
    restKeys = pRestKeys;
  }

  public void addRestKey( RESTRequestTypeKey pRestKey ) {
    restKeys.add(pRestKey);
  }

  public List<GenericRequestTypeKey> getGenericKeys( ) {
    return genericKeys;
  }

  public void setGenericKeys( List<GenericRequestTypeKey> pSoapKeys ) {
    genericKeys = pSoapKeys;
  }

  public void addGenericKey( GenericRequestTypeKey pGenericKey ) {
    genericKeys.add(pGenericKey);
  }

  /**
   * Method returns a list with all request type keys that should be processed by this pipeline.
   * 
   * @return {@link List} List with all request type keys that should be processed by this pipeline. The method never
   * returns null.
   */
  public List<RequestTypeKey> getRequestTypeKeys( ) {
    List<RequestTypeKey> lKeys = new ArrayList<>(genericKeys.size() + restKeys.size());

    // Process all generic keys.
    for (GenericRequestTypeKey lNextKey : genericKeys) {
      com.anaptecs.jeaf.workload.api.GenericRequestTypeKey lNewKey =
          new com.anaptecs.jeaf.workload.api.GenericRequestTypeKey(lNextKey.getKey());
      lKeys.add(lNewKey);
    }

    // Process all REST keys
    for (RESTRequestTypeKey lNextKey : restKeys) {
      com.anaptecs.jeaf.workload.api.rest.RESTRequestTypeKey lNewKey =
          new com.anaptecs.jeaf.workload.api.rest.RESTRequestTypeKey(lNextKey.getEndpointURL(),
              lNextKey.getHttpMethod());
      lKeys.add(lNewKey);
    }

    // Return list with all request type keys that should be processed by this pipeline.
    return lKeys;
  }

  /**
   * Method returns all the configuration parameters of this pipeline as {@link PipelineConfig} object.
   * 
   * @return {@link PipelineConfig} All configuration parameters of the pipeline. The method never returns null.
   */
  public PipelineConfig getPipelineConfig( ) {
    return new PipelineConfig() {

      @Override
      public Class<? extends Annotation> annotationType( ) {
        return PipelineConfig.class;
      }

      @Override
      public String name( ) {
        return name;
      }

      @Override
      public String description( ) {
        return description;
      }

      @Override
      public int coreThreads( ) {
        return coreThreads;
      }

      @Override
      public int maxThreads( ) {
        return maxThreads;
      }

      @Override
      public int threadPriority( ) {
        return threadPriority;
      }

      @Override
      public int maxThreadKeepAlive( ) {
        return maxThreadKeepAlive;
      }

      @Override
      public QueueType queueType( ) {
        return queueType;
      }

      @Override
      public int maxQueueDepth( ) {
        return maxQueueDepth;
      }

      @Override
      public int maxLatency( ) {
        return maxLatency;
      }

      @Override
      public TimeUnit timeUnit( ) {
        return timeUnit;
      }

      @Override
      public boolean defaultPipeline( ) {
        return defaultPipeline;
      }
    };
  }
}
