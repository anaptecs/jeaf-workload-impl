/**
 * Copyright 2004 - 2020 anaptecs GmbH, Burgstr. 96, 72764 Reutlingen, Germany
 *
 * All rights reserved.
 */
package com.anaptecs.jeaf.workload.impl;

import java.lang.management.ManagementFactory;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import javax.management.JMException;
import javax.management.MBeanServer;
import javax.management.ObjectName;

import com.anaptecs.jeaf.tools.api.Tools;
import com.anaptecs.jeaf.tools.api.reflect.ReflectionTools;
import com.anaptecs.jeaf.workload.annotations.ElasticWorkloadConfig;
import com.anaptecs.jeaf.workload.annotations.EndpointsGroup;
import com.anaptecs.jeaf.workload.annotations.HTTPMethod;
import com.anaptecs.jeaf.workload.annotations.PipelineConfig;
import com.anaptecs.jeaf.workload.annotations.Resource;
import com.anaptecs.jeaf.workload.annotations.StaticWorkloadConfig;
import com.anaptecs.jeaf.workload.annotations.WorkloadMapping;
import com.anaptecs.jeaf.workload.api.RequestType;
import com.anaptecs.jeaf.workload.api.RequestTypeKey;
import com.anaptecs.jeaf.workload.api.Workload;
import com.anaptecs.jeaf.workload.api.WorkloadErrorHandler;
import com.anaptecs.jeaf.workload.api.WorkloadManager;
import com.anaptecs.jeaf.workload.api.WorkloadMessages;
import com.anaptecs.jeaf.workload.api.WorkloadSystemException;
import com.anaptecs.jeaf.workload.api.rest.RESTRequestTypeKey;
import com.anaptecs.jeaf.workload.impl.yaml.PipelineConfigImpl;
import com.anaptecs.jeaf.workload.impl.yaml.PipelineConfiguration;
import com.anaptecs.jeaf.xfun.api.XFun;
import com.anaptecs.jeaf.xfun.api.checks.Assert;
import com.anaptecs.jeaf.xfun.api.config.Configuration;
import com.anaptecs.jeaf.xfun.api.config.ConfigurationReader;
import com.anaptecs.jeaf.xfun.api.errorhandling.JEAFSystemException;

/**
 * Class implements a workload manager that can be used to dispatch and execute requests. Using a workload manager it
 * can be ensured that a system will not be overloaded as the workload manager is able to queue requests.
 * 
 * This implementation is able to work with multiple pipelines (thread pools). Which pipeline will be used for a certain
 * request depends on the so called workload mapping. The workload mapping can be defined using annotation
 * {@link WorkloadMapping} and defines for which endpoints which pipelines should be used.
 * 
 * @author JEAF Development Team
 */
public class WorkloadManagerImpl implements WorkloadManager {
  /**
   * Name of the system property that can be used to define to location of a YAML file that contains additional pipeline
   * configurations.
   */
  public static final String YAML_CONFIG_PROPERTY_NAME = "jeaf.workload.config.yaml";

  /**
   * Map contains all request type keys and their associated pipelines.
   */
  private Map<RequestTypeKey, Pipeline> workloadMapping = new HashMap<>();

  /**
   * Pipeline is used in cases where no other pipeline could be resolved to handle a certain request. Depending on the
   * used configuration it may also be the case that no default pipeline is available.
   */
  private Pipeline defaultPipeline;

  /**
   * Initialize object. Therefore the configured pipelines will be created as well as the workload configurations are
   * applied.
   */
  public WorkloadManagerImpl( ) {
    // Resolve pipeline, static and elastic workload configuration.
    Map<Class<?>, PipelineConfig> lPipelineConfigurations = this.getPipelineConfigurationClasses();
    Map<Class<?>, StaticWorkloadConfig> lStaticWorkloadClasses = this.getStaticWorkloadConfigurationClasses();
    Map<Class<?>, ElasticWorkloadConfig> lElasticWorkloadClasses = this.getElasticWorkloadConfigurationClasses();

    // Create pipelines and initialize workload mappings.
    this.initialize(lPipelineConfigurations, lStaticWorkloadClasses, lElasticWorkloadClasses);
  }

  /**
   * Initialize object.
   * 
   * @param pPipelineConfigs List with classes that contain the pipeline configuration. The parameter must not be null
   * and there must be at least one class inside the list that has annotation {@link PipelineConfig}.
   * @param pStaticWorkloads List with classes that contain static workload configuration. The parameter must not be
   * null and there must be at least one class inside that has annotation {@link StaticWorkloadConfig}.
   * @param pElasticWorkloads List with classes that contain elastic workload configuration. The parameter must not be
   * null and there must be at least one class inside that has annotation {@link StaticWorkloadConfig}.
   */
  public WorkloadManagerImpl( List<Class<?>> pPipelineConfigs, List<Class<?>> pStaticWorkloads,
      List<Class<?>> pElasticWorkloads ) {

    // Create pipelines map.
    ReflectionTools lReflectionTools = Tools.getReflectionTools();
    Map<Class<?>, PipelineConfig> lPipelineConfigurations;
    if (pPipelineConfigs != null) {
      lPipelineConfigurations = lReflectionTools.getAnnotations(pPipelineConfigs, PipelineConfig.class);
    }
    else {
      lPipelineConfigurations = null;
    }

    // Resolve static workloads
    Map<Class<?>, StaticWorkloadConfig> lStaticWorkloadClasses;
    if (pStaticWorkloads != null) {
      lStaticWorkloadClasses = lReflectionTools.getAnnotations(pStaticWorkloads, StaticWorkloadConfig.class);
    }
    else {
      lStaticWorkloadClasses = null;
    }

    // Resolve elastic workloads
    Map<Class<?>, ElasticWorkloadConfig> lElasticWorkloadClasses;
    if (pElasticWorkloads != null) {
      lElasticWorkloadClasses = lReflectionTools.getAnnotations(pElasticWorkloads, ElasticWorkloadConfig.class);
    }
    else {
      lElasticWorkloadClasses = null;
    }

    // Create pipelines and initialize workload mappings.
    this.initialize(lPipelineConfigurations, lStaticWorkloadClasses, lElasticWorkloadClasses);
  }

  /**
   * Method initializes the workload manager. This means that all configured pipelines will be created and all workload
   * mappings will be applied.
   * 
   * @param pPipelineConfigurations Map with all pipeline configurations that should be used by the workload manager.
   * The parameter may be null.
   * @param pStaticWorkloadClasses Map with all static workload configuration classes that should be used by the
   * workload manager. The parameter must not be null.
   * @param pElasticWorkloadClasses Map with all elastic workload configuration classes that should be used by the
   * workload manager. The parameter must not be null.
   */
  private void initialize( Map<Class<?>, PipelineConfig> pPipelineConfigurations,
      Map<Class<?>, StaticWorkloadConfig> pStaticWorkloadClasses,
      Map<Class<?>, ElasticWorkloadConfig> pElasticWorkloadClasses ) {

    // Create pipelines based on annotations
    if (pPipelineConfigurations != null) {
      Map<String, Pipeline> lPipelines = this.createPipelines(pPipelineConfigurations);

      // Load and apply workload mappings.
      if (lPipelines.size() > 0) {
        // Static mappings
        if (pStaticWorkloadClasses != null) {
          Map<Class<?>, WorkloadMapping> lStaticWorkloadMappings =
              this.loadStaticWorkloadMappings(pStaticWorkloadClasses);
          this.applyWorkloadMappings(lPipelines, lStaticWorkloadMappings);
        }

        // Elastic mappings
        if (pElasticWorkloadClasses != null) {
          Map<Class<?>, WorkloadMapping> lElasticWorkloadMappings =
              this.loadElasticWorkloadMappings(pElasticWorkloadClasses);
          this.applyWorkloadMappings(lPipelines, lElasticWorkloadMappings);
        }

        // Ensure that at least 1 mapping is available.
        if (workloadMapping.size() == 0 && defaultPipeline == null) {
          throw new WorkloadSystemException(WorkloadMessages.NO_WORKLOAD_MAPPINGS_AVAILABLE);
        }
      }
      // No pipelines were configured through annotations. May be they are configured through YAML file.
      else {
        // Nothing to do.
      }
    }
    // No pipelines were configured through annotations. May be they are configured through YAML file.
    else {
      // Nothing to do.
    }

    // Also load pipelines from YAML configuration.
    this.loadPipelinesFromYAML();

    // After everything is done, we have to check if the configuration is correct.
    this.checkPipelineConfiguration();
  }

  private void checkPipelineConfiguration( ) {
    // No pipelines were configured.
    if (workloadMapping.size() == 0 && defaultPipeline == null) {
      throw new WorkloadSystemException(WorkloadMessages.NO_PIPELINES_CONFIGURED);
    }
  }

  /**
   * Method returns all classes that contain pipeline configuration as annotation.
   * 
   * @return {@link Map} Map with all pipeline configuration classes. The method never returns null.
   */
  private Map<Class<?>, PipelineConfig> getPipelineConfigurationClasses( ) {
    // Resolve configuration for all pipelines.
    ConfigurationReader lPipelineConfigReader = new ConfigurationReader();
    return lPipelineConfigReader.readAnnotationsMap(Workload.PIPELINES_RESOURCE_NAME, Workload.WORKLOAD_BASE_PATH,
        PipelineConfig.class);
  }

  /**
   * Method creates a map with all pipelines that are configured.
   * 
   * @return {@link Map} Map with all pipelines that were created. The map contains that class that was annotated with
   * the pipeline configuration as key and the pipeline as value.
   */
  private Map<String, Pipeline> createPipelines( Map<Class<?>, PipelineConfig> pPipelineConfigurations ) {
    // Check parameter
    Assert.assertNotNull(pPipelineConfigurations, "pPipelineConfigurations");

    // Use all configuration classes to create pipelines.
    Map<String, Pipeline> lPipelines = new HashMap<>();
    for (Entry<Class<?>, PipelineConfig> lNextEntry : pPipelineConfigurations.entrySet()) {
      // Create new pipeline.
      PipelineConfig lPipelinConfig = lNextEntry.getValue();
      Pipeline lPipeline = this.createPipeline(lNextEntry.getKey().getName(), lPipelinConfig);
      lPipelines.put(lPipeline.getPipelineID(), lPipeline);
    }
    return lPipelines;
  }

  private Pipeline createPipeline( String pPipelineID, PipelineConfig pPipelineConfig ) {
    // Check parameters.
    Assert.assertNotNull(pPipelineID, "pPipelineID");
    Assert.assertNotNull(pPipelineConfig, "pPipelineConfig");

    // Create new pipeline.
    Pipeline lPipeline = new Pipeline(pPipelineID, pPipelineConfig);

    // Current pipeline should also be used as default pipeline.
    if (pPipelineConfig.defaultPipeline() == true) {
      // We do not yet have a default pipeline.
      if (defaultPipeline == null) {
        defaultPipeline = lPipeline;
      }
      // Configuration error. More than 1 default pipeline is configured.
      else {
        throw new WorkloadSystemException(WorkloadMessages.MULTIPLE_DEFAULT_PIPELINES, defaultPipeline.getPipelineID(),
            lPipeline.getPipelineID());
      }
    }

    // Try to register pipeline as MBean
    try {
      final String PIPELINE_PREFIX = "com.anaptecs.jeaf.workload:type=Pipelines, name=";

      XFun.getTrace().info("Registering pipeline '" + lPipeline.getName() + "' as JMX MBean under name '"
          + lPipeline.getPipelineID() + "'.");
      MBeanServer lMBeanServer = ManagementFactory.getPlatformMBeanServer();
      lMBeanServer.registerMBean(lPipeline, new ObjectName(PIPELINE_PREFIX + lPipeline.getPipelineID()));
    }
    catch (JMException e) {
      XFun.getTrace().error(e.getMessage(), e);
    }
    // Return created pipeline.
    return lPipeline;
  }

  /**
   * Method resolves all classes that contain static workload configuration.
   * 
   * @return {@link Map} Map with all classes and their workload configuration. The method never returns null.
   */
  private Map<Class<?>, StaticWorkloadConfig> getStaticWorkloadConfigurationClasses( ) {
    ConfigurationReader lStaticWorkloadConfigurationReader = new ConfigurationReader();
    return lStaticWorkloadConfigurationReader.readAnnotationsMap(Workload.STATIC_WORKLOADS_RESOURCE_NAME,
        Workload.WORKLOAD_BASE_PATH, StaticWorkloadConfig.class);
  }

  /**
   * Method resolves the static workload configurations as they are configured.
   * 
   * @param pStaticWorkloadClasses Map with all classes that contain static workload configurations. The parameter must
   * not be null.
   * @return {@link Map} Map containing all static workload configurations and the classes where they were defined. The
   * method never returns null.
   */
  private Map<Class<?>, WorkloadMapping> loadStaticWorkloadMappings(
      Map<Class<?>, StaticWorkloadConfig> pStaticWorkloadClasses ) {

    // Check parameter
    Assert.assertNotNull(pStaticWorkloadClasses, "pStaticWorkloadClasses");

    // Collect workload mappings.
    Map<Class<?>, WorkloadMapping> lWorkloadMappings = new HashMap<>();
    for (Entry<Class<?>, StaticWorkloadConfig> lNextEntry : pStaticWorkloadClasses.entrySet()) {
      lWorkloadMappings.put(lNextEntry.getKey(), lNextEntry.getValue().workloadMapping());
    }

    // Return all workload mappings.
    return lWorkloadMappings;
  }

  private Map<Class<?>, ElasticWorkloadConfig> getElasticWorkloadConfigurationClasses( ) {
    ConfigurationReader lStaticWorkloadConfigurationReader = new ConfigurationReader();
    return lStaticWorkloadConfigurationReader.readAnnotationsMap(Workload.ELASTIC_WORKLOADS_RESOURCE_NAME,
        Workload.WORKLOAD_BASE_PATH, ElasticWorkloadConfig.class);
  }

  /**
   * Method resolves the static workload configurations as they are configured.
   * 
   * @return {@link Map} Map containing all static workload configurations and the classes where they were defined. The
   * method never returns null.
   */
  private Map<Class<?>, WorkloadMapping> loadElasticWorkloadMappings(
      Map<Class<?>, ElasticWorkloadConfig> pElasticWorkloadClasses ) {

    // Check parameter
    Assert.assertNotNull(pElasticWorkloadClasses, "pElasticWorkloadClasses");

    // Collect workload mappings.
    Map<Class<?>, WorkloadMapping> lWorkloadMappings = new HashMap<>();
    for (Entry<Class<?>, ElasticWorkloadConfig> lNextEntry : pElasticWorkloadClasses.entrySet()) {
      lWorkloadMappings.put(lNextEntry.getKey(), lNextEntry.getValue().workloadMapping());
    }

    // Return all workload mappings.
    return lWorkloadMappings;
  }

  /**
   * Method applies the passed workload mappings for this workload manager.
   * 
   * @param pPipelines Map containing all configure pipelines. The parameter must not be null.
   * @param pWorkloadMappings Workload mapping configuration that should be applied. The parameter must not be null.
   */
  private void applyWorkloadMappings( Map<String, Pipeline> pPipelines,
      Map<Class<?>, WorkloadMapping> pWorkloadMappings ) {

    // Check parameters
    Assert.assertNotNull(pPipelines, "pPipelines");
    Assert.assertNotNull(pWorkloadMappings, "pWorkloadMappings");

    // Process all workload mappings.
    for (Entry<Class<?>, WorkloadMapping> lNextEntry : pWorkloadMappings.entrySet()) {
      // Resolve current workload mapping
      WorkloadMapping lWorkloadMapping = lNextEntry.getValue();

      // Lookup pipeline that should be used.
      Class<?> lPipelineClass = lWorkloadMapping.pipeline();
      Pipeline lPipeline = pPipelines.get(lPipelineClass.getName());

      // Referenced pipeline exists.
      if (lPipeline != null) {
        // Build up mapping for all endpoints with the defined pipeline.
        List<RequestTypeKey> lRequestTypeKeys = this.createRequestTypeKeys(lWorkloadMapping.endpoints());
        for (RequestTypeKey lKey : lRequestTypeKeys) {
          workloadMapping.put(lKey, lPipeline);
        }
      }
      // Workload config references a not existing pipeline.
      else {
        throw new WorkloadSystemException(WorkloadMessages.NOT_EXISTING_PIPELINE_CONFIGURED, lPipelineClass.getName(),
            lNextEntry.getClass().getName());
      }
    }
  }

  /**
   * Method creates request type keys for the passed endpoint group.
   * 
   * @param pEndpointsGroup Endpoint group that should be used to create request type keys. The parameter must not be
   * null.
   * @return {@link List} List with all created request type keys. The method never returns null.
   */
  private List<RequestTypeKey> createRequestTypeKeys( EndpointsGroup pEndpointsGroup ) {
    // Check parameter.
    Assert.assertNotNull(pEndpointsGroup, "pEndpointsGroup");

    // Create request type key for every resource. As the array is returned from annotation it is never null, just
    // empty.
    List<RequestTypeKey> lKeys = new ArrayList<>();
    Resource[] lResourceEndpoints = pEndpointsGroup.resources();
    for (Resource lNextResource : lResourceEndpoints) {
      // Workload configuration contains http methods.
      HTTPMethod[] lHttpMethods = lNextResource.httpMethods();
      if (lHttpMethods.length > 0) {
        for (HTTPMethod lHttpMethod : lHttpMethods) {
          lKeys.add(new RESTRequestTypeKey(lNextResource.path(), lHttpMethod.name()));
        }
      }
      // Resource does not define specific http methods.
      else {
        lKeys.add(new RESTRequestTypeKey(lNextResource.path(), null));
      }
    }

    // Create request type key for endpoint URL. Also here, the array is never null.
    String[] lEndpointURLs = pEndpointsGroup.endpointURLs();
    for (String lNextURL : lEndpointURLs) {
      lKeys.add(new RESTRequestTypeKey(lNextURL, null));
    }

    // Return all created keys
    return lKeys;
  }

  /**
   * Method loads all pipelines that are configured through a YAML configuration file.
   */
  private void loadPipelinesFromYAML( ) {
    Configuration lSystemProperties = XFun.getConfigurationProvider().getSystemPropertiesConfiguration();
    String lYAMLFileLocation = lSystemProperties.getConfigurationValue(YAML_CONFIG_PROPERTY_NAME, String.class);

    // Link to YAML configuration file was provided.
    if (lYAMLFileLocation != null) {
      List<PipelineConfigImpl> lPipelines = PipelineConfiguration.loadPipelineConfigurations(lYAMLFileLocation);
      for (PipelineConfigImpl lNextPipelineConfig : lPipelines) {
        Pipeline lNewPipeline =
            this.createPipeline(lNextPipelineConfig.getId(), lNextPipelineConfig.getPipelineConfig());

        // Process request type keys.
        List<RequestTypeKey> lRequestTypeKeys = lNextPipelineConfig.getRequestTypeKeys();

        // Add workload mapping for all request type keys and the new pipeline.
        for (RequestTypeKey lNextKey : lRequestTypeKeys) {
          workloadMapping.put(lNextKey, lNewPipeline);
        }
      }
    }
    // No YAML configuration file defined.
    else {
      // Nothing to do.
    }
  }

  /**
   * Method is used to hand over a runnable to the workload manager that it is executed by one of its pipelines.
   * 
   * @see WorkloadManager#execute(RequestType, WorkloadErrorHandler, Runnable)
   */
  @Override
  public void execute( RequestType pRequestMetaInfo, WorkloadErrorHandler pErrorHandler, Runnable pCommand ) {
    // Resolve pipeline that should be used to execute the request.
    Pipeline lPipeline = this.resolvePipeline(pRequestMetaInfo.getRequestKey());

    // Delegate request to pipeline.
    lPipeline.execute(pRequestMetaInfo.getRequestKey(), pErrorHandler, pCommand);
  }

  /**
   * Method resolves the associated pipeline for the passed request type key.
   * 
   * @param pRequestTypeKey Request type key for which the associated pipeline should be returned. The parameter must
   * not be null.
   * @return {@link Pipeline} Pipeline that is defined to execute the passed request type. The method never returns
   * null.
   * @throws JEAFSystemException In case that no pipeline could be found to execute for the request type.
   */
  private Pipeline resolvePipeline( RequestTypeKey pRequestTypeKey ) {
    // Try to find executor for the passed key.
    Pipeline lPipeline = workloadMapping.get(pRequestTypeKey);

    // Pipeline found ;-)
    if (lPipeline != null) {
      // Nothing to do.
    }
    // No pipeline found for key. Try to work with reduced key.
    else {
      // Reduce key and try again.
      RequestTypeKey lReducedKey = pRequestTypeKey.reduceKey();

      // Try to find pipeline for reduced key.
      if (lReducedKey != null) {
        // Do recursive call of this method to resolve pipeline for reduced key.
        lPipeline = this.resolvePipeline(lReducedKey);
      }
      // Key can not be reduced any longer.
      else {
        // Nothing to do here. General behavior below is also sufficient for this case.
      }

      // We finally found the pipeline to be used or at least we have a default pipeline.
      if (lPipeline != null || defaultPipeline != null) {
        // Use default pipeline to execute request.
        if (lPipeline == null) {
          lPipeline = defaultPipeline;
        }
        // In order to speedup pipeline lookup for the next time we will add the mapping between the request key and
        // the pipeline.
        workloadMapping.put(pRequestTypeKey, lPipeline);
      }
      // We neither where able to find a pipeline nor do we have a default pipeline, so we have to give up :-(
      else {
        throw new WorkloadSystemException(WorkloadMessages.NO_PIPELINE_FOUND, pRequestTypeKey.getKey());
      }
    }
    // Return pipeline that should be used to execute the request.
    return lPipeline;
  }

  public PipelineInfo getDefaultPipeline( ) {
    PipelineInfo lThreadPoolInfo;
    if (defaultPipeline != null) {
      lThreadPoolInfo = defaultPipeline.getPipelineInfo();
    }
    else {
      lThreadPoolInfo = null;
    }
    return lThreadPoolInfo;
  }

  public List<PipelineInfo> getPipelines( ) {
    Set<Pipeline> lPipelines = new HashSet<>();
    // Add default pipeline.
    if (defaultPipeline != null) {
      lPipelines.add(defaultPipeline);
    }

    // Add pipelines from workload mapping.
    for (Entry<RequestTypeKey, Pipeline> lNextEntry : workloadMapping.entrySet()) {
      lPipelines.add(lNextEntry.getValue());
    }

    // Return only pipeline info and not the pipeline itself.
    List<PipelineInfo> lPipelineInfos = new ArrayList<>(lPipelines.size());
    for (Pipeline lNextPipeline : lPipelines) {
      lPipelineInfos.add(lNextPipeline.getPipelineInfo());
    }
    return lPipelineInfos;
  }

  public Map<RequestTypeKey, PipelineInfo> getWorkloadMappings( ) {
    Map<RequestTypeKey, PipelineInfo> lWorkloadMappings = new HashMap<>();
    for (Entry<RequestTypeKey, Pipeline> lEntry : workloadMapping.entrySet()) {
      lWorkloadMappings.put(lEntry.getKey(), lEntry.getValue().getPipelineInfo());
    }
    return lWorkloadMappings;
  }

  @Override
  public boolean isPipelineOverloaded( RequestType pRequestType ) {
    // TODO Implement real overload detection based on the request and the associated pipeline.
    return false;
  }

}
