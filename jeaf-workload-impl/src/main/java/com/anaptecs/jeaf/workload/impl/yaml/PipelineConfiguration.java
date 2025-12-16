/**
 * Copyright 2004 - 2020 anaptecs GmbH, Burgstr. 96, 72764 Reutlingen, Germany
 *
 * All rights reserved.
 */
package com.anaptecs.jeaf.workload.impl.yaml;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.yaml.snakeyaml.LoaderOptions;
import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.constructor.Constructor;
import org.yaml.snakeyaml.inspector.TagInspector;
import org.yaml.snakeyaml.nodes.Tag;

import com.anaptecs.jeaf.tools.api.file.FileTools;
import com.anaptecs.jeaf.workload.api.WorkloadMessages;
import com.anaptecs.jeaf.xfun.api.checks.Check;
import com.anaptecs.jeaf.xfun.api.errorhandling.JEAFSystemException;

public class PipelineConfiguration {
  /**
   * Method loads all pipeline configurations from the passed YAML file.
   * 
   * @param pFileName Name of the YAML file from which the pipelines should be read. The parameter must not be null and
   * a file must exist under the given path.
   * @return
   */
  public static List<PipelineConfigImpl> loadPipelineConfigurations( String pFileName ) {
    // Check parameter
    Check.checkInvalidParameterNull(pFileName, "pFileName");

    try {
      // Load pipeline configurations from YAML file with the passed name.
      String lFileContent = FileTools.getFileTools().getFileContentAsString(pFileName);
      Yaml lYAML = new Yaml(new Constructor(PipelineConfiguration.class, getSecureSnakeYAMLOptions()));
      PipelineConfiguration lPipelinesConfiguration = lYAML.load(lFileContent);

      // Return all pipelines that were part of the configuration.
      return lPipelinesConfiguration.getPipelines();
    }
    // Unable to load YAML file.
    catch (IOException e) {
      throw new JEAFSystemException(WorkloadMessages.UNABLE_TO_LOAD_YAML_CONFIG, pFileName, e.getMessage());
    }
  }

  List<PipelineConfigImpl> pipelines = new ArrayList<>();

  private static LoaderOptions getSecureSnakeYAMLOptions( ) {
    LoaderOptions lLoaderOptions = new LoaderOptions();
    lLoaderOptions.setTagInspector(new TagInspector() {
      @Override
      public boolean isGlobalTagAllowed( Tag pTag ) {
        return false;
      }
    });
    return lLoaderOptions;
  }

  public List<PipelineConfigImpl> getPipelines( ) {
    return pipelines;
  }

  public void setPipelines( List<PipelineConfigImpl> pPipelines ) {
    pipelines = pPipelines;
  }

  public void addPipeline( PipelineConfigImpl pPipeline ) {
    pipelines.add(pPipeline);
  }
}
