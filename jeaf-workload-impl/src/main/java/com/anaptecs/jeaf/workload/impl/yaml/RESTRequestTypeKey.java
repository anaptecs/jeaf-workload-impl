/**
 * Copyright 2004 - 2020 anaptecs GmbH, Burgstr. 96, 72764 Reutlingen, Germany
 *
 * All rights reserved.
 */
package com.anaptecs.jeaf.workload.impl.yaml;

public class RESTRequestTypeKey {
  private String endpointURL;

  private String httpMethod;

  public String getEndpointURL( ) {
    return endpointURL;
  }

  public void setEndpointURL( String pEndpointURL ) {
    endpointURL = pEndpointURL;
  }

  public String getHttpMethod( ) {
    return httpMethod;
  }

  public void setHttpMethod( String pHttpMethod ) {
    httpMethod = pHttpMethod;
  }

}
