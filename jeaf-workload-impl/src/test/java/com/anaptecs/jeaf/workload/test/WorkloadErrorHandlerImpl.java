/**
 * Copyright 2004 - 2020 anaptecs GmbH, Burgstr. 96, 72764 Reutlingen, Germany
 *
 * All rights reserved.
 */
package com.anaptecs.jeaf.workload.test;

import com.anaptecs.jeaf.workload.api.RequestTypeKey;
import com.anaptecs.jeaf.workload.api.WorkloadErrorHandler;
import com.anaptecs.jeaf.xfun.api.XFun;

class WorkloadErrorHandlerImpl implements WorkloadErrorHandler {
  public RequestTypeKey requestTypeKey;

  public Exception exception;

  public boolean requestRejected = false;

  public boolean maximumLatencyExceeded = false;

  public boolean noExceptions = true;

  @Override
  public void requestRejected( RequestTypeKey pRequestTypeKey, Exception pException ) {
    requestTypeKey = pRequestTypeKey;
    exception = pException;
    requestRejected = true;
    noExceptions = false;
    XFun.getTrace().error(pException.getMessage());
  }

  @Override
  public void maximumLatencyExceeded( RequestTypeKey pRequestTypeKey, Exception pException ) {
    requestTypeKey = pRequestTypeKey;
    exception = pException;
    maximumLatencyExceeded = true;
    noExceptions = false;
    XFun.getTrace().error(pException.getMessage());
  }

  public void reset( ) {
    requestTypeKey = null;
    exception = null;
    requestRejected = false;
    maximumLatencyExceeded = false;
    noExceptions = true;
  }
}