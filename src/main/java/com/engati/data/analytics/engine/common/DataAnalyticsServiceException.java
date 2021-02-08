package com.engati.data.analytics.engine.common;

import com.nethum.errorhandling.exception.NethumBaseException;
import com.nethum.errorhandling.exception.error.AppErrorObject;
import org.springframework.http.HttpStatus;

public class DataAnalyticsServiceException extends NethumBaseException {

  public DataAnalyticsServiceException(AppErrorObject customError) {
    super(customError);
  }
  public DataAnalyticsServiceException(DataAnalyticsEngineStatusCode statusCode) {
    super(new AppErrorObject.Builder().appCode(statusCode).HttpStatus(HttpStatus.OK).build());
  }
}
