package com.engati.data.analytics.engine.response.ingestion;

import org.springframework.http.HttpStatus;


public class IngestionResponse<T> {

  private T responseObject;
  private IngestionStatusCode status;
  private HttpStatus statusCode;

  public IngestionResponse() {
    this.statusCode = HttpStatus.OK;
  }

  public IngestionResponse(IngestionStatusCode status) {
    this();
    this.status = status;
  }

  public T getResponseObject() {
    return responseObject;
  }

  public void setResponseObject(T responseObject) {
    this.responseObject = responseObject;
  }

  public IngestionStatusCode getStatus() {
    return status;
  }

  public void setStatus(IngestionStatusCode status) {
    this.status = status;
  }

  public HttpStatus getStatusCode() {
    return statusCode;
  }

  public void setStatusCode(HttpStatus statusCode) {
    this.statusCode = statusCode;
  }
}
