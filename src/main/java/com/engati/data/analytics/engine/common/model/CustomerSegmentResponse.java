package com.engati.data.analytics.engine.common.model;

import com.engati.data.analytics.engine.constants.ResponseStatusCode;
import com.fasterxml.jackson.annotation.JsonInclude;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;
import org.springframework.http.HttpStatus;

import java.io.Serializable;

@Getter
@Setter
@ToString
@JsonInclude
@AllArgsConstructor
public class CustomerSegmentResponse<T> implements Serializable {
  private T responseObject;
  private ResponseStatusCode responseStatusCode;
  private HttpStatus statusCode;

  public CustomerSegmentResponse() {
    this.statusCode = HttpStatus.OK;
  }

  public CustomerSegmentResponse(T responseObject) {
    this.responseObject = responseObject;
    this.statusCode = HttpStatus.OK;
  }

  public CustomerSegmentResponse(T responseObject, ResponseStatusCode status) {
    this.responseObject = responseObject;
    this.responseStatusCode = status;
    this.statusCode = HttpStatus.OK;
  }

  public CustomerSegmentResponse(ResponseStatusCode status) {
    this.responseStatusCode = status;
    this.statusCode = HttpStatus.OK;
  }
}