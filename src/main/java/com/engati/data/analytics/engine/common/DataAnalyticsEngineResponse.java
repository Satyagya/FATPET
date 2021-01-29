package com.engati.data.analytics.engine.common;

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
public class DataAnalyticsEngineResponse<T> implements Serializable {
  private T responseObject;
  private DataAnalyticsEngineStatusCode status;

  private HttpStatus statusCode;

  public DataAnalyticsEngineResponse() {
    this.statusCode = HttpStatus.OK;
  }

  public DataAnalyticsEngineResponse(DataAnalyticsEngineStatusCode status) {
    this();
    this.status = status;
  }
}
