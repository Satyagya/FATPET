package com.engati.data.analytics.engine.service;

public interface PrometheusManagementService {

  /***
   * To slack alert any API failure
   * @param event
   * @param botRef
   * @param causeOfFailure
   * @param apiRequest
   */
  void apiRequestFailureEvent(String event, String botRef, String causeOfFailure, String apiRequest);
}
