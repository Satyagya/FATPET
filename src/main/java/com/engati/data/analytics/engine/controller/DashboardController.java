package com.engati.data.analytics.engine.controller;

import com.engati.data.analytics.engine.common.model.DataAnalyticsResponse;
import com.engati.data.analytics.engine.constants.constant.ApiPathConstants;
import com.engati.data.analytics.engine.model.request.DashboardRequest;
import com.engati.data.analytics.engine.model.response.DashboardFlierResponse;
import com.engati.data.analytics.engine.service.DashboardService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping(ApiPathConstants.API_BASE_PATH)
@Slf4j
public class DashboardController {

  @Autowired
  private DashboardService dashboardService;

  @PostMapping(value = ApiPathConstants.ENGAGED_USERS)
  public ResponseEntity<DataAnalyticsResponse> getEngagedUsers(@PathVariable(name = ApiPathConstants.BOTREF) Long botRef, @RequestBody
      DashboardRequest dashboardRequest) {
    log.info("Got call for getting engaged users for botRef: {}, for timeRanges between {} and {}", botRef, dashboardRequest.getStartTime(), dashboardRequest.getEndTime());
    DataAnalyticsResponse<DashboardFlierResponse> response = dashboardService.getEngagedUsers(botRef, dashboardRequest);
    return new ResponseEntity<>(response, response.getStatusCode());
  }


}
