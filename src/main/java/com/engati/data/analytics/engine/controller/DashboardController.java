package com.engati.data.analytics.engine.controller;

import com.engati.data.analytics.engine.common.model.DataAnalyticsResponse;
import com.engati.data.analytics.engine.constants.constant.ApiPathConstants;
import com.engati.data.analytics.engine.model.request.DashboardRequest;
import com.engati.data.analytics.engine.model.response.DashboardChartResponse;
import com.engati.data.analytics.engine.model.response.DashboardFlierResponse;
import com.engati.data.analytics.engine.model.response.DashboardGraphResponse;
import com.engati.data.analytics.engine.model.response.DashboardProductResponse;
import com.engati.data.analytics.engine.service.DashboardService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

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

  @PostMapping(value = ApiPathConstants.ORDER_COUNT)
  public ResponseEntity<DataAnalyticsResponse> getOrderCount(@PathVariable(name = ApiPathConstants.BOTREF) Long botRef, @RequestBody
      DashboardRequest dashboardRequest) {
    log.info("Got call for getting OrderCounts for botRef: {}, for timeRanges between {} and {}", botRef, dashboardRequest.getStartTime(), dashboardRequest.getEndTime());
    DataAnalyticsResponse<DashboardFlierResponse> response = dashboardService.getOrderCount(botRef, dashboardRequest);
    return new ResponseEntity<>(response, response.getStatusCode());
  }

  @PostMapping(value = ApiPathConstants.GET_AOV)
  public ResponseEntity<DataAnalyticsResponse> getAOV(@PathVariable(name = ApiPathConstants.BOTREF) Long botRef, @RequestBody
      DashboardRequest dashboardRequest) {
    log.info("Got call for getting AOV for botRef: {}, for timeRanges between {} and {}", botRef, dashboardRequest.getStartTime(), dashboardRequest.getEndTime());
    DataAnalyticsResponse<DashboardFlierResponse> response = dashboardService.getAOV(botRef, dashboardRequest);
    return new ResponseEntity<>(response, response.getStatusCode());
  }

  @PostMapping(value = ApiPathConstants.GET_ABANDONED_CHECKOUTS)
  public ResponseEntity<DataAnalyticsResponse> getAbandonedCheckouts(@PathVariable(name = ApiPathConstants.BOTREF) Long botRef, @RequestBody
      DashboardRequest dashboardRequest) {
    log.info("Got call for getting AbandonedCheckouts for botRef: {}, for timeRanges between {} and {}", botRef, dashboardRequest.getStartTime(), dashboardRequest.getEndTime());
    DataAnalyticsResponse<DashboardFlierResponse> response = dashboardService.getAbandonedCheckouts(botRef, dashboardRequest);
    return new ResponseEntity<>(response, response.getStatusCode());
  }

  @PostMapping(value = ApiPathConstants.MOST_PURCHASED_PRODUCTS)
  public ResponseEntity<DataAnalyticsResponse<List<DashboardProductResponse>>> getMostPurchasedProducts(@PathVariable(name = ApiPathConstants.BOTREF) Long botRef, @RequestBody
      DashboardRequest dashboardRequest) {
    log.info("Got call for getting most purchased products for botRef: {}, for timeRanges between {} and {}", botRef, dashboardRequest.getStartTime(), dashboardRequest.getEndTime());
    DataAnalyticsResponse<List<DashboardProductResponse>> response = dashboardService.getMostPurchasedProducts(botRef, dashboardRequest);
    return new ResponseEntity<>(response, response.getStatusCode());
  }

  @PostMapping(value = ApiPathConstants.MOST_ABANDONED_PRODUCTS)
  public ResponseEntity<DataAnalyticsResponse<List<DashboardProductResponse>>> getMostAbandonedProducts(@PathVariable(name = ApiPathConstants.BOTREF) Long botRef, @RequestBody
      DashboardRequest dashboardRequest) {
    log.info("Got call for getting most purchased products for botRef: {}, for timeRanges between {} and {}", botRef, dashboardRequest.getStartTime(), dashboardRequest.getEndTime());
    DataAnalyticsResponse<List<DashboardProductResponse>> response = dashboardService.getMostAbandonedProducts(botRef, dashboardRequest);
    return new ResponseEntity<>(response, response.getStatusCode());
  }

  @PostMapping(value = ApiPathConstants.BOT_QUERIES_CHART)
  public ResponseEntity<DataAnalyticsResponse<List<DashboardGraphResponse>>> getBotQueriesChart(@PathVariable(name = ApiPathConstants.BOTREF) Long botRef, @RequestBody
      DashboardRequest dashboardRequest) {
    log.info("Got call for getting botQueries Chart for botRef: {}, for timeRanges between {} and {}", botRef, dashboardRequest.getStartTime(), dashboardRequest.getEndTime());
    DataAnalyticsResponse<List<DashboardGraphResponse>> response = dashboardService.getBotQueriesChart(botRef, dashboardRequest);
    return new ResponseEntity<>(response, response.getStatusCode());
  }

  @PostMapping(value = ApiPathConstants.ENGAGED_USERS_PER_PLATFORM)
  public ResponseEntity<DataAnalyticsResponse> getEngagedUsersPerPlatform(@PathVariable(name =
      ApiPathConstants.BOTREF) Long botRef, @RequestBody
      DashboardRequest dashboardRequest) {
    log.info("Got call for getting engaged users for botRef: {}, for timeRanges between {} and {} by platform", botRef,
        dashboardRequest.getStartTime(), dashboardRequest.getEndTime());
    DataAnalyticsResponse<DashboardChartResponse> response = dashboardService.getEngagedUsersPerPlatform(botRef,
        dashboardRequest);
    return new ResponseEntity<>(response, response.getStatusCode());
  }

  @PostMapping(value = ApiPathConstants.CONVERSATION_INTENT)
  public ResponseEntity<DataAnalyticsResponse> getConversationIntentBreakdown(@PathVariable(name =
      ApiPathConstants.BOTREF) Long botRef, @RequestBody
      DashboardRequest dashboardRequest) {
    log.info("Got call for getting conversations intents percentage for botRef: {}, for timeRanges between {} and {} ",
        botRef, dashboardRequest.getStartTime(), dashboardRequest.getEndTime());
    DataAnalyticsResponse<DashboardChartResponse> response = dashboardService.getConversationIntentBreakdown(botRef,
        dashboardRequest);
    return new ResponseEntity<>(response, response.getStatusCode());
  }

  @PostMapping(value = ApiPathConstants.CONVERSATION_SENTIMENT)
  public ResponseEntity<DataAnalyticsResponse> getConversationSentimentBreakdown(@PathVariable(name =
      ApiPathConstants.BOTREF) Long botRef, @RequestBody
      DashboardRequest dashboardRequest) {
    log.info("Got call for getting conversations intents percentage for botRef: {}, for timeRanges between {} and {} ",
        botRef, dashboardRequest.getStartTime(), dashboardRequest.getEndTime());
    DataAnalyticsResponse<DashboardChartResponse> response = dashboardService.getConversationSentimentBreakdown(botRef,
        dashboardRequest);
    return new ResponseEntity<>(response, response.getStatusCode());
  }

  @PostMapping(value = ApiPathConstants.LAST_UPDATED_ON)
  public ResponseEntity<DataAnalyticsResponse> getLastUpdatedOnForDashboard(@PathVariable(name = ApiPathConstants.BOTREF) Long botRef){
    log.info("Got call for getting Last Updated On for botRef: {}", botRef);
    DataAnalyticsResponse<String> response = dashboardService.getLastUpdatedOn(botRef);
    return new ResponseEntity<>(response, response.getStatusCode());
  }

}
