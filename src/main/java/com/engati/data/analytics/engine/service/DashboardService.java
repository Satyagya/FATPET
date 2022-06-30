package com.engati.data.analytics.engine.service;


import com.engati.data.analytics.engine.common.model.DataAnalyticsResponse;
import com.engati.data.analytics.engine.model.request.DashboardRequest;
import com.engati.data.analytics.engine.model.response.DashboardChartResponse;
import com.engati.data.analytics.engine.model.response.DashboardFlierResponse;
import com.engati.data.analytics.engine.model.response.DashboardGraphResponse;
import com.engati.data.analytics.engine.model.response.DashboardProductResponse;

import java.util.List;

public interface DashboardService {
  DataAnalyticsResponse<DashboardFlierResponse> getEngagedUsers(Long botRef, DashboardRequest dashboardRequest);

  DataAnalyticsResponse<DashboardFlierResponse> getOrderCount(Long botRef, DashboardRequest dashboardRequest);

  DataAnalyticsResponse<DashboardFlierResponse> getAOV(Long botRef, DashboardRequest dashboardRequest);

  DataAnalyticsResponse<DashboardFlierResponse> getAbandonedCheckouts(Long botRef, DashboardRequest dashboardRequest);

  DataAnalyticsResponse<DashboardFlierResponse> getTransactionsFromEngati(Long botRef, DashboardRequest dashboardRequest);

  DataAnalyticsResponse<DashboardFlierResponse> getTransactionRevenueFromEngati(Long botRef, DashboardRequest dashboardRequest);

  DataAnalyticsResponse<List<DashboardProductResponse>> getMostPurchasedProducts(Long botRef, DashboardRequest dashboardRequest);

  DataAnalyticsResponse<List<DashboardProductResponse>> getMostAbandonedProducts(Long botRef, DashboardRequest dashboardRequest);

  DataAnalyticsResponse<List<DashboardGraphResponse>> getBotQueriesChart(Long botRef, DashboardRequest dashboardRequest);

  DataAnalyticsResponse<DashboardChartResponse> getEngagedUsersPerPlatform(Long botRef, DashboardRequest dashboardRequest);

  DataAnalyticsResponse<DashboardChartResponse> getConversationIntentBreakdown(Long botRef, DashboardRequest dashboardRequest);

  DataAnalyticsResponse<DashboardChartResponse> getConversationSentimentBreakdown(Long botRef, DashboardRequest dashboardRequest);

  DataAnalyticsResponse<String> getLastUpdatedOn(Long botRef);

}
