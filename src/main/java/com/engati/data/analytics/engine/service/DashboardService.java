package com.engati.data.analytics.engine.service;


import com.engati.data.analytics.engine.common.model.DataAnalyticsResponse;
import com.engati.data.analytics.engine.model.request.DashboardRequest;
import com.engati.data.analytics.engine.model.response.DashboardFlierResponse;

public interface DashboardService {
  DataAnalyticsResponse<DashboardFlierResponse> getEngagedUsers(Long botRef, DashboardRequest dashboardRequest);
}
