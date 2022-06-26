package com.engati.data.analytics.engine.model.request;

import lombok.Data;

import java.util.Date;

@Data
public class DashboardRequest {

  private Date startTime;
  private Date endTime;

}