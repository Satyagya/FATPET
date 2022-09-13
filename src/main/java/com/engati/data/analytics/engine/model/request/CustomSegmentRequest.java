package com.engati.data.analytics.engine.model.request;

import lombok.Builder;
import lombok.Data;

import java.sql.Date;

@Data
@Builder
public class CustomSegmentRequest {
  private String segmentCondition;
  private String segmentName;
  private String fileName;
  private Date startDate;
  private Date endDate;
}
