package com.engati.data.analytics.engine.model.request;

import lombok.Builder;
import lombok.Data;

import java.sql.Date;
import java.util.Optional;

@Data
@Builder
public class CustomSegmentRequest {
  private String segmentCondition;
  private String segmentName;
  private String fileName;
  private Optional<Date> startDate;
  private Optional<Date> endDate;
}
