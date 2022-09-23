package com.engati.data.analytics.engine.model.request;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import lombok.Builder;
import lombok.Data;

import java.sql.Date;
import java.util.Optional;

@Data
@Builder
@JsonIgnoreProperties(ignoreUnknown = true)
public class CustomSegmentRequest {
  private String segmentCondition;
  private String segmentName;
  private String fileName;
  private Date startDate;
  private Date endDate;
}
