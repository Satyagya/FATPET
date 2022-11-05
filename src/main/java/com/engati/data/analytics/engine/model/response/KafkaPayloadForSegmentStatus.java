package com.engati.data.analytics.engine.model.response;

import lombok.Data;

import java.sql.Date;
import java.sql.Timestamp;
import java.util.ArrayList;

@Data
public class KafkaPayloadForSegmentStatus {

  private Timestamp timestamp;
  private String status;
  private String segmentName;
  private Long botRef;
  private String fileName;
  private int customerCount;
  private String segmentType;
  private String dateRange;
}
