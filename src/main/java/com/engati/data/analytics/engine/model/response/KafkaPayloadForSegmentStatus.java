package com.engati.data.analytics.engine.model.response;

import lombok.Data;

import java.sql.Timestamp;

@Data
public class KafkaPayloadForSegmentStatus {

  private Timestamp timestamp;
  private String status;
  private String segmentName;
  private Long botRef;
  private String fileName;

}
