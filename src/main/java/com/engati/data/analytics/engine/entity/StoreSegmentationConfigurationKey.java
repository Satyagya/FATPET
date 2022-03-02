package com.engati.data.analytics.engine.entity;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import javax.persistence.Column;
import java.io.Serializable;

@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
public class StoreSegmentationConfigurationKey implements Serializable {

  @Column(name = "CUSTOMER_ID")
  private Long customerId;

  @Column(name = "BOT_REF")
  private Long botRef;

  @Column(name = "SEGMENT_NAME")
  private String segmentName;
}
