package com.engati.data.analytics.engine.entity;

import lombok.*;

import javax.persistence.*;

@Builder
@Setter
@Getter
@NoArgsConstructor
@AllArgsConstructor
@ToString(doNotUseGetters = true)
@Entity
@IdClass(StoreSegmentationConfigurationKey.class)
@Table(name = "SEGMENTATION_CONFIGURATION_STORE")
public class StoreSegmentationConfiguration<S> {

  @Id
  @Column(name = "BOT_REF")
  private Long botRef;

  @Id
  @Column(name = "CUSTOMER_ID")
  private Long customerId;

  @Id
  @Column(name = "SEGMENT_NAME")
  private String segmentName;

  @Column(name = "RECENCY_METRIC")
  private String recencyMetric;

  @Column(name = "RECENCY_OPERATOR")
  private String recencyOperator;

  @Column(name = "RECENCY_VALUE")
  private Long recencyValue;

  @Column(name = "FREQUENCY_METRIC")
  private Long frequencyMetric;

  @Column(name = "FREQUENCY_OPERATOR")
  private String frequencyOperator;

  @Column(name = "FREQUENCY_VALUE")
  private Long frequencyValue;

  @Column(name = "MONETARY_METRIC")
  private String monetaryMetric;

  @Column(name = "MONETARY_OPERATOR")
  private String monetaryOperator;

  @Column(name = "MONETARY_VALUE")
  private String monetaryValue;

}
