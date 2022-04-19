package com.engati.data.analytics.engine.entity;



import com.engati.data.analytics.engine.constants.enums.RecencyMetric;
import lombok.Data;

import javax.persistence.*;


@Data
@Entity
@Table(name = "customer_segmentation_configuration")
public class CustomerSegmentationConfiguration {

  @Id
  @GeneratedValue(strategy = GenerationType.AUTO)
  private int id;

  @Column(name = "bot_ref")
  private Long botRef;

  @Column(name = "customer_id")
  private Long customerId;

  @Column(name = "segment_name")
  private String segmentName;

//  @Column(name = "recency_metric")
//  @Enumerated(EnumType.STRING)
//  private RecencyMetric recencyMetric;

  @Column(name = "recency_metric")
  private String recencyMetric;

  @Column(name = "recency_operator")
  private String recencyOperator;

  @Column(name = "recency_value")
  private Long recencyValue;

  @Column(name = "frequency_metric")
  private Long frequencyMetric;

  @Column(name = "frequency_operator")
  private String frequencyOperator;

  @Column(name = "frequency_value")
  private Long frequencyValue;

  @Column(name = "monetary_metric")
  private String monetaryMetric;

  @Column(name = "monetary_operator")
  private String monetaryOperator;

  @Column(name = "monetary_value")
  private String monetaryValue;

}
