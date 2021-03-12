package com.engati.data.analytics.engine.druid.query.service;

import com.engati.data.analytics.sdk.druid.aggregator.DruidAggregatorMetaInfo;
import in.zapr.druid.druidry.aggregator.DruidAggregator;

import java.util.List;

public interface DruidAggregateGenerator {

  /**
   * Create druid aggregator for given metaInfo
   *
   * @param druidAggregateMetaInfoDtos
   * @param botRef
   * @param customerId
   *
   * @return Druid Aggregator List
   */
  List<DruidAggregator> getQueryAggregators(List<DruidAggregatorMetaInfo>
      druidAggregateMetaInfoDtos, Integer botRef, Integer customerId);
}
