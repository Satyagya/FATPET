package com.engati.data.analytics.engine.druid.query.service;

import com.engati.data.analytics.sdk.druid.postaggregator.DruidPostAggregatorMetaInfo;
import in.zapr.druid.druidry.postAggregator.DruidPostAggregator;

import java.util.List;

public interface DruidPostAggregateGenerator {

  /**
   * Create druid post aggregator for given metaInfo
   *
   * @param postAggregateMetaInfoDtos
   * @param botRef
   * @param customerId
   *
   * @return Druid Post Aggregator List
   */
  List<DruidPostAggregator> getQueryPostAggregators(List<DruidPostAggregatorMetaInfo>
      postAggregateMetaInfoDtos, Integer botRef, Integer customerId);
}
