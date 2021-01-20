package com.engati.data.analytics.engine.queryGenerator.service;

import com.engati.data.analytics.sdk.druid.postaggregator.DruidPostAggregatorMetaInfo;
import in.zapr.druid.druidry.postAggregator.DruidPostAggregator;

import java.util.List;

public interface DruidPostAggregateGenerator {

  List<DruidPostAggregator> getQueryPostAggregators(List<DruidPostAggregatorMetaInfo>
      postAggregateMetaInfoDtos);
}
