package com.engati.data.analytics.engine.queryGenerator.service;

import com.engati.data.analytics.sdk.druid.aggregator.DruidAggregatorMetaInfo;
import com.engati.data.analytics.sdk.druid.filters.DruidFilterMetaInfo;
import com.engati.data.analytics.sdk.druid.postaggregator.DruidPostAggregatorMetaInfo;
import in.zapr.druid.druidry.aggregator.DruidAggregator;
import in.zapr.druid.druidry.filter.DruidFilter;
import in.zapr.druid.druidry.postAggregator.DruidPostAggregator;

import java.util.List;

public interface DruidQueryGenerator {

  List<DruidAggregator> generateAggregators(List<DruidAggregatorMetaInfo> druidAggregateMetaInfoList);

  List<DruidPostAggregator> generatePostAggregator(List<DruidPostAggregatorMetaInfo>
      druidPostAggregateMetaInfoList);

  DruidFilter generateFilters(DruidFilterMetaInfo druidFilterMetaInfoDto);
}
