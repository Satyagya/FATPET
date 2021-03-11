package com.engati.data.analytics.engine.druid.query.service;

import com.engati.data.analytics.engine.druid.query.druidry.join.DruidJoin;
import com.engati.data.analytics.sdk.druid.aggregator.DruidAggregatorMetaInfo;
import com.engati.data.analytics.sdk.druid.filters.DruidFilterMetaInfo;
import com.engati.data.analytics.sdk.druid.postaggregator.DruidPostAggregatorMetaInfo;
import com.engati.data.analytics.sdk.druid.query.join.JoinMetaInfo;
import in.zapr.druid.druidry.aggregator.DruidAggregator;
import in.zapr.druid.druidry.filter.DruidFilter;
import in.zapr.druid.druidry.postAggregator.DruidPostAggregator;

import java.util.List;

public interface DruidQueryGenerator {

  List<DruidAggregator> generateAggregators(List<DruidAggregatorMetaInfo>
      druidAggregateMetaInfoList, Integer botRef, Integer customerId);

  List<DruidPostAggregator> generatePostAggregator(List<DruidPostAggregatorMetaInfo>
      druidPostAggregateMetaInfoList, Integer botRef, Integer customerId);

  DruidFilter generateFilters(DruidFilterMetaInfo druidFilterMetaInfoDto, Integer botRef,
      Integer customerId);

  DruidJoin getJoinDataSource(JoinMetaInfo joinMetaInfo, Integer botRef,
      Integer customerId);
}
