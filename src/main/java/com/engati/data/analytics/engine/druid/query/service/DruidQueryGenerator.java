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

  /**
   * Create druid aggregator for given metaInfo
   *
   * @param druidAggregateMetaInfoList
   * @param botRef
   * @param customerId
   *
   * @return Druid Aggregator List
   */
  List<DruidAggregator> generateAggregators(List<DruidAggregatorMetaInfo>
      druidAggregateMetaInfoList, Integer botRef, Integer customerId);

  /**
   * Create druid post aggregator for given metaInfo
   *
   * @param druidPostAggregateMetaInfoList
   * @param botRef
   * @param customerId
   *
   * @return Druid Post Aggregator List
   */
  List<DruidPostAggregator> generatePostAggregator(List<DruidPostAggregatorMetaInfo>
      druidPostAggregateMetaInfoList, Integer botRef, Integer customerId);

  /**
   * Create druid filter for given metaInfo
   *
   * @param druidFilterMetaInfoDto
   * @param botRef
   * @param customerId
   *
   * @return Druid Filter
   */
  DruidFilter generateFilters(DruidFilterMetaInfo druidFilterMetaInfoDto, Integer botRef,
      Integer customerId);

  /**
   * Create druid joins for given metaInfo
   *
   * @param joinMetaInfo
   * @param botRef
   * @param customerId
   *
   * @return Druid Join
   */
  DruidJoin getJoinDataSource(JoinMetaInfo joinMetaInfo, Integer botRef,
      Integer customerId);
}
