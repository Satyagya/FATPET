package com.engati.data.analytics.engine.queryGenerator.service.impl;

import com.engati.data.analytics.engine.queryGenerator.service.DruidAggregateGenerator;
import com.engati.data.analytics.engine.queryGenerator.service.DruidFilterGenerator;
import com.engati.data.analytics.engine.queryGenerator.service.DruidPostAggregateGenerator;
import com.engati.data.analytics.engine.queryGenerator.service.DruidQueryGenerator;
import com.engati.data.analytics.sdk.druid.aggregator.DruidAggregatorMetaInfo;
import com.engati.data.analytics.sdk.druid.filters.DruidFilterMetaInfo;
import com.engati.data.analytics.sdk.druid.postaggregator.DruidPostAggregatorMetaInfo;
import in.zapr.druid.druidry.aggregator.DruidAggregator;
import in.zapr.druid.druidry.filter.DruidFilter;
import in.zapr.druid.druidry.postAggregator.DruidPostAggregator;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;

@Service
@Slf4j
public class DruidQueryGeneratorImpl implements DruidQueryGenerator {

  @Autowired
  private DruidAggregateGenerator druidAggregateGenerator;

  @Autowired
  private DruidPostAggregateGenerator druidPostAggregateGenerator;

  @Autowired
  private DruidFilterGenerator druidFilterGenerator;

  @Override
  public List<DruidAggregator> generateAggregators(List<DruidAggregatorMetaInfo>
      druidAggregateMetaInfoList) {
    return druidAggregateGenerator.getQueryAggregators(druidAggregateMetaInfoList);
  }

  @Override
  public List<DruidPostAggregator> generatePostAggregator(
      List<DruidPostAggregatorMetaInfo> druidPostAggregateMetaInfoList) {
    return druidPostAggregateGenerator.getQueryPostAggregators(druidPostAggregateMetaInfoList);
  }

  @Override
  public DruidFilter generateFilters(DruidFilterMetaInfo druidFilterMetaInfoDto) {
    return druidFilterGenerator.getFiltersByType(druidFilterMetaInfoDto);
  }
}
