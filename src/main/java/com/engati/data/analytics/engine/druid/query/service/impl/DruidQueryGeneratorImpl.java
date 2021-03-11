package com.engati.data.analytics.engine.druid.query.service.impl;

import com.engati.data.analytics.engine.druid.query.druidry.DruidScanQuery;
import com.engati.data.analytics.engine.druid.query.druidry.datasource.DruidDataSource;
import com.engati.data.analytics.engine.druid.query.druidry.datasource.QueryDataSource;
import com.engati.data.analytics.engine.druid.query.druidry.datasource.TableDataSource;
import com.engati.data.analytics.engine.druid.query.druidry.join.DruidJoin;
import com.engati.data.analytics.engine.druid.query.service.DruidAggregateGenerator;
import com.engati.data.analytics.engine.druid.query.service.DruidFilterGenerator;
import com.engati.data.analytics.engine.druid.query.service.DruidPostAggregateGenerator;
import com.engati.data.analytics.engine.druid.query.service.DruidQueryGenerator;
import com.engati.data.analytics.engine.util.Utility;
import com.engati.data.analytics.sdk.druid.aggregator.DruidAggregatorMetaInfo;
import com.engati.data.analytics.sdk.druid.filters.DruidFilterMetaInfo;
import com.engati.data.analytics.sdk.druid.postaggregator.DruidPostAggregatorMetaInfo;
import com.engati.data.analytics.sdk.druid.query.DruidQueryMetaInfo;
import com.engati.data.analytics.sdk.druid.query.GroupByQueryMetaInfo;
import com.engati.data.analytics.sdk.druid.query.ScanMetaInfo;
import com.engati.data.analytics.sdk.druid.query.datasource.DataSourceType;
import com.engati.data.analytics.sdk.druid.query.datasource.QueryDataSourceType;
import com.engati.data.analytics.sdk.druid.query.datasource.SimpleDataSourceType;
import com.engati.data.analytics.sdk.druid.query.join.JoinMetaInfo;
import in.zapr.druid.druidry.aggregator.DruidAggregator;
import in.zapr.druid.druidry.filter.DruidFilter;
import in.zapr.druid.druidry.postAggregator.DruidPostAggregator;
import in.zapr.druid.druidry.query.aggregation.DruidGroupByQuery;
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
      druidAggregateMetaInfoList, Integer botRef, Integer customerId) {
    return druidAggregateGenerator.getQueryAggregators(druidAggregateMetaInfoList,
        botRef, customerId);
  }

  @Override
  public List<DruidPostAggregator> generatePostAggregator(List<DruidPostAggregatorMetaInfo>
      druidPostAggregateMetaInfoList, Integer botRef, Integer customerId) {
    return druidPostAggregateGenerator.getQueryPostAggregators(druidPostAggregateMetaInfoList,
        botRef, customerId);
  }

  @Override
  public DruidFilter generateFilters(DruidFilterMetaInfo druidFilterMetaInfoDto,
      Integer botRef, Integer customerId) {
    return druidFilterGenerator.getFiltersByType(druidFilterMetaInfoDto, botRef, customerId);
  }

  @Override
  public DruidJoin getJoinDataSource(JoinMetaInfo joinMetaInfo, Integer botRef,
      Integer customerId) {
    DruidDataSource left = getDataSource(joinMetaInfo.getLeftDataSource(),
        botRef, customerId);
    DruidDataSource right = getDataSource(joinMetaInfo.getRightDataSource(),
        botRef, customerId);
    return DruidJoin.builder().left(left).right(right)
        .rightPrefix(joinMetaInfo.getRightPrefix())
        .condition(joinMetaInfo.getJoinCondition())
        .joinType(joinMetaInfo.getJoinType()).build();
  }

  private DruidDataSource getDataSource(DataSourceType dataSource, Integer botRef,
      Integer customerId) {
    DruidDataSource druidDataSource = null;
    if (dataSource instanceof SimpleDataSourceType) {
      druidDataSource = new TableDataSource(((SimpleDataSourceType) dataSource)
          .getDataSource());
    } else if (dataSource instanceof QueryDataSourceType) {
      Object query = generateQueryFromMetaInfo(((QueryDataSourceType) dataSource)
          .getDruidQueryMetaInfo(), botRef, customerId);
      druidDataSource = new QueryDataSource(query);
    }
    return druidDataSource;
  }

  private Object generateQueryFromMetaInfo(DruidQueryMetaInfo
      druidQueryMetaInfo, Integer botRef, Integer customerId) {
    Object query = null;
    if (druidQueryMetaInfo instanceof ScanMetaInfo) {
      query = getScanQuery(((ScanMetaInfo) druidQueryMetaInfo), botRef, customerId);
    } else if (druidQueryMetaInfo instanceof GroupByQueryMetaInfo) {
      query = getGroupByQuery((GroupByQueryMetaInfo) druidQueryMetaInfo, botRef, customerId);
    }
    return query;
  }

  private DruidScanQuery getScanQuery(ScanMetaInfo scanMetaInfo, Integer botRef,
      Integer customerId) {
    return DruidScanQuery.builder()
        .dataSource(Utility.convertDataSource(botRef, customerId, scanMetaInfo.getDataSource()))
        .intervals(Utility.extractInterval(scanMetaInfo.getIntervals()))
        .columns(scanMetaInfo.getColumns()).build();
  }

  private DruidGroupByQuery getGroupByQuery(GroupByQueryMetaInfo groupByQueryMetaInfo,
      Integer botRef, Integer customerId) {
    return DruidGroupByQuery.builder()
        .dataSource(Utility.convertDataSource(botRef, customerId,
            groupByQueryMetaInfo.getDataSource()))
        .intervals(Utility.extractInterval(groupByQueryMetaInfo.getIntervals()))
        .aggregators(generateAggregators(groupByQueryMetaInfo.getDruidAggregateMetaInfo(),
            botRef, customerId))
        .postAggregators(generatePostAggregator(groupByQueryMetaInfo
            .getDruidPostAggregateMetaInfo(), botRef, customerId))
        .filter(generateFilters(groupByQueryMetaInfo.getDruidFilterMetaInfo(), botRef,
            customerId))
        .dimensions(Utility.getDimension(groupByQueryMetaInfo.getDimension()))
        .granularity(Utility.getGranularity(groupByQueryMetaInfo.getGrain()))
        .build();
  }
}
