package com.engati.data.analytics.engine.druid.query.service.impl;

import com.engati.data.analytics.engine.druid.query.druidry.aggregator.DistinctCountAggregator;
import com.engati.data.analytics.engine.druid.query.druidry.aggregator.NewCardinalityAggregator;
import com.engati.data.analytics.engine.druid.query.service.DruidAggregateGenerator;
import com.engati.data.analytics.sdk.druid.aggregator.DruidAggregatorMetaInfo;
import in.zapr.druid.druidry.aggregator.CountAggregator;
import in.zapr.druid.druidry.aggregator.DoubleSumAggregator;
import in.zapr.druid.druidry.aggregator.DruidAggregator;
import in.zapr.druid.druidry.aggregator.JavaScriptAggregator;
import in.zapr.druid.druidry.aggregator.LongSumAggregator;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;

@Slf4j
@Service
public class DruidAggregateGeneratorImpl implements DruidAggregateGenerator {

  @Override
  public List<DruidAggregator> getQueryAggregators(List<DruidAggregatorMetaInfo>
      aggregateMetaInfos, Integer botRef, Integer customerId) {

    log.debug("DruidAggregateGeneratorImpl: Generating druid aggregator from the meta-info: {} "
        + "for botRef: {} and customerId: {}", aggregateMetaInfos, botRef, customerId);
    List<DruidAggregator> druidAggregators = new ArrayList<>();;
    if (CollectionUtils.isNotEmpty(aggregateMetaInfos)) {
      for (DruidAggregatorMetaInfo druidAggregateMetaInfo : aggregateMetaInfos) {
        switch (druidAggregateMetaInfo.getType()) {
          case COUNT:
            druidAggregators.add(new CountAggregator(druidAggregateMetaInfo.getAggregatorName()));
            break;
          case LONGSUM:
            druidAggregators.add(new LongSumAggregator(druidAggregateMetaInfo
                .getAggregatorName(), druidAggregateMetaInfo.getFieldName()));
            break;
          case DOUBLESUM:
            druidAggregators.add(new DoubleSumAggregator(druidAggregateMetaInfo
                .getAggregatorName(), druidAggregateMetaInfo.getFieldName()));
            break;
          case DISTINCT_COUNT:
            druidAggregators.add(new DistinctCountAggregator(druidAggregateMetaInfo
                .getAggregatorName(), druidAggregateMetaInfo.getFieldName()));
            break;
          case CARDINALITY:
            druidAggregators.add(new NewCardinalityAggregator(druidAggregateMetaInfo
                .getAggregatorName(), druidAggregateMetaInfo.getFields(),
                druidAggregateMetaInfo.getByRow(), druidAggregateMetaInfo.getRound()));
            break;
          case JAVASCRIPT:
            druidAggregators.add(
                JavaScriptAggregator.builder().name(druidAggregateMetaInfo.getAggregatorName())
                    .fieldNames(druidAggregateMetaInfo.getFields())
                    .functionAggregate(druidAggregateMetaInfo.getFnAggregate())
                    .functionCombine(druidAggregateMetaInfo.getFnCombine())
                    .functionReset(druidAggregateMetaInfo.getFnReset()).build());
            break;
          default:
            log.error("DruidAggregateGeneratorImpl: Provided aggregator: {} does not have "
                    + "implementation for botRef: {}, customerId: {}",
                druidAggregateMetaInfo.getType(), botRef, customerId);
        }
      }
    }
    return druidAggregators;
  }
}
