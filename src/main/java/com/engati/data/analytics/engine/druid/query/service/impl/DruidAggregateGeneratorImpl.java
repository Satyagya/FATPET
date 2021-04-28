package com.engati.data.analytics.engine.druid.query.service.impl;

import com.engati.data.analytics.engine.druid.query.druidry.aggregator.DistinctCountAggregator;
import com.engati.data.analytics.engine.druid.query.druidry.aggregator.NewCardinalityAggregator;
import com.engati.data.analytics.engine.druid.query.service.DruidAggregateGenerator;
import com.engati.data.analytics.engine.druid.query.service.DruidFilterGenerator;
import com.engati.data.analytics.sdk.druid.aggregator.DruidAggregatorMetaInfo;
import in.zapr.druid.druidry.aggregator.CountAggregator;
import in.zapr.druid.druidry.aggregator.DoubleSumAggregator;
import in.zapr.druid.druidry.aggregator.DruidAggregator;
import in.zapr.druid.druidry.aggregator.FilteredAggregator;
import in.zapr.druid.druidry.aggregator.JavaScriptAggregator;
import in.zapr.druid.druidry.aggregator.LongSumAggregator;
import in.zapr.druid.druidry.filter.DruidFilter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

@Slf4j
@Service
public class DruidAggregateGeneratorImpl implements DruidAggregateGenerator {

  @Autowired
  private DruidFilterGenerator druidFilterGenerator;

  @Override
  public List<DruidAggregator> getQueryAggregators(List<DruidAggregatorMetaInfo> aggregateMetaInfos,
      Integer botRef, Integer customerId) {

    log.debug("DruidAggregateGeneratorImpl: Generating druid aggregator from the meta-info: {} "
        + "for botRef: {} and customerId: {}", aggregateMetaInfos, botRef, customerId);
    List<DruidAggregator> druidAggregators = new ArrayList<>();
    ;
    if (CollectionUtils.isNotEmpty(aggregateMetaInfos)) {
      for (DruidAggregatorMetaInfo druidAggregateMetaInfo : aggregateMetaInfos) {

        DruidAggregator druidAggregator =
            generateDruidAggregatorFromMetaInfo(druidAggregateMetaInfo, botRef, customerId);
        if (Objects.nonNull(druidAggregator)) {
          druidAggregators.add(druidAggregator);
        }
      }
    }
    return druidAggregators;
  }

  private DruidAggregator generateDruidAggregatorFromMetaInfo(
      DruidAggregatorMetaInfo druidAggregateMetaInfo, Integer botRef, Integer customerId) {

    switch (druidAggregateMetaInfo.getType()) {
      case COUNT:
        return new CountAggregator(druidAggregateMetaInfo.getAggregatorName());
      case LONGSUM:
        return new LongSumAggregator(druidAggregateMetaInfo.getAggregatorName(),
            druidAggregateMetaInfo.getFieldName());
      case DOUBLESUM:
        return new DoubleSumAggregator(druidAggregateMetaInfo.getAggregatorName(),
            druidAggregateMetaInfo.getFieldName());
      case DISTINCT_COUNT:
        return new DistinctCountAggregator(druidAggregateMetaInfo.getAggregatorName(),
            druidAggregateMetaInfo.getFieldName());
      case CARDINALITY:
        return new NewCardinalityAggregator(druidAggregateMetaInfo.getAggregatorName(),
            druidAggregateMetaInfo.getFields(), druidAggregateMetaInfo.getByRow(),
            druidAggregateMetaInfo.getRound());
      case JAVASCRIPT:
        return JavaScriptAggregator.builder().name(druidAggregateMetaInfo.getAggregatorName())
            .fieldNames(druidAggregateMetaInfo.getFields())
            .functionAggregate(druidAggregateMetaInfo.getFnAggregate())
            .functionCombine(druidAggregateMetaInfo.getFnCombine())
            .functionReset(druidAggregateMetaInfo.getFnReset()).build();
      case FILTERED:
        DruidFilter druidFilter = druidFilterGenerator
            .getFiltersByType(druidAggregateMetaInfo.getFilter(), botRef, customerId);

        DruidAggregator nestedDruidAggregator =
            generateDruidAggregatorFromMetaInfo(druidAggregateMetaInfo.getAggregator(), botRef,
                customerId);
        if (Objects.nonNull(nestedDruidAggregator)) {
          return new FilteredAggregator(druidFilter, nestedDruidAggregator);
        } else {
          log.error(
              "DruidAggregateGeneratorImpl: Nested aggregator for FilteredAggregator: {} does not"
                  + " have "
                  + "implementation for botRef: {}, customerId: {}",
              druidAggregateMetaInfo.getType(), botRef, customerId);
          return null;
        }

      default:
        log.error("DruidAggregateGeneratorImpl: Provided aggregator: {} does not have "
                + "implementation for botRef: {}, customerId: {}", druidAggregateMetaInfo.getType(),
            botRef, customerId);
        return null;
    }
  }
}
