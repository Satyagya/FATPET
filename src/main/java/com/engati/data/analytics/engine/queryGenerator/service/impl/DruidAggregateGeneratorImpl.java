package com.engati.data.analytics.engine.queryGenerator.service.impl;

import com.engati.data.analytics.engine.queryGenerator.druidry.aggregator.DistinctCountAggregator;
import com.engati.data.analytics.engine.queryGenerator.druidry.aggregator.NewCardinalityAggregator;
import com.engati.data.analytics.engine.queryGenerator.service.DruidAggregateGenerator;
import com.engati.data.analytics.sdk.druid.aggregator.CardinalityAggregatorMetaInfo;
import com.engati.data.analytics.sdk.druid.aggregator.DruidAggregatorMetaInfo;
import com.engati.data.analytics.sdk.druid.aggregator.JavaScriptAggregatorMetaInfo;
import in.zapr.druid.druidry.aggregator.CountAggregator;
import in.zapr.druid.druidry.aggregator.DoubleSumAggregator;
import in.zapr.druid.druidry.aggregator.DruidAggregator;
import in.zapr.druid.druidry.aggregator.JavaScriptAggregator;
import in.zapr.druid.druidry.aggregator.LongSumAggregator;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

@Slf4j
@Service
public class DruidAggregateGeneratorImpl implements DruidAggregateGenerator {

  @Override
  public List<DruidAggregator> getQueryAggregators(List<DruidAggregatorMetaInfo> aggregateMetaInfos) {

    log.info("DruidAggregators: create list of aggregators");
    List<DruidAggregator> druidAggregators = new ArrayList<>();
    for(DruidAggregatorMetaInfo druidAggregateMetaInfo: aggregateMetaInfos) {
      switch (druidAggregateMetaInfo.getType()) {
        case COUNT:
          druidAggregators.add(new CountAggregator(druidAggregateMetaInfo.getNames()));
          break;
        case LONGSUM:
          druidAggregators.add(new LongSumAggregator(druidAggregateMetaInfo.getNames(),
              druidAggregateMetaInfo.getFieldNames()));
          break;
        case DOUBLESUM:
          druidAggregators.add(new DoubleSumAggregator(druidAggregateMetaInfo.getNames(),
              druidAggregateMetaInfo.getFieldNames()));
          break;
        case DISTINCT_COUNT:
          druidAggregators.add(new DistinctCountAggregator(druidAggregateMetaInfo.getNames(),
              druidAggregateMetaInfo.getFieldNames()));
          break;
        case CARDINALITY:
          CardinalityAggregatorMetaInfo cardinalityAggregatorMetaInfo =
              ((CardinalityAggregatorMetaInfo) druidAggregateMetaInfo);
          druidAggregators.add(new NewCardinalityAggregator(cardinalityAggregatorMetaInfo.getNames(),
              cardinalityAggregatorMetaInfo.getFields(), cardinalityAggregatorMetaInfo.getByRow(),
              cardinalityAggregatorMetaInfo.getRound()));
          break;
        case JAVASCRIPT:
          JavaScriptAggregatorMetaInfo javaScriptAggregatorMetaInfo =
              ((JavaScriptAggregatorMetaInfo) druidAggregateMetaInfo);
          druidAggregators.add(JavaScriptAggregator.builder()
              .name(javaScriptAggregatorMetaInfo.getNames())
              .fieldNames(javaScriptAggregatorMetaInfo.getFields())
              .functionAggregate(javaScriptAggregatorMetaInfo.getFnAggregate())
              .functionCombine(javaScriptAggregatorMetaInfo.getFnCombine())
              .functionReset(javaScriptAggregatorMetaInfo.getFnReset()).build());
          break;
        default:
          log.error("The given aggregator: {} does not have implementation",
              druidAggregateMetaInfo.getType());
      }
    }
    return druidAggregators;
  }
}
