package com.engati.data.analytics.engine.druid.query;

import com.engati.data.analytics.engine.druid.query.service.impl.DruidAggregateGeneratorImpl;
import com.engati.data.analytics.sdk.druid.aggregator.DruidAggregatorMetaInfo;
import com.engati.data.analytics.sdk.druid.aggregator.DruidAggregatorType;
import in.zapr.druid.druidry.aggregator.DruidAggregator;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.MockitoAnnotations;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class DruidAggregatorImplTest {

  @InjectMocks
  private DruidAggregateGeneratorImpl druidAggregateGenerator;

  @Before
  public void setup() {
    MockitoAnnotations.initMocks(this);
  }

  @Test
  public void druidAggregator_success() {
    Integer botRef = 24075;
    Integer customerId = 5234;
    List<DruidAggregatorMetaInfo> aggregatorMetaInfo = new ArrayList<>();
    aggregatorMetaInfo.add(DruidAggregatorMetaInfo.builder()
        .aggregatorName("row_count").type(DruidAggregatorType.COUNT).build());
    aggregatorMetaInfo.add(DruidAggregatorMetaInfo.builder()
        .aggregatorName("order_count").fields(Collections.singletonList("name")).byRow(false)
        .round(true).type(DruidAggregatorType.CARDINALITY).build());
    List<DruidAggregator> actualResponse = druidAggregateGenerator
        .getQueryAggregators(aggregatorMetaInfo, botRef, customerId);
    Assert.assertEquals(actualResponse.size(), 2);
    Assert.assertEquals(actualResponse.get(0).getType(), "count");
    Assert.assertEquals(actualResponse.get(0).getName(), "row_count");
    Assert.assertEquals(actualResponse.get(1).getType(), "cardinality");
    Assert.assertEquals(actualResponse.get(1).getName(), "order_count");
  }

  @Test
  public void druidAggregator_null() {
    Integer botRef = 24075;
    Integer customerId = 5234;
    List<DruidAggregatorMetaInfo> aggregatorMetaInfo = null;
    List<DruidAggregator> actualResponse = druidAggregateGenerator
        .getQueryAggregators(aggregatorMetaInfo, botRef, customerId);
    Assert.assertEquals(actualResponse, Collections.EMPTY_LIST);
  }
}
