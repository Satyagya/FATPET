package com.engati.data.analytics.engine.druid.query;

import com.engati.data.analytics.engine.druid.query.service.DruidFilterGenerator;
import com.engati.data.analytics.engine.druid.query.service.impl.DruidAggregateGeneratorImpl;
import com.engati.data.analytics.sdk.druid.aggregator.DruidAggregatorMetaInfo;
import com.engati.data.analytics.sdk.druid.aggregator.DruidAggregatorType;
import com.engati.data.analytics.sdk.druid.filters.DruidFilterMetaInfo;
import com.engati.data.analytics.sdk.druid.filters.DruidFilterType;
import in.zapr.druid.druidry.aggregator.DruidAggregator;
import in.zapr.druid.druidry.aggregator.FilteredAggregator;
import in.zapr.druid.druidry.filter.SelectorFilter;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class DruidAggregatorImplTest {

  @InjectMocks
  private DruidAggregateGeneratorImpl druidAggregateGenerator;

  @Mock
  private DruidFilterGenerator druidFilterGenerator;

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
  public void filteredDruidAggregator_success() {
    Integer botRef = 24075;
    Integer customerId = 5234;
    List<DruidAggregatorMetaInfo> aggregatorMetaInfoList = new ArrayList<>();

    DruidFilterMetaInfo druidFilterMetaInfo =
        DruidFilterMetaInfo.builder().dimension("source").value("google.com")
            .type(DruidFilterType.SELECTOR).build();
    DruidAggregatorMetaInfo aggregatorMetaInfo =
        DruidAggregatorMetaInfo.builder().aggregatorName("order_count")
            .fields(Collections.singletonList("name")).byRow(false).round(true)
            .type(DruidAggregatorType.CARDINALITY).build();
    DruidAggregatorMetaInfo filteredAggregatorMetaInfo =
        DruidAggregatorMetaInfo.builder().aggregatorName("order_count_filtered")
            .type(DruidAggregatorType.FILTERED).aggregator(aggregatorMetaInfo)
            .filter(druidFilterMetaInfo).build();


    aggregatorMetaInfoList.add(filteredAggregatorMetaInfo);


    SelectorFilter druidFilter = new SelectorFilter("source", "google.com");
    Mockito.when(druidFilterGenerator
        .getFiltersByType(Mockito.any(DruidFilterMetaInfo.class), Mockito.anyInt(),
            Mockito.anyInt())).thenReturn(druidFilter);

    List<DruidAggregator> actualResponse =
        druidAggregateGenerator.getQueryAggregators(aggregatorMetaInfoList, botRef, customerId);

    Assert.assertEquals(actualResponse.size(), 1);
    Assert.assertEquals(actualResponse.get(0).getType(), "filtered");
    Assert.assertTrue(actualResponse.get(0) instanceof FilteredAggregator);
    FilteredAggregator filteredAggregator = (FilteredAggregator)actualResponse.get(0);
    Assert.assertEquals(filteredAggregator.getAggregator().getType(), "cardinality");
    Assert.assertEquals(filteredAggregator.getAggregator().getName(), "order_count");
    Assert.assertEquals(filteredAggregator.getFilter(), druidFilter);
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
