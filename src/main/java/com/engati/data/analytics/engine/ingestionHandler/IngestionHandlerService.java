package com.engati.data.analytics.engine.ingestionHandler;

import com.engati.data.analytics.engine.response.ingestion.IngestionResponse;
import com.engati.data.analytics.engine.response.ingestion.UserIngestionProcess;

public interface IngestionHandlerService {

  IngestionResponse<UserIngestionProcess> ingestToDruid(Long customerId, Long botRef,
      String timestamp, String dataSourceName, Boolean isInitialLoad);
}
