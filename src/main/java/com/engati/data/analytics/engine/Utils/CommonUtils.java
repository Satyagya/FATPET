package com.engati.data.analytics.engine.Utils;

import com.engati.data.analytics.engine.configuration.csv.OrderedComparatorIgnoringCase;
import com.engati.data.analytics.engine.constants.constant.Constants;
import com.engati.data.analytics.engine.constants.constant.TableConstants;
import com.engati.data.analytics.engine.model.response.CustomerSegmentationResponse;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.opencsv.CSVWriter;
import com.opencsv.bean.HeaderColumnNameMappingStrategy;
import com.opencsv.bean.StatefulBeanToCsv;
import com.opencsv.bean.StatefulBeanToCsvBuilder;
import lombok.extern.slf4j.Slf4j;
import org.json.simple.JSONObject;
import org.springframework.stereotype.Component;
import org.springframework.web.multipart.MultipartFile;

import java.io.File;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.util.List;

@Slf4j
@Component
public class CommonUtils {

    public static final ObjectMapper MAPPER = new ObjectMapper();
    public static Boolean createCsv(List<CustomerSegmentationResponse> customerDetail, Long botRef,
                                   String segmentName, String fileName) {
        Boolean isCsvCreated = Boolean.TRUE;
        log.info("Creating CSV file for SegmentName: {} for BotRef: {}", segmentName, botRef);
        try (FileWriter writer = new FileWriter(fileName)) {
            HeaderColumnNameMappingStrategy<CustomerSegmentationResponse> mappingStrategy =
                    new HeaderColumnNameMappingStrategy<>();
            mappingStrategy.setType(CustomerSegmentationResponse.class);
            mappingStrategy.setColumnOrderOnWrite(new OrderedComparatorIgnoringCase(Constants.CUSTOMER_SEGMENT_HEADER));

            StatefulBeanToCsv statefulBeanToCsv = new StatefulBeanToCsvBuilder(writer)
                    .withSeparator(CSVWriter.DEFAULT_SEPARATOR)
                    .withMappingStrategy(mappingStrategy)
                    .build();
            statefulBeanToCsv.write(customerDetail);

        } catch (Exception e) {
            log.error("Error while creating CSV file for SegmentName: {} for BotRef: {}", segmentName, botRef, e);
            isCsvCreated = Boolean.FALSE;

        }
        return isCsvCreated;
    }

    public static JSONObject removeUnnecessaryKeys(JSONObject jsonObject) {
        jsonObject.remove(TableConstants.AUTH_URI);
        jsonObject.remove(TableConstants.TOKEN_URI);
        jsonObject.remove(TableConstants.AUTH_PROVIDER_CERT_URL);
        jsonObject.remove(TableConstants.CLIENT_CERT_URL);
        return jsonObject;
    }

    public static Boolean validateAuthJsonFile(JSONObject jsonObject) {
        return jsonObject.containsKey(TableConstants.TYPE) && jsonObject.containsKey(
            TableConstants.PROJECT_ID) && jsonObject.containsKey(TableConstants.PRIVATE_KEY_ID)
            && jsonObject.containsKey(TableConstants.PRIVATE_KEY) && jsonObject.containsKey(
            TableConstants.CLIENT_EMAIL) && jsonObject.containsKey(TableConstants.CLIENT_ID);
    }

}
