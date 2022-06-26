package com.engati.data.analytics.engine.Utils;

import com.engati.data.analytics.engine.configuration.csv.OrderedComparatorIgnoringCase;
import com.engati.data.analytics.engine.constants.constant.Constants;
import com.engati.data.analytics.engine.model.response.CustomerSegmentationResponse;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.opencsv.CSVWriter;
import com.opencsv.bean.HeaderColumnNameMappingStrategy;
import com.opencsv.bean.StatefulBeanToCsv;
import com.opencsv.bean.StatefulBeanToCsvBuilder;
import lombok.extern.slf4j.Slf4j;
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

    public static File convertMultiPartToFile(MultipartFile file) throws IOException {
        File convFile = new File(file.getOriginalFilename());
        FileOutputStream fos = new FileOutputStream(convFile);
        fos.write(file.getBytes());
        fos.close();
        return convFile;
    }

}
