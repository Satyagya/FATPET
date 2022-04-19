package com.engati.data.analytics.engine.Utils;

import com.engati.data.analytics.engine.constants.constant.Constants;
import com.engati.data.analytics.engine.model.response.CustomerSegmentationResponse;
import com.opencsv.CSVWriter;
import com.opencsv.bean.StatefulBeanToCsv;
import com.opencsv.bean.StatefulBeanToCsvBuilder;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.io.File;
import java.io.FileWriter;
import java.util.List;

@Slf4j
@Component
public class CommonUtils {

    public static String createCsv(List<CustomerSegmentationResponse> customerDetail, Long botRef, String segmentName) {
        log.info("Creating CSV file for SegmentName: {} for BotRef: {}", segmentName, botRef);
        String csvBasePath = String.format(Constants.CSV_BASE_PATH_FORMAT, botRef);
        String fileName = String.format(Constants.CSV_PATH_FORMAT, botRef, segmentName);
        File file = new File(csvBasePath);
        if(!file.exists() && !file.mkdir()) {
            return null;
        }
        try (FileWriter writer = new FileWriter(fileName)) {
            StatefulBeanToCsv statefulBeanToCsv = new StatefulBeanToCsvBuilder(writer)
                    .withSeparator(CSVWriter.DEFAULT_SEPARATOR)
                    .build();
            statefulBeanToCsv.write(customerDetail);

        } catch (Exception e) {
            log.error("Error while creating CSV file for SegmentName: {} for BotRef: {}", segmentName, botRef, e);
        }

        return fileName;
    }

}
