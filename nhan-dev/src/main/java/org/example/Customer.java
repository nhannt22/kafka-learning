package org.example;

import com.opencsv.CSVReader;
import com.opencsv.exceptions.CsvException;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileReader;
import java.io.IOException;
import java.time.Instant;
import java.util.List;
import java.util.Properties;

import java.time.ZoneOffset;
import java.time.ZonedDateTime;

public class Customer {

    private static final Logger log = LoggerFactory.getLogger(Customer.class);

    public static void main(String[] args) {
        log.info("Starting Kafka Producer...");

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "54.169.46.195:19092");
        properties.setProperty("key.serializer", StringSerializer.class.getName());
        properties.setProperty("value.serializer", StringSerializer.class.getName());

        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        String filePath = "src/main/resources/olist_customers_dataset.csv";
        List<String[]> records = readCsvFile(filePath);

        if (records != null && records.size() > 1) {
            String[] headers = addProcessingTimestampHeader(records.get(0));
            for (int i = 1; i < records.size(); i++) {
                String[] record = addProcessingTimestampValue(records.get(i));
                JSONObject jsonObject = new JSONObject();
                for (int j = 0; j < headers.length; j++) {
                    jsonObject.put(headers[j], record[j]);
                }
                ProducerRecord<String, String> producerRecord = new ProducerRecord<>("customer", jsonObject.toString());
                producer.send(producerRecord);
            }
        }

        producer.flush();
        producer.close();
        log.info("Kafka Producer finished.");
    }

    private static List<String[]> readCsvFile(String filePath) {
        try (CSVReader reader = new CSVReader(new FileReader(filePath))) {
            return reader.readAll();
        } catch (IOException | CsvException e) {
            log.error("Error reading CSV file: ", e);
            return null;
        }
    }

    private static String[] addProcessingTimestampHeader(String[] headers) {
        String[] newHeaders = new String[headers.length + 1];
        newHeaders[0] = "processing_timestamp";
        System.arraycopy(headers, 0, newHeaders, 1, headers.length);
        return newHeaders;
    }

    private static String[] addProcessingTimestampValue(String[] record) {
        String[] newRecord = new String[record.length + 1];
        // Get current time in UTC
        newRecord[0] = ZonedDateTime.now(ZoneOffset.UTC).toString();
        System.arraycopy(record, 0, newRecord, 1, record.length);
        return newRecord;
    }
}
