package org.example.adapters;

import com.opencsv.CSVReader;
import com.opencsv.exceptions.CsvException;
import lombok.extern.slf4j.Slf4j;
import org.example.models.dto.UnifiedRecord;
import org.example.models.entity.Source;
import org.example.models.enums.SourceType;
import org.springframework.stereotype.Component;

import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@Slf4j
@Component
public class CSVDataAdapter implements DataSourceAdapter {

    @Override
    public List<UnifiedRecord> extract(Source source) throws Exception {
        log.info("Extracting data from CSV source: {}", source.getName());
        
        Map<String, Object> config = source.getConfig();
        String filePath = (String) config.get("filePath");
        if ((filePath == null || filePath.isEmpty()) && config.containsKey("contentAddressablePath")) {
            filePath = (String) config.get("contentAddressablePath");
        }

        if (filePath == null || filePath.isEmpty()) {
            throw new IllegalArgumentException("CSV file path not provided in source config");
        }
        
        List<UnifiedRecord> records = new ArrayList<>();
        
        try (CSVReader reader = new CSVReader(new FileReader(filePath))) {
            List<String[]> allRows = reader.readAll();
            
            if (allRows.isEmpty()) {
                log.warn("CSV file is empty: {}", filePath);
                return records;
            }
            
            String[] headers = allRows.get(0);
            
            for (int i = 1; i < allRows.size(); i++) {
                String[] row = allRows.get(i);
                UnifiedRecord record = new UnifiedRecord();
                record.setSourceIdentifier(source.getSourceUid());
                record.setRecordKey("row-" + i);
                
                for (int j = 0; j < headers.length && j < row.length; j++) {
                    String fieldName = headers[j].trim();
                    String fieldValue = row[j] != null ? row[j].trim() : null;
                    record.addField(fieldName, fieldValue);
                }
                
                records.add(record);
            }
            
            log.info("Extracted {} records from CSV: {}", records.size(), source.getName());
            
        } catch (IOException | CsvException e) {
            log.error("Error reading CSV file: {}", filePath, e);
            throw new Exception("Failed to extract data from CSV: " + e.getMessage(), e);
        }
        
        return records;
    }

    @Override
    public boolean supportsSource(Source source) {
        return source.getType() == SourceType.CSV;
    }
}
