package org.example.adapters;

import lombok.extern.slf4j.Slf4j;
import org.example.models.dto.UnifiedRecord;
import org.example.models.entity.Source;
import org.example.models.enums.SourceType;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@Slf4j
@Component
public class PostgresDataAdapter implements DataSourceAdapter {

    @Override
    public List<UnifiedRecord> extract(Source source) throws Exception {
        log.info("Extracting data from PostgreSQL source: {}", source.getName());
        
        Map<String, Object> config = source.getConfig();
        String host = (String) config.get("host");
        Integer port = config.get("port") != null ? ((Number) config.get("port")).intValue() : 5432;
        String database = (String) config.get("database");
        String username = (String) config.get("username");
        String password = (String) config.get("password");
        String tableName = (String) config.get("tableName");
        
        if (tableName == null || tableName.isEmpty()) {
            throw new IllegalArgumentException("Table name not provided in source config");
        }
        
        String jdbcUrl = String.format("jdbc:postgresql://%s:%d/%s", host, port, database);
        
        DriverManagerDataSource dataSource = new DriverManagerDataSource();
        dataSource.setDriverClassName("org.postgresql.Driver");
        dataSource.setUrl(jdbcUrl);
        dataSource.setUsername(username);
        dataSource.setPassword(password);
        
        JdbcTemplate jdbcTemplate = new JdbcTemplate(dataSource);
        
        List<UnifiedRecord> records = new ArrayList<>();
        
        try {
            String query = "SELECT * FROM " + tableName;
            
            jdbcTemplate.query(query, (rs) -> {
                UnifiedRecord record = new UnifiedRecord();
                record.setSourceIdentifier(source.getSourceUid());
                
                int columnCount = rs.getMetaData().getColumnCount();
                StringBuilder recordKey = new StringBuilder();
                
                for (int i = 1; i <= columnCount; i++) {
                    String columnName = rs.getMetaData().getColumnName(i);
                    Object columnValue = rs.getObject(i);
                    record.addField(columnName, columnValue);
                    
                    if (i == 1) {
                        recordKey.append(columnValue);
                    }
                }
                
                record.setRecordKey(recordKey.toString());
                records.add(record);
            });
            
            log.info("Extracted {} records from PostgreSQL table: {}", records.size(), tableName);
            
        } catch (Exception e) {
            log.error("Error extracting data from PostgreSQL source: {}", source.getName(), e);
            throw new Exception("Failed to extract data from PostgreSQL: " + e.getMessage(), e);
        }
        
        return records;
    }

    @Override
    public boolean supportsSource(Source source) {
        return source.getType() == SourceType.DB;
    }
}
