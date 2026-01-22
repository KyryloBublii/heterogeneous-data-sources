package org.example.service.ingestion;

import org.example.models.entity.Source;

import java.util.List;
import java.util.Map;

public interface RecordExtractor {

    boolean supports(String format);

    List<Map<String, Object>> extract(Source source, Map<String, Object> config);
}
