package org.example.adapters;

import org.example.models.dto.UnifiedRecord;
import org.example.models.entity.Source;

import java.util.List;

public interface DataSourceAdapter {
    List<UnifiedRecord> extract(Source source) throws Exception;
    boolean supportsSource(Source source);
}
