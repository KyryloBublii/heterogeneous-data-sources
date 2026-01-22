package org.example.service.ingestion;

import lombok.RequiredArgsConstructor;
import org.example.models.entity.IngestionRun;
import org.example.models.enums.RunStatus;
import org.example.repository.IngestionRunRepository;
import org.springframework.stereotype.Component;

import java.time.Instant;

@Component
@RequiredArgsConstructor
public class IngestionRunService {

    private final IngestionRunRepository ingestionRunRepository;

    public IngestionRun markRunning(IngestionRun run) {
        run.setRunStatus(RunStatus.RUNNING);
        run.setStartedAt(run.getStartedAt() == null ? Instant.now() : run.getStartedAt());
        return ingestionRunRepository.save(run);
    }

    public IngestionRun markSuccess(IngestionRun run, int rowsRead, int rowsStored) {
        run.setRunStatus(RunStatus.SUCCESS);
        run.setRowsRead(rowsRead);
        run.setRowsStored(rowsStored);
        run.setEndedAt(Instant.now());
        run.setErrorMessage(null);
        return ingestionRunRepository.save(run);
    }

    public IngestionRun markFailure(IngestionRun run, String message) {
        run.setRunStatus(RunStatus.FAILED);
        run.setEndedAt(Instant.now());
        run.setErrorMessage(message);
        return ingestionRunRepository.save(run);
    }
}
