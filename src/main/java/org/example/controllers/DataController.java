package org.example.controllers;

import org.example.models.entity.IngestionRun;
import org.example.service.DataService;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.security.core.Authentication;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.server.ResponseStatusException;

import java.util.List;

@RestController
@RequestMapping("/api/transform")
@CrossOrigin(origins = "*")
public class DataController {

    private final DataService dataService;

    public DataController(DataService dataService) {
        this.dataService = dataService;
    }

    @GetMapping("/runs")
    public ResponseEntity<List<IngestionRun>> getAllRuns(Authentication authentication) {
        return ResponseEntity.ok(dataService.getAllRuns(requireUserEmail(authentication)));
    }

    @GetMapping("/runs/recent")
    public ResponseEntity<List<IngestionRun>> getRecentRuns(@RequestParam(defaultValue = "10") int limit,
                                                            Authentication authentication) {
        return ResponseEntity.ok(dataService.getRecentRuns(requireUserEmail(authentication), limit));
    }

    private String requireUserEmail(Authentication authentication) {
        if (authentication == null || !authentication.isAuthenticated()) {
            throw new ResponseStatusException(HttpStatus.UNAUTHORIZED, "Authentication required");
        }
        return authentication.getName();
    }
}
