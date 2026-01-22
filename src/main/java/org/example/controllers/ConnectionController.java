package org.example.controllers;

import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import org.example.models.dto.ConnectionRequest;
import org.example.models.dto.ConnectionResponse;
import org.example.models.dto.TableSelectionUpdateRequest;
import org.example.service.ConnectionService;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.security.core.Authentication;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.server.ResponseStatusException;
import java.util.List;

@RestController
@RequestMapping("/api/connections")
@CrossOrigin(origins = "*")
@RequiredArgsConstructor
public class ConnectionController {

    private final ConnectionService connectionService;

    @GetMapping
    public ResponseEntity<List<ConnectionResponse>> listConnections(@RequestParam(value = "datasetId", required = false) Long datasetId,
                                                                    Authentication authentication) {
        return ResponseEntity.ok(connectionService.listConnections(requireUserEmail(authentication), datasetId));
    }

    @PostMapping
    public ResponseEntity<ConnectionResponse> createConnection(@Valid @RequestBody ConnectionRequest request,
                                                               Authentication authentication) {
        String userEmail = requireUserEmail(authentication);
        String createdBy = authentication.getName();
        return ResponseEntity.ok(connectionService.createConnection(request, userEmail, createdBy));
    }

    @PostMapping("/{connectionId}/table-selection")
    public ResponseEntity<ConnectionResponse> updateTableSelection(@PathVariable("connectionId") String connectionId,
                                                                   @Valid @RequestBody TableSelectionUpdateRequest request,
                                                                   Authentication authentication) {
        String userEmail = requireUserEmail(authentication);
        return ResponseEntity.ok(connectionService.updateTableSelection(connectionId, request, userEmail));
    }

    private String requireUserEmail(Authentication authentication) {
        if (authentication == null || !authentication.isAuthenticated()) {
            throw new ResponseStatusException(HttpStatus.UNAUTHORIZED, "Authentication required");
        }
        return authentication.getName();
    }
}
