package org.example.service;

import lombok.RequiredArgsConstructor;
import org.example.models.entity.ApplicationUser;
import org.example.models.entity.IngestionRun;
import org.example.repository.IngestionRunRepository;
import org.example.repository.UserRepository;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Sort;
import org.springframework.stereotype.Service;
import org.springframework.util.StringUtils;

import java.util.List;

@Service
@RequiredArgsConstructor
public class DataService {

    private static final Sort DEFAULT_SORT = Sort.by(Sort.Direction.DESC, "startedAt");

    private final IngestionRunRepository ingestionRunRepository;
    private final UserRepository userRepository;

    public List<IngestionRun> getRecentRuns(String userEmail, int limit) {
        ApplicationUser user = requireUser(userEmail);
        int pageSize = Math.max(limit, 1);
        Pageable pageable = PageRequest.of(0, pageSize, DEFAULT_SORT);
        return ingestionRunRepository.findBySource_ApplicationUser_Id(user.getId(), pageable).getContent();
    }

    public List<IngestionRun> getAllRuns(String userEmail) {
        ApplicationUser user = requireUser(userEmail);
        return ingestionRunRepository.findAllBySource_ApplicationUser_Id(user.getId(), DEFAULT_SORT);
    }

    private ApplicationUser requireUser(String userEmail) {
        if (!StringUtils.hasText(userEmail)) {
            throw new IllegalArgumentException("User email is required");
        }
        return userRepository.findByEmail(userEmail)
                .orElseThrow(() -> new IllegalArgumentException("User not found: " + userEmail));
    }
}