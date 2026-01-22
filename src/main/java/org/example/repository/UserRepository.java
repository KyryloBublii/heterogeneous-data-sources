package org.example.repository;

import org.example.models.entity.ApplicationUser;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.util.Optional;

@Repository
public interface UserRepository extends JpaRepository<ApplicationUser, Long> {
    void deleteByUserUid(String id);

    Optional<ApplicationUser> findByEmail(String email);
}