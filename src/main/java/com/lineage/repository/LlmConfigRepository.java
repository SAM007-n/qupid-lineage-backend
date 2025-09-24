package com.lineage.repository;

import com.lineage.entity.LlmConfig;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.Optional;
import java.util.UUID;

@Repository
public interface LlmConfigRepository extends JpaRepository<LlmConfig, UUID> {
    
    /**
     * Find LLM config by provider and name
     */
    Optional<LlmConfig> findByProviderAndName(String provider, String name);
    
    /**
     * Find all LLM configs by provider
     */
    List<LlmConfig> findByProvider(String provider);
    
    /**
     * Find all active LLM configs
     */
    List<LlmConfig> findByIsActiveTrue();
    
    /**
     * Find active LLM config by provider and name
     */
    Optional<LlmConfig> findByProviderAndNameAndIsActiveTrue(String provider, String name);
    
    /**
     * Find the default (first active) LLM config for a provider
     */
    @Query("SELECT l FROM LlmConfig l WHERE l.provider = :provider AND l.isActive = true ORDER BY l.createdAt ASC")
    Optional<LlmConfig> findDefaultByProvider(@Param("provider") String provider);
    
    /**
     * Check if a provider/name combination exists
     */
    boolean existsByProviderAndName(String provider, String name);
}
