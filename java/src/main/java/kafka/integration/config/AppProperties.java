package kafka.integration.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "app")
public record AppProperties(
        String goServiceUrl,
        int weatherBatchSize,
        int aqBatchSize
) {}
