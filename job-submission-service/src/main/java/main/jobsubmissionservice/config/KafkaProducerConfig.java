package main.jobsubmissionservice.config;

import main.jobsubmissionservice.models.JobEvent;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.support.serializer.JsonSerializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.HashMap;
import java.util.Map;

@Configuration
public class KafkaProducerConfig {

    @Value("${spring.kafka.bootstrap-servers:localhost:9092}")
    private String bootstrapServers;

    @Bean
    public ProducerFactory<String, JobEvent> producerFactory(){
        Map<String , Object> configProps =  new HashMap<>();

        configProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,bootstrapServers);

        configProps.put(ProducerConfig.ACKS_CONFIG,"all");
        configProps.put(ProducerConfig.RETRIES_CONFIG,3);
        configProps.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG,true);

        return new DefaultKafkaProducerFactory<>(configProps, new StringSerializer(), new JsonSerializer<JobEvent>());
    }

    @Bean
    public KafkaTemplate<String, JobEvent> kafkaTemplate(){
        return new KafkaTemplate<>(producerFactory());
    }
}
