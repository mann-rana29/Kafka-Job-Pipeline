package main.workerservice.consumers;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import main.workerservice.models.JobEvent;
import main.workerservice.services.JobProcessingService;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;

import java.util.concurrent.ThreadLocalRandom;

@RequiredArgsConstructor
@Slf4j
@Component
public class RetryConsumer {
    private final RedisTemplate<String,Object> redisTemplate;
    private final JobProcessingService jobProcessingService;

    @KafkaListener(topics = "jobs.retry", groupId = "pipeline-worker-retry-group")
    public void consumeRetry(
            @Payload JobEvent event,
            @Header(KafkaHeaders.OFFSET) long offset,
            @Header(KafkaHeaders.RECEIVED_PARTITION) int partition,
            Acknowledgment ack
            ){

        long waitMs = (long) (Math.pow(2,event.getAttemptNumber() -1) * 1000);
        long jitter = ThreadLocalRandom.current().nextLong(0,500);

        waitMs += jitter;

        log.info("Retry attempt {} for job {} - waited {}ms on partition {} at offset {}", event.getAttemptNumber(), event.getJobId(), waitMs, partition,offset );

        try{
            Thread.sleep(waitMs);

            jobProcessingService.processAndRoute(event,ack);
        }
        catch (InterruptedException e){
            Thread.currentThread().interrupt();
            log.error("Retry backoff delay thread was interrupted for job Id : {} ", event.getJobId(),e);
        }
    }
}
