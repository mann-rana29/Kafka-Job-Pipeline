package main.workerservice.consumers;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import main.workerservice.enums.JobStatus;
import main.workerservice.enums.JobType;
import main.workerservice.models.JobEvent;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;

@Component
@Slf4j
@RequiredArgsConstructor
public class JobConsumer {
    private final RedisTemplate<String,Object> redisTemplate;
    private final KafkaTemplate<String,JobEvent> kafkaTemplate;

    @KafkaListener(topics = "jobs", groupId = "pipeline-worker-group")
    public void consume(
            @Payload JobEvent event,
            @Header(KafkaHeaders.RECEIVED_PARTITION) int partition,
            @Header(KafkaHeaders.OFFSET) long offset,
            Acknowledgment ack
    ){
        String redisKey = "job:" + event.getJobId();

        String currentStatus = (String) redisTemplate.opsForHash().get(redisKey,"status");
        if("DONE".equals(currentStatus) || "FAILED".equals(currentStatus)){
            log.info("Job {} already processed with status: {}. Skipping Execution.", event.getJobId(), currentStatus);
            ack.acknowledge();
            return;
        }
        try{
            log.info("Successfully pulled job {} from kafka partition", event.getJobId());

            redisTemplate.opsForHash().put(redisKey,"status",JobStatus.PROCESSING.name());
            log.info("Job {} state updated to PROCESSING in Redis", event.getJobId());

            // simulating a process
            processJob(event);

            if(event.getJobId().toString().endsWith("0")) throw new RuntimeException("Simulated processing failed!");

            redisTemplate.opsForHash().put(redisKey,"status",JobStatus.DONE.name());
            String outputUrl = String.format("https://storage/%s/%s.output",event.getJobType() , event.getJobId());
            redisTemplate.opsForHash().put(redisKey,"result",outputUrl);
            log.info("Job {} successfully completed processing! Output URL stored. ", event.getJobId());

            ack.acknowledge();
        }
        catch (RuntimeException e){
            log.error("Processing failed for job Id: {}",event.getJobId());

            int currentAttempts = event.getAttemptNumber();
            int maxAttempts = event.getMaxAttempts();

            if(currentAttempts < maxAttempts){
                event.setAttemptNumber(currentAttempts+1);
                event.setLastError(e.getMessage());

                kafkaTemplate.send("jobs.retry",event.getJobId().toString(),event);
                log.info("Job Id {} failed (Attempt {} / {}. Forwarded to jobs.retry.", event.getJobId(),event.getAttemptNumber(),event.getMaxAttempts());

                redisTemplate.opsForHash().put(redisKey,"status", JobStatus.PENDING.name());
                ack.acknowledge();
            }
            else{
                event.setLastError(e.getMessage());

                kafkaTemplate.send("jobs.dlq", event.getJobId().toString(),event);
                log.error("Job Id: {} completely failed after {} attempts. Forwarded to jobs.dlq", event.getJobId(), maxAttempts);

                redisTemplate.opsForHash().put(redisKey,"status",JobStatus.FAILED.name());
                redisTemplate.opsForHash().put(redisKey,"error",e.getMessage());
                ack.acknowledge();

            }
        }
    }

    private void processJob(JobEvent event) throws InterruptedException{
        switch (event.getJobType()){
            case PDF_GENERATION :
                log.info("Processing PDF Generation task for job Id: {}",event.getJobId());
                Thread.sleep(4000);
                break;

            case IMAGE_RESIZE :
                log.info("Resizing image for Job id: {}",event.getJobId());
                Thread.sleep(10000);
                break;

            case DATA_EXPORT :
                log.info("Processing Data Export task for job Id: {}",event.getJobId());
                Thread.sleep(2000);
                break;

            default:
                throw new IllegalArgumentException("Unsupported Job execution path: " + event.getJobType());
        }
    }
}
