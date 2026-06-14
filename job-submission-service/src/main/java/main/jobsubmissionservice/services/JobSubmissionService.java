package main.jobsubmissionservice.services;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import main.jobsubmissionservice.enums.JobStatus;
import main.jobsubmissionservice.enums.JobType;
import main.jobsubmissionservice.models.JobEvent;
import main.jobsubmissionservice.models.RequestDTO;
import org.apache.kafka.common.errors.ResourceNotFoundException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

@Service
@Slf4j
@RequiredArgsConstructor
public class JobSubmissionService {

    private final RedisTemplate<String,Object> redisTemplate;
    private final KafkaTemplate<String,JobEvent> kafkaTemplate;

    public String submitJob(RequestDTO request){

        UUID jobId = UUID.randomUUID();

        String redisKey = "job:"+jobId;
        long now = System.currentTimeMillis();

        Map<String,String> hashFields = new HashMap<>();
        hashFields.put("status",JobStatus.PENDING.name());
        hashFields.put("jobType", request.getJobType().name());
        hashFields.put("submittedAt", String.valueOf(now));

        redisTemplate.opsForHash().putAll(redisKey,hashFields);
        log.info("Saved initial job state to Redis for Id : {}", jobId);

        JobEvent event = new JobEvent(
                jobId,
                request.getJobType(),
                JobStatus.PENDING,
                "system_user",
                now,
                1,
                3,
                request.getPayload(),
                null
        );

        kafkaTemplate.send("jobs",request.getJobType().name(),event)
                .whenComplete((result,ex) -> {
                    if(ex != null){
                        log.error("Failed to stream job {} to Kafka", jobId,ex);
                    }
                    else {
                        log.info("Job {} successfully published to partition {} at offset {}", jobId, result.getRecordMetadata().partition(), result.getRecordMetadata().offset());
                    }
                });

        return jobId.toString();
    }

    public Map<Object,Object> getJobStatus(String jobId){
        String redisKey = "job:" + jobId;

        Map<Object,Object> jobDetails = redisTemplate.opsForHash().entries(redisKey);

        if(jobDetails.isEmpty()){
            throw new ResourceNotFoundException("Job not found with ID: " + jobId);
        }

        return jobDetails;
    }
}
