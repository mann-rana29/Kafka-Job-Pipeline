package main.jobsubmissionservice.models;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import main.jobsubmissionservice.enums.JobStatus;
import main.jobsubmissionservice.enums.JobType;

import java.util.Map;
import java.util.UUID;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class JobEvent {
    private UUID jobId;
    private JobType jobType;
    private JobStatus jobStatus;
    private String submittedBy;
    private long submittedAt;
    private int attemptNumber;
    private int maxAttempts;
    private Map<String,Object> payload;
    private String lastError;
}
