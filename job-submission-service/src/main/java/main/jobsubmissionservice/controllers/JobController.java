package main.jobsubmissionservice.controllers;

import main.jobsubmissionservice.enums.JobType;
import main.jobsubmissionservice.models.RequestDTO;
import main.jobsubmissionservice.services.JobSubmissionService;
import org.apache.kafka.common.errors.ResourceNotFoundException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.Map;

@RestController
public class JobController {

    @Autowired
    private JobSubmissionService jobSubmissionService;

    @ExceptionHandler(IllegalArgumentException.class)
    public ResponseEntity<Map<String, String>> handleInvalidEnum(IllegalArgumentException ex) {
        return ResponseEntity.status(HttpStatus.BAD_REQUEST)
                .body(Map.of("error", "Bad Request", "message", ex.getMessage()));
    }

    @ExceptionHandler(ResourceNotFoundException.class)
    public ResponseEntity<Map<String, String>> handleNotFound(ResourceNotFoundException ex) {
        return ResponseEntity.status(HttpStatus.NOT_FOUND)
                .body(Map.of("error", "Not Found", "message", ex.getMessage()));
    }

    @PostMapping("/api/jobs")
    public ResponseEntity<Map<String,Object>> createJob(@RequestBody RequestDTO request){
        String jobId = jobSubmissionService.submitJob(request);

        return ResponseEntity.status(HttpStatus.CREATED).body(Map.of("jobId",jobId));
    }

    @GetMapping("/api/jobs/{id}/status")
    public ResponseEntity<Map<Object,Object>> getJobStatus(@PathVariable String id){
        return ResponseEntity.status(HttpStatus.OK).body(jobSubmissionService.getJobStatus(id));
    }
}
