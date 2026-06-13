package main.jobsubmissionservice.models;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import main.jobsubmissionservice.enums.JobType;

import java.util.Map;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class RequestDTO {
    private JobType jobType;
    private Map<String,Object> payload;
}
