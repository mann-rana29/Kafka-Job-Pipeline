package main.jobsubmissionservice.enums;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;

public enum JobStatus {
    PENDING,
    PROCESSING,
    DONE,
    FAILED;

    @JsonValue
    public String toValue(){
        return this.name();
    }

    @JsonCreator
    public static JobStatus fromValue(String value){
        if(value == null) return null;

        String normalized = value.trim().toUpperCase().replace("-","_").replace(" ", "_");

        for(JobStatus jobStatus : JobStatus.values()){
            if(jobStatus.name().equals(normalized)) return jobStatus;
        }

        throw new IllegalArgumentException("Unknown Job status : " + value);
    }
}

