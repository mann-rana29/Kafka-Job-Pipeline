package main.jobsubmissionservice.enums;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;

public enum JobType {
    PDF_GENERATION,
    IMAGE_RESIZE,
    DATA_EXPORT;

    @JsonValue
    public String toValue(){
        return this.name();
    }

    @JsonCreator
    public static JobType fromValue(String value){
        if(value == null) return null;

        String normalized = value.trim().toUpperCase().replace("-","_").replace(" ", _);

        for(JobType type : JobType.values()){
            if(type.name().equals(normalized)){
                return type;
            }
        }

        throw new IllegalArgumentException("Unknown Job Tyoe : " + value);
    }
}
