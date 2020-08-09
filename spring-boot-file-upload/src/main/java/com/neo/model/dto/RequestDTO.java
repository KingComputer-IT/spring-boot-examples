package com.neo.model.dto;

import lombok.Data;

@Data
public class RequestDTO {
    private String FileName;
    private Integer threadPoolSize;
    private Integer threadNum;
}
