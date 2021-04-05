package com.zjh.wf.flink.domain.model;

import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;
import org.springframework.context.annotation.Bean;

import java.util.Properties;

@Builder
@Getter
@NoArgsConstructor
public class FlinkJobParamsInfo {
    private String name;
    private String queue;
    private String runMode;
    private String yarnConfDir;
    private String[] dependFile;
    private String args;
    private Properties confProperties;
    private Properties yarnSessionConfProperties;
}
