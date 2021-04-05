package com.zjh.wf.flink.domain.service;

import com.zjh.wf.flink.domain.model.FlinkJobParamsInfo;

public abstract class ExecutorService {
    public abstract String submit(FlinkJobParamsInfo flinkJobParamsInfo) throws Exception;
}
