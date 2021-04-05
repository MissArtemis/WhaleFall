package com.zjh.wf.flink.domain.service;

import com.zjh.wf.flink.domain.model.FlinkJobParamsInfo;
import org.apache.flink.client.deployment.ClusterDescriptor;
import org.apache.flink.client.deployment.StandaloneClusterDescriptor;
import org.apache.flink.client.deployment.StandaloneClusterId;
import org.apache.flink.client.program.ClusterClient;
import org.apache.flink.client.program.ClusterClientProvider;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.GlobalConfiguration;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.util.concurrent.CompletableFuture;

@Service
public class StandaloneExecutorService extends ExecutorService{

    @Autowired
    private JobGraphBuildService jobGraphBuildService;

    @Value("${flink.jar-path}")
    private String flinkJarPath;

    @Override
    public String submit(FlinkJobParamsInfo flinkJobParamsInfo) throws Exception {
        JobGraph jobGraph = jobGraphBuildService.build(flinkJobParamsInfo);
        Configuration configuration = GlobalConfiguration.loadConfiguration(flinkJarPath);
        ClusterDescriptor clusterDescriptor = new StandaloneClusterDescriptor(configuration);
        ClusterClientProvider clusterClientProvider =
                clusterDescriptor.retrieve(StandaloneClusterId.getInstance());
        ClusterClient clusterClient = clusterClientProvider.getClusterClient();
        String appId = clusterClient.getClusterId().toString();
        clusterClient.submitJob(jobGraph).whenCompleteAsync((a, b) -> clusterClient.close());
        return appId;
    }
}
