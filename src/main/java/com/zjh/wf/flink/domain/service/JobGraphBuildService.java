package com.zjh.wf.flink.domain.service;

import com.zjh.wf.flink.domain.model.FlinkJobParamsInfo;
import org.apache.commons.lang3.BooleanUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.client.program.PackagedProgram;
import org.apache.flink.client.program.PackagedProgramUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.GlobalConfiguration;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.SavepointRestoreSettings;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import java.io.File;
import java.util.Properties;

@Service
public class JobGraphBuildService {

    @Value("${flink.conf-dir}")
    private String flinkConfDir;

    @Value("${jar.path}")
    private String jarPath;

    @Value("${jar.main-class}")
    private String mainClass;

    public static final String SAVE_POINT_PATH_KEY = "savePointPath";
    public static final String ALLOW_NON_RESTORED_STATE_KEY = "allowNonRestoredState";
    public static final String PARALLELISM = "parallelism";

    public JobGraph build(FlinkJobParamsInfo flinkJobParamsInfo) throws Exception {
        String args = flinkJobParamsInfo.getArgs();
        File jarFile = new File(jarPath);
        int parallelism = Integer.parseInt(flinkJobParamsInfo.getConfProperties().getProperty(PARALLELISM, "1"));
        SavepointRestoreSettings savepointRestoreSettings =
                dealSavepointRestoreSettings(flinkJobParamsInfo.getConfProperties());
        PackagedProgram program = PackagedProgram
                                    .newBuilder()
                                    .setArguments(args)
                                    .setJarFile(jarFile)
                                    .setEntryPointClassName(mainClass)
                                    .setSavepointRestoreSettings(savepointRestoreSettings)
                                    .build();
        Configuration flinkConfig = GlobalConfiguration.loadConfiguration(flinkConfDir);
        JobGraph jobGraph =
                PackagedProgramUtils.createJobGraph(program, flinkConfig, parallelism, false);
        return jobGraph;
    }

    private static SavepointRestoreSettings dealSavepointRestoreSettings(
            Properties confProperties) {
        SavepointRestoreSettings savepointRestoreSettings = SavepointRestoreSettings.none();
        String savePointPath = confProperties.getProperty(SAVE_POINT_PATH_KEY);
        if (StringUtils.isNotBlank(savePointPath)) {
            String allowNonRestoredState =
                    confProperties.getOrDefault(ALLOW_NON_RESTORED_STATE_KEY, "false").toString();
            savepointRestoreSettings =
                    SavepointRestoreSettings.forPath(
                            savePointPath, BooleanUtils.toBoolean(allowNonRestoredState));
        }
        return savepointRestoreSettings;
    }
}
