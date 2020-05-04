/*
 * Copyright © 2018-2020 VMware, Inc. All Rights Reserved.
 *
 * SPDX-License-Identifier: BSD-2
 */

package org.dcm;

import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListenableScheduledFuture;
import com.google.common.util.concurrent.ListeningScheduledExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.SettableFuture;
import io.fabric8.kubernetes.api.model.Container;
import io.fabric8.kubernetes.api.model.Node;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.Quantity;
import io.fabric8.kubernetes.api.model.ResourceRequirements;
import io.fabric8.kubernetes.client.DefaultKubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClientException;
import io.fabric8.kubernetes.client.Watcher;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * To run these specific tests, pass a `schedulerName` property to maven, for example:
 *
 *  mvn integrate-test -DargLine="-Dk8sUrl=<hostname>:<port> -DschedulerName=dcm-scheduler"
 */
class WorkloadGeneratorIT extends ITBase {
    private static final Logger LOG = LoggerFactory.getLogger(WorkloadGeneratorIT.class);
    private static final String SCHEDULER_NAME_PROPERTY = "schedulerName";
    private static final String SCHEDULER_NAME_DEFAULT = "default-scheduler";
    private static final String CPU_SCALE_DOWN_PROPERTY = "cpuScaleDown";
    private static final int CPU_SCALE_DOWN_DEFAULT = 40;
    private static final String MEM_SCALE_DOWN_PROPERTY = "memScaleDown";
    private static final int MEM_SCALE_DOWN_DEFAULT = 50;
    private static final String TIME_SCALE_DOWN_PROPERTY = "timeScaleDown";
    private static final int TIME_SCALE_DOWN_DEFAULT = 1000;
    private static final String START_TIME_CUTOFF = "startTimeCutOff";
    private static final int START_TIME_CUTOFF_DEFAULT = 1000;

    private final List<ListenableFuture<?>> deletions = new ArrayList<>();
    private final ListeningScheduledExecutorService scheduledExecutorService =
            MoreExecutors.listeningDecorator(Executors.newScheduledThreadPool(100));

    @BeforeEach
    public void logBuildInfo() {
        final InputStream resourceAsStream = Scheduler.class.getResourceAsStream("/git.properties");
        try (final BufferedReader gitPropertiesFile = new BufferedReader(new InputStreamReader(resourceAsStream,
                Charset.forName("UTF8")))) {
            final String gitProperties = gitPropertiesFile.lines().collect(Collectors.joining(" "));
            LOG.info("Running integration test for the following build: {}", gitProperties);
        } catch (final IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    public void testSmallTrace() throws Exception {
        if (getClass().getClassLoader().getResource("test-data.txt") != null) {
            System.out.println("Running small trace");
            final long traceId = System.currentTimeMillis();
            fabricClient.pods().inAnyNamespace().watch(new LoggingPodWatcher(traceId));
            fabricClient.nodes().watch(new LoggingNodeWatcher(traceId));

            final IPodDeployer deployer = new KubernetesPodDeployer(fabricClient, TEST_NAMESPACE);
            runTrace("test-data.txt", deployer);
        } else {
            System.out.println("test file not found");
        }
    }

    @Test
    public void testAzureV1Complete() throws Exception {
        if (getClass().getClassLoader().getResource("v1-data.txt") != null) {
            System.out.println("Running Azure v1 complete trace");
            final long traceId = System.currentTimeMillis();
            fabricClient.pods().inAnyNamespace().watch(new LoggingPodWatcher(traceId));
            fabricClient.nodes().watch(new LoggingNodeWatcher(traceId));

            final IPodDeployer deployer = new KubernetesPodDeployer(fabricClient, TEST_NAMESPACE);
            runTrace("v1-data.txt", deployer);
        } else {
            System.out.println("Azure v1 trace not found. Please run \"bash getAzureTraces.sh\" in the folder" +
                    " workload-generator to generate the required traces.");
        }
    }

    @Test
    public void testAzureV1Cropped() throws Exception {
        if (getClass().getClassLoader().getResource("v1-cropped.txt") != null) {
            System.out.println("Running Azure v1 cropped trace");
            final long traceId = System.currentTimeMillis();
            fabricClient.pods().inAnyNamespace().watch(new LoggingPodWatcher(traceId));
            fabricClient.nodes().watch(new LoggingNodeWatcher(traceId));

            final IPodDeployer deployer = new KubernetesPodDeployer(fabricClient, TEST_NAMESPACE);
            runTrace("v1-cropped.txt", deployer);
        } else {
            System.out.println("Azure v1 trace not found. Please run \"bash getAzureTraces.sh\" in the folder" +
                    " workload-generator to generate the required traces.");
        }
    }

    @Test
    public void testAzureV2Complete() throws Exception {
        if (getClass().getClassLoader().getResource("v2-data.txt") != null) {
            System.out.println("Running Azure v2 complete trace");
            final long traceId = System.currentTimeMillis();
            fabricClient.pods().inAnyNamespace().watch(new LoggingPodWatcher(traceId));
            fabricClient.nodes().watch(new LoggingNodeWatcher(traceId));

            final IPodDeployer deployer = new KubernetesPodDeployer(fabricClient, TEST_NAMESPACE);
            runTrace("v2-data.txt", deployer);
        } else {
            System.out.println("Azure v2 trace not found. Please run \"bash getAzureTraces.sh\" in the folder" +
                    " workload-generator to generate the required traces.");
        }
    }

    @Test
    public void testAzureV2Cropped() throws Exception {
        if (getClass().getClassLoader().getResource("v2-cropped.txt") != null) {
            System.out.println("Running Azure v2 cropped trace");
            // Trace pod and node arrivals/departure
            final long traceId = System.currentTimeMillis();
            fabricClient.pods().inAnyNamespace().watch(new LoggingPodWatcher(traceId));
            fabricClient.nodes().watch(new LoggingNodeWatcher(traceId));

            final IPodDeployer deployer = new KubernetesPodDeployer(fabricClient, TEST_NAMESPACE);
            runTrace("v2-cropped.txt", deployer);
        } else {
            System.out.println("Azure v2 trace not found. Please run \"bash getAzureTraces.sh\" in the folder" +
                    " workload-generator to generate the required traces.");
        }
    }

    private void runTrace(final String fileName, final IPodDeployer deployer) throws Exception {
        final String schedulerNameProperty = System.getProperty(SCHEDULER_NAME_PROPERTY);
        final String schedulerName = schedulerNameProperty == null ? SCHEDULER_NAME_DEFAULT : schedulerNameProperty;

        final String cpuScaleProperty = System.getProperty(CPU_SCALE_DOWN_PROPERTY);
        final int cpuScaleDown = cpuScaleProperty == null ? CPU_SCALE_DOWN_DEFAULT : Integer.parseInt(cpuScaleProperty);

        final String memScaleProperty = System.getProperty(MEM_SCALE_DOWN_PROPERTY);
        final int memScaleDown = memScaleProperty == null ? MEM_SCALE_DOWN_DEFAULT : Integer.parseInt(memScaleProperty);

        final String timeScaleProperty = System.getProperty(TIME_SCALE_DOWN_PROPERTY);
        final int timeScaleDown = timeScaleProperty == null ? TIME_SCALE_DOWN_DEFAULT :
                                        Integer.parseInt(timeScaleProperty);

        final String startTimeCutOffProperty = System.getProperty(START_TIME_CUTOFF);
        final int startTimeCutOff = startTimeCutOffProperty == null ? START_TIME_CUTOFF_DEFAULT :
                Integer.parseInt(startTimeCutOffProperty);
        runTrace(fabricClient, fileName, deployer, schedulerName, cpuScaleDown, memScaleDown, timeScaleDown,
                 startTimeCutOff);
    }

    void runTrace(final DefaultKubernetesClient client, final String fileName, final IPodDeployer deployer,
                  final String schedulerName, final int cpuScaleDown, final int memScaleDown, final int timeScaleDown,
                  final int startTimeCutOff)
            throws Exception {
        LOG.info("Running trace with parameters: SchedulerName:{} CpuScaleDown:{}" +
                 " MemScaleDown:{} TimeScaleDown:{} StartTimeCutOff:{}", schedulerName, cpuScaleDown, memScaleDown,
                                                                      timeScaleDown, startTimeCutOff);

        // Load data from file
        final ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        final InputStream inStream = classLoader.getResourceAsStream(fileName);
        Preconditions.checkNotNull(inStream);

        long maxStart = 0;
        long maxEnd = 0;
        int totalPods = 0;

        try (final BufferedReader reader = new BufferedReader(new InputStreamReader(inStream,
                StandardCharsets.UTF_8))) {
            String line;
            int taskCount = 0;
            final long startTime = System.currentTimeMillis();
            System.out.println("Starting at " + startTime);
            while ((line = reader.readLine()) != null) {
                final String[] parts = line.split(" ", 7);
                final int start = Integer.parseInt(parts[2]) / timeScaleDown;
                // final int end = Integer.parseInt(parts[3]) / timeScaleDown;
                final float cpu = Float.parseFloat(parts[4].replace(">", "")) / cpuScaleDown;
                final float mem = Float.parseFloat(parts[5].replace(">", "")) / memScaleDown;
                final int vmCount = Integer.parseInt(parts[6].replace(">", ""));

                // generate a deployment's details based on cpu, mem requirements
                final List<Pod> deployment = getDeployment(client, schedulerName, cpu, mem, vmCount, taskCount);
                totalPods += deployment.size();

                if (Integer.parseInt(parts[2]) > startTimeCutOff) { // window in seconds
                    break;
                }
                // get task time info
                final long taskStartTime = (long) start * 1000; // converting to millisec
                final long currentTime = System.currentTimeMillis();
                final long timeDiff = currentTime - startTime;
                final long waitTime = taskStartTime - timeDiff;

                // create deployment in the k8s cluster at the correct start time
                final ListenableFuture<?> scheduledStart = scheduledExecutorService.schedule(
                        deployer.startDeployment(deployment), waitTime, TimeUnit.MILLISECONDS);

                // get duration based on start and end times
                // final int duration = getDuration(start, end);

                final long computedEndTime = (waitTime / 1000) + 60;

                // Schedule deletion of this deployment based on duration + time until start of the dep
                final SettableFuture<Boolean> onComplete = SettableFuture.create();
                scheduledStart.addListener(() -> {
                    final ListenableScheduledFuture<?> deletion =
                            scheduledExecutorService.schedule(deployer.endDeployment(deployment),
                            60, TimeUnit.SECONDS);
                    deletion.addListener(() -> onComplete.set(true), scheduledExecutorService);
                }, scheduledExecutorService);
                deletions.add(onComplete);

                maxStart = Math.max(maxStart, waitTime / 1000);
                maxEnd = Math.max(maxEnd, computedEndTime);

                // Add to a list to enable keeping the test active until deletion
                taskCount++;
            }
        } catch (final IOException e) {
            throw new RuntimeException(e);
        }

        LOG.info("All tasks launched ({} pods total). The latest application will start at {}s, and the last deletion" +
                 " will happen at {}s. Sleeping for {}s before teardown.", totalPods, maxStart,
                maxEnd, maxEnd);

        final List<Object> objects = Futures.successfulAsList(deletions).get(maxEnd + 100, TimeUnit.SECONDS);
        assert objects.size() != 0;
    }

    private List<Pod> getDeployment(final DefaultKubernetesClient client, final String schedulerName, final float cpu,
                                     final float mem, final int count, final int taskCount) {
        // Load the template file and update its contents to generate a new deployment template
        final List<Pod> podsToCreate = IntStream.range(0, count)
                .mapToObj(podCount -> {
                    try (final InputStream fileStream =
                                 getClass().getClassLoader().getResourceAsStream("pod-only.yml")) {
                        final Pod pod = client.pods().load(fileStream).get();
                        pod.getSpec().setSchedulerName(schedulerName);
                        final String appName = "app-" + taskCount;
                        pod.getMetadata().setName(appName + "-" + podCount);

                        final List<Container> containerList = pod.getSpec().getContainers();
                        for (ListIterator<Container> iter = containerList.listIterator(); iter.hasNext(); ) {
                            final Container container = iter.next();
                            final ResourceRequirements resReq = new ResourceRequirements();
                            final Map<String, Quantity> reqs = new HashMap<>();
                            reqs.put("cpu", new Quantity(cpu * 1000 + "m"));
                            reqs.put("memory", new Quantity(Float.toString(mem)));
                            resReq.setRequests(reqs);
                            container.setResources(resReq);
                            iter.set(container);
                        }
                        return pod;
                    } catch (final IOException e) {
                        throw new RuntimeException(e);
                    }
                })
                .collect(Collectors.toList());
        return podsToCreate;
    }

    /*
    private int getDuration(final int startTime, int endTime) {
        if (endTime <= startTime) {
            endTime = startTime + 5;
        }
        return (endTime - startTime);
    }*/

    private static final class LoggingPodWatcher implements Watcher<Pod> {
        private final long traceId;

        LoggingPodWatcher(final long traceId) {
            this.traceId = traceId;
        }

        @Override
        public void eventReceived(final Action action, final Pod pod) {
            LOG.info("Timestamp: {}, Trace: {}, PodName: {}, NodeName: {}, Status: {}, Action: {}",
                    System.currentTimeMillis(), traceId, pod.getMetadata().getName(), pod.getSpec().getNodeName(),
                    pod.getStatus().getPhase(), action);
        }

        @Override
        public void onClose(final KubernetesClientException cause) {
            LOG.info("Timestamp: {}, Trace: {}, PodWatcher closed", System.currentTimeMillis(), traceId);
        }
    }


    private static final class LoggingNodeWatcher implements Watcher<Node> {
        private final long traceId;

        LoggingNodeWatcher(final long traceId) {
            this.traceId = traceId;
        }

        @Override
        public void eventReceived(final Action action, final Node node) {
            LOG.info("Timestamp: {}, Trace: {}, NodeName: {}, Action: {}", System.currentTimeMillis(), traceId,
                    node.getMetadata().getName(), action);
        }

        @Override
        public void onClose(final KubernetesClientException cause) {
            LOG.info("Timestamp: {}, Trace: {}, NodeWatcher closed", System.currentTimeMillis(), traceId);
        }
    }
}