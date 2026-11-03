/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.test.example.failing;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestartStrategyOptions;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.test.junit5.MiniClusterExtension;
import org.apache.flink.util.CloseableIterator;
import org.apache.flink.util.CollectionUtil;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.apache.flink.util.ExceptionUtils.findThrowableWithMessage;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Parallel stress-test for the AdaptiveScheduler collect-result race condition.
 *
 * <p><b>The race:</b> AdaptiveScheduler was missing a call to {@code executionAttemptFailed()} on
 * operator coordinators when a task fails. {@code
 * CollectSinkOperatorCoordinator.executionAttemptFailed()} sets {@code address = null}. Without
 * that call the stale attempt-0 address remains live. The fetcher may connect during the brief
 * window when attempt 0 is alive, learn {@code version_0}, then when attempt 1 registers with
 * {@code version_1} the fetcher sees a version mismatch and {@code sinkRestarted()} throws {@code
 * RuntimeException("Job restarted")} instead of the real job failure.
 *
 * <p><b>The fix:</b> {@code StateWithExecutionGraph.updateTaskExecutionState()} now calls {@code
 * notifyCoordinatorsAboutTaskFailure()} the moment FAILED state is processed, nulling the address
 * synchronously before attempt 1 can register. The fetcher sees {@code address=null} and the
 * coordinator echoes back whatever version the fetcher sent — so the fetcher's stored version never
 * advances past INIT_VERSION ({@code ""}). When attempt 1 registers the fetcher learns {@code
 * version_1} with no prior version to mismatch against.
 *
 * <p><b>Race-window widening strategy:</b> Each mapper sleeps {@value MAPPER_SLEEP_MS} ms — just
 * enough for the OS to context-switch to the fetcher's background thread and let it start a TCP
 * connection to attempt 0's socket. This is well below the 100 ms poll interval, so the fetcher
 * will not complete a full round-trip and learn {@code version_0} — but it gives the fetcher thread
 * time to be scheduled and begin its connection attempt. Under the parallel variant, 8 concurrent
 * jobs all go through this window simultaneously, multiplying the contention on the scheduler's
 * failure-processing path and making the "address not yet nulled" gap wider per job.
 *
 * <p><b>NOT for commit</b> — local validation only. Run with {@code -Penable-adaptive-scheduler}.
 */
class TaskFailureParallelStressTest {

    private static final String EXCEPTION_STRING = "This is an expected Test Exception";

    /**
     * 5 ms: long enough for the OS to schedule the fetcher thread and start a TCP handshake to
     * attempt 0's socket, but far below the 100 ms poll interval so the fetcher cannot complete a
     * full request/response cycle and learn {@code version_0}.
     */
    private static final long MAPPER_SLEEP_MS = 5;

    /**
     * 16 slots: 8 concurrent jobs × 1 slot each, with headroom for restart slot requests that
     * transiently double slot consumption per job.
     */
    @RegisterExtension
    static final MiniClusterExtension CLUSTER =
            new MiniClusterExtension(
                    new MiniClusterResourceConfiguration.Builder()
                            .setNumberTaskManagers(1)
                            .setNumberSlotsPerTaskManager(16)
                            .build());

    // ── parallel burst ─────────────────────────────────────────────────────────

    /**
     * Submits 8 jobs concurrently (5 rounds = 40 total concurrent job pairs). Under parallel load
     * the scheduler's single-threaded failure path is backlogged with 8 concurrent FAILED
     * transitions at once, widening the gap between "address registered" and "address nulled" for
     * each individual job. Without the fix at least one of the 40 pairs will see "Job restarted".
     */
    @Test
    void testConcurrentFailingJobsAllPropagateCorrectException() throws Exception {
        final int concurrency = 8;
        final int rounds = 5;

        for (int round = 0; round < rounds; round++) {
            final ExecutorService pool = Executors.newFixedThreadPool(concurrency);
            final List<CompletableFuture<Throwable>> futures = new ArrayList<>(concurrency);

            for (int i = 0; i < concurrency; i++) {
                futures.add(CompletableFuture.supplyAsync(this::runAlwaysFailingJob, pool));
            }

            final List<Throwable> results = new ArrayList<>(concurrency);
            for (final CompletableFuture<Throwable> f : futures) {
                results.add(f.get());
            }
            pool.shutdown();

            for (int i = 0; i < concurrency; i++) {
                final Throwable failure = results.get(i);
                assertThat(failure)
                        .as("round %d job %d must surface an exception", round, i)
                        .isNotNull();
                assertThat(findThrowableWithMessage(failure, EXCEPTION_STRING + ":1"))
                        .as(
                                "round %d job %d cause chain must contain '%s:1' but got: %s",
                                round, i, EXCEPTION_STRING, failure.getMessage())
                        .isPresent();
            }
        }
    }

    // ── high-rep sequential ────────────────────────────────────────────────────

    /**
     * Runs 200 sequential iterations of the same job. With a 5 ms mapper sleep each iteration
     * covers attempt-0 lifetime, fetcher connection attempt, FAILED notification, restart, attempt
     * 1 lifetime and final failure. This exercises every slot-lifecycle code path sequentially and
     * accumulates scheduler state across many transitions.
     */
    @Test
    void testHighRepSequentialFailuresPropagateCorrectException() {
        for (int i = 0; i < 200; i++) {
            final Throwable failure = runAlwaysFailingJob();
            assertThat(failure).as("iteration %d must surface an exception", i).isNotNull();
            assertThat(findThrowableWithMessage(failure, EXCEPTION_STRING + ":1"))
                    .as(
                            "iteration %d cause chain must contain '%s:1' but got: %s",
                            i, EXCEPTION_STRING, failure.getMessage())
                    .isPresent();
        }
    }

    // ── helpers ────────────────────────────────────────────────────────────────

    private Throwable runAlwaysFailingJob() {
        try {
            final Configuration configuration = new Configuration();
            configuration.set(RestartStrategyOptions.RESTART_STRATEGY, "fixed-delay");
            configuration.set(RestartStrategyOptions.RESTART_STRATEGY_FIXED_DELAY_ATTEMPTS, 1);
            configuration.set(
                    RestartStrategyOptions.RESTART_STRATEGY_FIXED_DELAY_DELAY,
                    Duration.ofMillis(0));

            final StreamExecutionEnvironment env =
                    StreamExecutionEnvironment.getExecutionEnvironment(configuration);
            env.setParallelism(1);

            final DataStream<Long> stream =
                    env.fromSequence(1, 9).map(new ShortSleepFailingMapper());
            final DataStream.Collector<Long> collector = new DataStream.Collector<>();
            stream.collectAsync(collector);

            final JobClient jobClient = env.executeAsync("parallel-stress-fail");
            try (CloseableIterator<Long> iterator = collector.getOutput()) {
                CollectionUtil.iteratorToList(iterator);
            }
            jobClient.getJobExecutionResult().get();
            return new AssertionError("Job was expected to fail but succeeded");
        } catch (RuntimeException e) {
            return e;
        } catch (Exception e) {
            return new RuntimeException(e);
        }
    }

    // ── mapper ─────────────────────────────────────────────────────────────────

    /**
     * Sleeps {@value MAPPER_SLEEP_MS} ms then throws. The sleep is short enough that the fetcher
     * cannot complete a full poll cycle (100 ms) and learn {@code version_0} — but long enough to
     * give the OS a chance to schedule the fetcher's background thread and let it begin a TCP
     * connection. Under parallel load this window is sufficient for the race to fire without the
     * fix. With the fix, {@code address} is nulled synchronously before attempt 1 registers, so any
     * connection attempt by the fetcher returns an empty-echo response.
     */
    private static class ShortSleepFailingMapper extends RichMapFunction<Long, Long> {
        private static final long serialVersionUID = 1L;

        @Override
        public Long map(Long value) throws Exception {
            Thread.sleep(MAPPER_SLEEP_MS);
            throw new RuntimeException(
                    EXCEPTION_STRING + ":" + getRuntimeContext().getTaskInfo().getAttemptNumber());
        }
    }
}
