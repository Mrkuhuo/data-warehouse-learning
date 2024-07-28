package org.bigdatatechcir.learn.flink.local.util;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.java.utils.MultipleParameterTool;
import org.apache.flink.configuration.ExecutionOptions;
import org.apache.flink.core.fs.Path;
import org.apache.flink.util.TimeUtils;

import java.time.Duration;
import java.util.Arrays;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalInt;

public class CLI extends ExecutionConfig.GlobalJobParameters {

    public static final String INPUT_KEY = "input";
    public static final String OUTPUT_KEY = "output";
    public static final String DISCOVERY_INTERVAL = "discovery-interval";
    public static final String EXECUTION_MODE = "execution-mode";

    public static CLI fromArgs(String[] args) throws Exception {
        MultipleParameterTool params = MultipleParameterTool.fromArgs(args);
        Path[] inputs = null;
        if (params.has(INPUT_KEY)) {
            inputs =
                    params.getMultiParameterRequired(INPUT_KEY).stream()
                            .map(Path::new)
                            .toArray(Path[]::new);
        } else {
            System.out.println("Executing example with default input data.");
            System.out.println("Use --input to specify file input.");
        }

        Path output = null;
        if (params.has(OUTPUT_KEY)) {
            output = new Path(params.get(OUTPUT_KEY));
        } else {
            System.out.println("Printing result to stdout. Use --output to specify output path.");
        }

        Duration watchInterval = null;
        if (params.has(DISCOVERY_INTERVAL)) {
            watchInterval = TimeUtils.parseDuration(params.get(DISCOVERY_INTERVAL));
        }

        RuntimeExecutionMode executionMode = ExecutionOptions.RUNTIME_MODE.defaultValue();
        if (params.has(EXECUTION_MODE)) {
            executionMode = RuntimeExecutionMode.valueOf(params.get(EXECUTION_MODE).toUpperCase());
        }

        return new CLI(inputs, output, watchInterval, executionMode, params);
    }

    private final Path[] inputs;
    private final Path output;
    private final Duration discoveryInterval;
    private final RuntimeExecutionMode executionMode;
    private final MultipleParameterTool params;

    private CLI(
            Path[] inputs,
            Path output,
            Duration discoveryInterval,
            RuntimeExecutionMode executionMode,
            MultipleParameterTool params) {
        this.inputs = inputs;
        this.output = output;
        this.discoveryInterval = discoveryInterval;
        this.executionMode = executionMode;
        this.params = params;
    }

    public Optional<Path[]> getInputs() {
        return Optional.ofNullable(inputs);
    }

    public Optional<Duration> getDiscoveryInterval() {
        return Optional.ofNullable(discoveryInterval);
    }

    public Optional<Path> getOutput() {
        return Optional.ofNullable(output);
    }

    public RuntimeExecutionMode getExecutionMode() {
        return executionMode;
    }

    public OptionalInt getInt(String key) {
        if (params.has(key)) {
            return OptionalInt.of(params.getInt(key));
        }

        return OptionalInt.empty();
    }

    @Override
    public Map<String, String> toMap() {
        return params.toMap();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        if (!super.equals(o)) {
            return false;
        }
        CLI cli = (CLI) o;
        return Arrays.equals(inputs, cli.inputs)
                && Objects.equals(output, cli.output)
                && Objects.equals(discoveryInterval, cli.discoveryInterval);
    }

    @Override
    public int hashCode() {
        int result = Objects.hash(output, discoveryInterval);
        result = 31 * result + Arrays.hashCode(inputs);
        return result;
    }
}