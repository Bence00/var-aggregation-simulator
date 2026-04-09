package com.example.risk.ui;

import com.example.risk.generator.GeneratorConfig;
import com.example.risk.generator.distribution.Distribution;
import com.example.risk.model.BenchmarkResult;
import com.example.risk.model.GroupKey;
import com.example.risk.model.LevelResult;
import com.example.risk.model.RiskRecord;
import com.example.risk.service.VaRService;
import javafx.application.Platform;
import javafx.collections.FXCollections;
import javafx.fxml.FXML;
import javafx.scene.control.*;

import java.util.*;
import java.util.stream.Collectors;

/**
 * FXML controller. All long-running operations run on a background thread
 * so the UI stays responsive; results are posted back with Platform.runLater.
 */
public class MainController {

    // ── FXML fields ────────────────────────────────────────────────────
    @FXML private TextField  datasetIdField;
    @FXML private TextField  numRecordsField;
    @FXML private TextField  numAttributesField;
    @FXML private TextField  attrCardinalityField;
    @FXML private TextField  numbersLengthField;
    @FXML private TextField  schemaArraySizeField;
    @FXML private ComboBox<String> distributionCombo;
    @FXML private TextField  seedField;

    @FXML private ComboBox<String> datasetIdCombo;
    @FXML private ComboBox<String> strategyCombo;
    @FXML private ComboBox<String> calculatorCombo;
    @FXML private TextField  interestingField;
    @FXML private TextField  percentilesField;

    @FXML private TextArea   resultsArea;
    @FXML private Label      statusLabel;

    @FXML private Button     generateBtn;
    @FXML private Button     loadBtn;
    @FXML private Button     runVarBtn;
    @FXML private Button     benchmarkBtn;
    @FXML private Button     queryBenchmarkBtn;
    @FXML private Button     applySchemaBtn;

    private VaRService service;

    // ── Initialisation ─────────────────────────────────────────────────

    @FXML
    public void initialize() {
        service = RiskApp.getService();

        distributionCombo.setItems(FXCollections.observableList(service.getDistributionNames()));
        distributionCombo.getSelectionModel().selectFirst();

        strategyCombo.setItems(FXCollections.observableList(new ArrayList<>(service.getStrategyNames())));
        strategyCombo.getSelectionModel().selectFirst();

        calculatorCombo.setItems(FXCollections.observableList(new ArrayList<>(service.getCalculatorNames())));
        calculatorCombo.getSelectionModel().selectFirst();

        if (schemaArraySizeField != null) {
            schemaArraySizeField.setText(String.valueOf(service.getSchemaMaxAttributes()));
        }

        onRefreshDatasets();
        append("System ready. Configure parameters on the left and click a button to start.");
    }

    // ── Button handlers ────────────────────────────────────────────────

    @FXML
    public void onGenerate() {
        GeneratorConfig config;
        try {
            config = buildGeneratorConfig();
        } catch (NumberFormatException e) {
            setStatus("Invalid input: " + e.getMessage());
            return;
        }
        setAllButtons(false);
        setStatus("Generating " + config.getNumRecords() + " records...");
        runAsync(() -> {
            BenchmarkResult r = service.generateAndSave(config);
            String msg = String.format(
                "✓ Generated %d records  [gen=%dms  insert=%dms  total=%dms]",
                r.getRecordCount(), r.getGenerationMs(), r.getDbInsertMs(), r.getTotalMs());
            Platform.runLater(() -> {
                append(msg);
                setStatus("Done");
                onRefreshDatasets();
                setAllButtons(true);
            });
        });
    }

    @FXML
    public void onLoad() {
        String datasetId = datasetIdCombo.getValue();
        if (datasetId == null || datasetId.isBlank()) { setStatus("Select a dataset first"); return; }
        setAllButtons(false);
        setStatus("Loading " + datasetId + " from DB...");
        runAsync(() -> {
            long ms = service.loadFromDb(datasetId);
            int  n  = service.getLoadedRecords().size();
            Platform.runLater(() -> {
                append(String.format("✓ Loaded %d records for '%s' in %dms", n, datasetId, ms));
                setStatus("Loaded " + n + " records");
                setAllButtons(true);
            });
        });
    }

    @FXML
    public void onApplySchemaArraySize() {
        int newSize;
        try {
            newSize = Integer.parseInt(schemaArraySizeField.getText().trim());
        } catch (NumberFormatException e) {
            setStatus("Invalid array size");
            return;
        }
        if (newSize <= 0) {
            setStatus("Array size must be positive");
            return;
        }

        setAllButtons(false);
        setStatus("Applying attribute array size: " + newSize);
        runAsync(() -> {
            service.updateSchemaMaxAttributes(newSize);
            Platform.runLater(() -> {
                append("Schema attribute array size set to " + newSize);
                setStatus("Schema updated");
                setAllButtons(true);
            });
        });
    }

    @FXML
    public void onRunVaR() {
        if (service.getLoadedRecords().isEmpty()) {
            setStatus("No records loaded. Generate or Load first.");
            return;
        }
        List<Integer> dims;
        List<Double>  percs;
        try {
            dims  = parseDims(interestingField.getText());
            percs = parsePercentiles(percentilesField.getText());
        } catch (Exception e) {
            setStatus("Parse error: " + e.getMessage());
            return;
        }
        String strategy   = strategyCombo.getValue();
        String calculator = calculatorCombo.getValue();
        setAllButtons(false);
        setStatus("Running VaR (" + strategy + " / " + calculator + ")...");

        runAsync(() -> {
            long t0     = System.nanoTime();
            List<LevelResult> levels = service.runVaR(strategy, calculator, dims, percs, this::setStatusFromWorker);
            long elapsed = (System.nanoTime() - t0) / 1_000_000L;

            String output = formatVaRResults(levels, percs, elapsed);
            Platform.runLater(() -> {
                append(output);
                setStatus(String.format("VaR done in %dms (%d levels)", elapsed, levels.size()));
                setAllButtons(true);
            });
        });
    }

    @FXML
    public void onBenchmark() {
        GeneratorConfig config;
        List<Integer>   dims;
        List<Double>    percs;
        try {
            config = buildGeneratorConfig();
            dims   = parseDims(interestingField.getText());
            percs  = parsePercentiles(percentilesField.getText());
        } catch (Exception e) {
            setStatus("Parse error: " + e.getMessage());
            return;
        }
        String strategy   = strategyCombo.getValue();
        String calculator = calculatorCombo.getValue();
        setAllButtons(false);
        setStatus("Running full benchmark...");

        runAsync(() -> {
            BenchmarkResult r = service.runFullBenchmark(config, strategy, calculator, dims, percs, this::setStatusFromWorker);
            String msg = String.format(
                """
                ═══════ BENCHMARK RESULT ═══════
                Records     : %d
                Vector len  : %d
                Aggregation : %s
                Selection   : %s
                ────────────────────────────────
                Generation  : %5d ms
                DB insert   : %5d ms
                DB load     : %5d ms
                Ordering    : %5d ms
                Aggregation : %5d ms
                Percentile  : %5d ms
                ────────────────────────────────
                TOTAL       : %5d ms
                ════════════════════════════════
                """,
                r.getRecordCount(), r.getNumbersLength(), strategy, calculator,
                r.getGenerationMs(), r.getDbInsertMs(), r.getDbLoadMs(),
                r.getOrderingMs(), r.getAggregationMs(), r.getPercentileMs(), r.getTotalMs());

            Platform.runLater(() -> {
                append(msg);
                setStatus("Benchmark done. Total: " + r.getTotalMs() + "ms");
                onRefreshDatasets();
                setAllButtons(true);
            });
        });
    }

    @FXML
    public void onQueryBenchmark() {
        String datasetId = datasetIdCombo.getValue();
        if (datasetId == null || datasetId.isBlank()) {
            setStatus("Select a dataset first");
            return;
        }

        List<Integer> dims;
        List<Double> percs;
        try {
            dims = parseDims(interestingField.getText());
            percs = parsePercentiles(percentilesField.getText());
        } catch (Exception e) {
            setStatus("Parse error: " + e.getMessage());
            return;
        }

        String strategy = strategyCombo.getValue();
        String calculator = calculatorCombo.getValue();
        setAllButtons(false);
        setStatus("Running query benchmark...");

        runAsync(() -> {
            BenchmarkResult r = service.runQueryBenchmark(datasetId, strategy, calculator, dims, percs, this::setStatusFromWorker);
            String msg = String.format(
                    """
                    ═══════ QUERY BENCHMARK RESULT ═══════
                    Dataset     : %s
                    Records     : %d
                    Vector len  : %d
                    Aggregation : %s
                    Selection   : %s
                    ────────────────────────────────
                    DB read     : %5d ms
                    Ordering    : %5d ms
                    Aggregation : %5d ms
                    Percentile  : %5d ms
                    ────────────────────────────────
                    TOTAL       : %5d ms
                    ════════════════════════════════
                    """,
                    datasetId, r.getRecordCount(), r.getNumbersLength(), strategy, calculator,
                    r.getDbLoadMs(), r.getOrderingMs(), r.getAggregationMs(), r.getPercentileMs(), r.getTotalMs());

            Platform.runLater(() -> {
                append(msg);
                setStatus("Query benchmark done. Total: " + r.getTotalMs() + "ms");
                setAllButtons(true);
            });
        });
    }

    @FXML
    public void onRefreshDatasets() {
        runAsync(() -> {
            try {
                List<String> ids = service.listDatasetIds();
                Platform.runLater(() ->
                    datasetIdCombo.setItems(FXCollections.observableList(ids)));
            } catch (Exception e) {
                // DB not available — silently ignore
            }
        });
    }

    @FXML
    public void onClear() {
        resultsArea.clear();
    }

    // ── Formatting helpers ─────────────────────────────────────────────

    private String formatVaRResults(List<LevelResult> levels, List<Double> percs, long ms) {
        StringBuilder sb = new StringBuilder();
        sb.append(String.format("═══ VaR Results — %d levels — %dms ═══%n", levels.size(), ms));

        for (int lvl = 0; lvl < levels.size(); lvl++) {
            LevelResult level = levels.get(lvl);
            sb.append(String.format("%n  Level %d: %s  (%d groups)%n",
                    lvl, level.dimensionsLabel(), level.getGroupCount()));

            int shown = 0;
            for (Map.Entry<GroupKey, Map<Double, Double>> entry : level.getVarValues().entrySet()) {
                if (shown++ >= 5) { sb.append("    ... (truncated)%n".formatted()); break; }
                sb.append("    ").append(entry.getKey()).append("  →");
                for (double p : percs) {
                    double val = entry.getValue().getOrDefault(p, Double.NaN);
                    sb.append(String.format("  p%.0f%%=%.4f", p, val));
                }
                sb.append("\n");
            }
        }
        sb.append("═".repeat(50)).append("\n");
        return sb.toString();
    }

    // ── Parsing ────────────────────────────────────────────────────────

    private List<Integer> parseDims(String s) {
        return Arrays.stream(s.split(","))
                .map(String::trim)
                .filter(t -> !t.isEmpty())
                .map(Integer::parseInt)
                .collect(Collectors.toList());
    }

    private List<Double> parsePercentiles(String s) {
        return Arrays.stream(s.split(","))
                .map(String::trim)
                .filter(t -> !t.isEmpty())
                .map(Double::parseDouble)
                .collect(Collectors.toList());
    }

    private GeneratorConfig buildGeneratorConfig() {
        String      distName = distributionCombo.getValue();
        long        seed     = Long.parseLong(seedField.getText().trim());
        Distribution dist    = VaRService.createDistribution(distName, seed);

        return GeneratorConfig.builder()
                .datasetId(datasetIdField.getText().trim())
                .numRecords(Integer.parseInt(numRecordsField.getText().trim()))
                .numAttributes(Integer.parseInt(numAttributesField.getText().trim()))
                .attributeCardinality(Integer.parseInt(attrCardinalityField.getText().trim()))
                .numbersLength(Integer.parseInt(numbersLengthField.getText().trim()))
                .seed(seed)
                .distributions(List.of(dist))
                .build();
    }

    // ── UI utilities ───────────────────────────────────────────────────

    private void append(String text) {
        resultsArea.appendText(text + "\n");
    }

    private void setStatus(String msg) {
        statusLabel.setText(msg);
    }

    private void setStatusFromWorker(String msg) {
        Platform.runLater(() -> setStatus(msg));
    }

    private void setAllButtons(boolean enabled) {
        generateBtn.setDisable(!enabled);
        loadBtn.setDisable(!enabled);
        runVarBtn.setDisable(!enabled);
        benchmarkBtn.setDisable(!enabled);
        if (queryBenchmarkBtn != null) queryBenchmarkBtn.setDisable(!enabled);
        if (applySchemaBtn != null) applySchemaBtn.setDisable(!enabled);
    }

    private void runAsync(ThrowingRunnable task) {
        Thread t = new Thread(() -> {
            try {
                task.run();
            } catch (Exception e) {
                Platform.runLater(() -> {
                    append("ERROR: " + e.getMessage());
                    setStatus("Error — see output");
                    setAllButtons(true);
                });
            }
        });
        t.setDaemon(true);
        t.start();
    }

    @FunctionalInterface
    interface ThrowingRunnable { void run() throws Exception; }
}
