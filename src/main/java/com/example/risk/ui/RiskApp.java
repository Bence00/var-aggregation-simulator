package com.example.risk.ui;

import com.example.risk.config.AppConfig;
import com.example.risk.service.VaRService;
import javafx.application.Application;
import javafx.fxml.FXMLLoader;
import javafx.scene.Scene;
import javafx.stage.Stage;

/**
 * JavaFX entry point. Wires the VaRService and hands it to the controller.
 */
public class RiskApp extends Application {

    /** Shared service instance passed to the FXML controller via a static accessor. */
    private static VaRService service;

    public static VaRService getService() { return service; }

    @Override
    public void start(Stage stage) throws Exception {
        long maxHeap = Runtime.getRuntime().maxMemory() / (1024 * 1024);
        System.out.println("Max heap: " + maxHeap + " MB");

        // Bootstrap service before UI starts
        AppConfig config = new AppConfig();
        service = new VaRService(config);
        tryInitDb(service);

        FXMLLoader loader = new FXMLLoader(
                RiskApp.class.getResource("/com/example/risk/ui/main-view.fxml"));
        Scene scene = new Scene(loader.load());
        stage.setTitle("Historical VaR Computation System");
        stage.setScene(scene);
        stage.setMinWidth(900);
        stage.setMinHeight(600);
        stage.show();
    }

    /** Attempt DB schema init; warn on failure so the app still starts without Postgres. */
    private static void tryInitDb(VaRService svc) {
        try {
            svc.initDatabase();
        } catch (Exception e) {
            System.err.println("[WARN] DB init failed (Postgres not running?): " + e.getMessage());
            System.err.println("[WARN] DB features will be disabled. Generation and VaR will still work in-memory.");
        }
    }
}
