package entry;

import connectors.BinanceConnector;
import connectors.KrakenConnector;
import engine.ArbitrageEngine;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class Main {
    private static final Logger log = LoggerFactory.getLogger(Main.class);

    public static void main(String[] args) {
        Properties config = loadConfig();
        String binanceSymbol = config.getProperty("binance.symbol", "BTCUSDT");
        String krakenSymbol = config.getProperty("kraken.symbol", "BTC/USD");

        ArbitrageEngine engine = new ArbitrageEngine();

        BinanceConnector binance = new BinanceConnector();
        binance.setUpdateHandler((bid, ask) -> engine.updateSnapshot("binance", binanceSymbol, bid, ask));
        binance.connect();
        binance.subscribe(binanceSymbol);

        KrakenConnector kraken = new KrakenConnector();
        kraken.setUpdateHandler((bid, ask) -> engine.updateSnapshot("kraken", krakenSymbol, bid, ask));
        kraken.connect();
        kraken.subscribe(krakenSymbol);

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            log.info("Shutting down...");
            binance.disconnect();
            kraken.disconnect();
            engine.shutdown();
        }));

        log.info("Arbitrage engine started. Monitoring {} on Binance and {} on Kraken.",
                binanceSymbol, krakenSymbol);

        // Keep main thread alive
        try {
            Thread.sleep(Long.MAX_VALUE);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private static Properties loadConfig() {
        Properties props = new Properties();
        try (InputStream input = Main.class.getClassLoader().getResourceAsStream("application.properties")) {
            if (input != null) {
                props.load(input);
            } else {
                log.warn("application.properties not found, using defaults");
            }
        } catch (IOException e) {
            log.error("Failed to load config", e);
        }
        return props;
    }
}