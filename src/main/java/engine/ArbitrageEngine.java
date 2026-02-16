package engine;

import dto.MarketSnapShot;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.*;

public class ArbitrageEngine {
    private static final Logger log = LoggerFactory.getLogger(ArbitrageEngine.class);

    // Fee rates (as double) – can be loaded from config
    private static final double BINANCE_FEE = 0.001;  // 0.1%
    private static final double KRAKEN_FEE = 0.002;  // 0.2% (example)

    // Minimum profit in satoshis (1 satoshi = 0.00000001 BTC)
    private static final long MIN_PROFIT = 100; // e.g., 100 satoshis = $0.01 (approx)

    // Heartbeat interval in seconds
    private static final long HEARTBEAT_INTERVAL_SEC = 60;

    // Maps symbol -> exchange -> snapshot
    private final Map<String, Map<String, MarketSnapShot>> snapshots = new HashMap<>();

    // Threading: single consumer for all updates
    private final BlockingQueue<Event> queue = new LinkedBlockingQueue<>();
    private final ExecutorService processor = Executors.newSingleThreadExecutor(r -> {
        Thread t = new Thread(r, "ArbitrageEngine-Processor");
        t.setDaemon(true);
        return t;
    });

    // Scheduler for periodic heartbeat events
    private final ScheduledExecutorService heartbeatScheduler = Executors.newSingleThreadScheduledExecutor(r -> {
        Thread t = new Thread(r, "ArbitrageEngine-HeartbeatScheduler");
        t.setDaemon(true);
        return t;
    });

    public ArbitrageEngine() {
        processor.submit(this::processUpdates);
        // Schedule heartbeat events
        heartbeatScheduler.scheduleAtFixedRate(
                () -> queue.offer(new HeartbeatEvent()),
                HEARTBEAT_INTERVAL_SEC,
                HEARTBEAT_INTERVAL_SEC,
                TimeUnit.SECONDS
        );
    }

    /**
     * Called by exchange connectors (from their WebSocket threads).
     * Enqueues the update for sequential processing.
     */
    public void updateSnapshot(String exchange, String symbol, long bid, long ask) {
        queue.offer(new SnapshotEvent(exchange, symbol, bid, ask));
    }

    private void processUpdates() {
        while (true) {
            try {
                Event event = queue.take();
                handleEvent(event);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
        }
    }

    private void handleEvent(Event event) {
        if (event instanceof SnapshotEvent se) {
            handleSnapshot(se);
        } else if (event instanceof HeartbeatEvent) {
            logHeartbeat();
        }
    }

    private void handleSnapshot(SnapshotEvent event) {
        Map<String, MarketSnapShot> perSymbol = snapshots.computeIfAbsent(event.symbol, k -> new HashMap<>());
        perSymbol.put(event.exchange, new MarketSnapShot(event.bid, event.ask));

        // Check arbitrage if we have both snapshots for this symbol
        MarketSnapShot binance = perSymbol.get("binance");
        MarketSnapShot kraken = perSymbol.get("kraken");
        if (binance != null && kraken != null) {
            checkArbitrage(event.symbol, binance, kraken);
        }
    }

    private void checkArbitrage(String symbol, MarketSnapShot binance, MarketSnapShot kraken) {
        // Scenario A: Buy on Binance, sell on Kraken
        long buyBinance = (long)(binance.ask() * (1 + BINANCE_FEE));
        long sellKraken = (long)(kraken.bid() * (1 - KRAKEN_FEE));
        long profitA = sellKraken - buyBinance;

        // Scenario B: Buy on Kraken, sell on Binance
        long buyKraken = (long)(kraken.ask() * (1 + KRAKEN_FEE));
        long sellBinance = (long)(binance.bid() * (1 - BINANCE_FEE));
        long profitB = sellBinance - buyKraken;

        if (profitA >= MIN_PROFIT) {
            log.info("OPPORTUNITY [{}]: Buy Binance @{} -> Sell Kraken @{}, profit={} satoshis (${})",
                    symbol,
                    BigDecimal.valueOf(binance.ask(), 8),
                    BigDecimal.valueOf(kraken.bid(), 8),
                    profitA,
                    BigDecimal.valueOf(profitA, 8).multiply(BigDecimal.valueOf(50000))); // rough USD estimate
        }
        if (profitB >= MIN_PROFIT) {
            log.info("OPPORTUNITY [{}]: Buy Kraken @{} -> Sell Binance @{}, profit={} satoshis",
                    symbol,
                    BigDecimal.valueOf(kraken.ask(), 8),
                    BigDecimal.valueOf(binance.bid(), 8),
                    profitB);
        }
    }

    private void logHeartbeat() {
        if (snapshots.isEmpty()) {
            log.info("Heartbeat: No price data received yet.");
            return;
        }
        // For each symbol, log the latest bid/ask from each exchange (if available)
        for (Map.Entry<String, Map<String, MarketSnapShot>> entry : snapshots.entrySet()) {
            String symbol = entry.getKey();
            Map<String, MarketSnapShot> perExchange = entry.getValue();
            StringBuilder sb = new StringBuilder();
            sb.append("Heartbeat [").append(symbol).append("]: ");
            boolean first = true;
            for (Map.Entry<String, MarketSnapShot> e : perExchange.entrySet()) {
                if (!first) sb.append(", ");
                first = false;
                sb.append(e.getKey()).append(": bid=")
                        .append(BigDecimal.valueOf(e.getValue().bid(), 8))
                        .append(" ask=")
                        .append(BigDecimal.valueOf(e.getValue().ask(), 8));
            }
            log.info(sb.toString());
        }
    }

    // Shutdown hook
    public void shutdown() {
        processor.shutdownNow();
        heartbeatScheduler.shutdownNow();
    }

    // Event types
    private sealed interface Event permits SnapshotEvent, HeartbeatEvent {}
    private record SnapshotEvent(String exchange, String symbol, long bid, long ask) implements Event {}
    private record HeartbeatEvent() implements Event {}
}