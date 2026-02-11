package listeners;


import engine.ArbitrageEngine;
import java.math.BigDecimal;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class MockExchange {
    private final String name;
    private final ArbitrageEngine engine;
    private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();

    public MockExchange(String name, ArbitrageEngine engine) {
        this.name = name;
        this.engine = engine;
    }

    public void start() {
        scheduler.scheduleAtFixedRate(() -> {
            BigDecimal bid, ask;
            if (name.equalsIgnoreCase("Binance")) {
                // Binance is CHEAP
                bid = new BigDecimal("40000").add(new BigDecimal(Math.random() * 10));
                ask = bid.add(new BigDecimal("1.00"));
                engine.updateBinance(bid, ask);
            } else {
                // Kraken is EXPENSIVE
                bid = new BigDecimal("60000").add(new BigDecimal(Math.random() * 10));
                ask = bid.add(new BigDecimal("1.00"));
                engine.updateKraken(bid, ask);
            }
        }, 0, 1, TimeUnit.MILLISECONDS); // Update every 100ms
    }
}