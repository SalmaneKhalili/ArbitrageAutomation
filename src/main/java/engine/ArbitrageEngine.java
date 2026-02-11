package engine;

import dto.MarketSnapShot;

import java.math.BigDecimal;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

public class ArbitrageEngine {
    private BigDecimal lastReportedProfit = BigDecimal.ZERO;
    private final Map<Integer, Consumer<BigDecimal>> actions = Map.of(
            -1, profit -> { /* Do nothing: Loss */ },
            0, profit -> { /* Do nothing: Break even */ },
            1, profit -> System.out.println("TRADE EXECUTED! Profit: " + profit)
    );

    private final AtomicReference<MarketSnapShot> binanceData = new AtomicReference<>();
    private final AtomicReference<MarketSnapShot> krakenData = new AtomicReference<>();

    public void updateBinance(BigDecimal bid, BigDecimal ask) {
        binanceData.set(new MarketSnapShot(bid, ask));
        checkForOpportunity();
    }

    public void updateKraken(BigDecimal bid, BigDecimal ask) {
        krakenData.set(new MarketSnapShot(bid, ask));
        checkForOpportunity();
    }

    public void checkForOpportunity() {

        MarketSnapShot b = binanceData.get();
        MarketSnapShot k = krakenData.get();

        if (b == null || k == null) {
            System.out.println("null");
            return;
        }

        // Calculate Scenario A: Buy B, Sell K
        BigDecimal netBuyB = b.ask().multiply(new BigDecimal("1.001"));
        BigDecimal netSellK = k.bid().multiply(new BigDecimal("0.998"));
        BigDecimal profitA = netSellK.subtract(netBuyB);


        // Calculate Scenario B: Buy K, Sell B
        BigDecimal netBuyK = k.ask().multiply(new BigDecimal("1.002"));
        BigDecimal netSellB = b.bid().multiply(new BigDecimal("0.999"));
        BigDecimal profitB = netSellB.subtract(netBuyK);

        // We get the signum (1, 0, or -1) and fetch the corresponding action.
        actions.get(profitA.signum()).accept(profitA);
        actions.get(profitB.signum()).accept(profitB);

    }
}
