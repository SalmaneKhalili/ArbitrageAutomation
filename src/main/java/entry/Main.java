package entry;

import engine.ArbitrageEngine;
import listeners.MockExchange;

public class Main {
    public static void main(String[] args){
        ArbitrageEngine engine = new ArbitrageEngine();
        MockExchange binance = new MockExchange("Binance", engine);
        MockExchange kraken = new MockExchange("Kraken", engine);
        binance.start();
        kraken.start();
    }
}
