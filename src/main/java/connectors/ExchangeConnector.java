package connectors;

import java.util.function.BiConsumer;

public interface ExchangeConnector {
    void connect();
    void subscribe(String symbol);
    void setUpdateHandler(BiConsumer<Long, Long> handler); // bid, ask in satoshis
    void disconnect();
}