package connectors;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import org.java_websocket.client.WebSocketClient;
import org.java_websocket.handshake.ServerHandshake;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.net.URI;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;

public class BinanceConnector implements ExchangeConnector {
    private static final Logger log = LoggerFactory.getLogger(BinanceConnector.class);
    private static final String BINANCE_WS_URL = "wss://stream.binance.com:9443/ws/";
    private static final int SCALE = 8; // satoshis

    private WebSocketClient client;
    private BiConsumer<Long, Long> updateHandler;
    private String symbol;
    private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();

    @Override
    public void connect() {
        // Connection is lazy: will happen when subscribe() is called
    }

    @Override
    public void subscribe(String symbol) {
        this.symbol = symbol.toLowerCase();
        String streamName = this.symbol + "@bookTicker";
        String url = BINANCE_WS_URL + streamName;

        try {
            client = new WebSocketClient(new URI(url)) {
                @Override
                public void onOpen(ServerHandshake handshake) {
                    log.info("Connected to Binance {}", symbol);
                }

                @Override
                public void onMessage(String message) {
                    try {
                        JsonObject json = JsonParser.parseString(message).getAsJsonObject();
                        if (json.has("b") && json.has("a")) {
                            // Binance sends prices as strings
                            String bidStr = json.get("b").getAsString();
                            String askStr = json.get("a").getAsString();

                            long bid = new BigDecimal(bidStr).scaleByPowerOfTen(SCALE).longValue();
                            long ask = new BigDecimal(askStr).scaleByPowerOfTen(SCALE).longValue();

                            if (updateHandler != null) {
                                updateHandler.accept(bid, ask);
                            }
                        }
                    } catch (Exception e) {
                        log.error("Error processing Binance message: {}", message, e);
                    }
                }

                @Override
                public void onClose(int code, String reason, boolean remote) {
                    log.warn("Binance connection closed: {} - {}", code, reason);
                    scheduleReconnect();
                }

                @Override
                public void onError(Exception ex) {
                    log.error("Binance WebSocket error", ex);
                    // The connection will be closed automatically; onClose will trigger reconnect
                }
            };
            client.connect();
        } catch (Exception e) {
            log.error("Failed to connect to Binance", e);
            scheduleReconnect();
        }
    }

    private void scheduleReconnect() {
        scheduler.schedule(() -> {
            log.info("Reconnecting to Binance...");
            subscribe(symbol);
        }, 5, TimeUnit.SECONDS);
    }

    @Override
    public void setUpdateHandler(BiConsumer<Long, Long> handler) {
        this.updateHandler = handler;
    }

    @Override
    public void disconnect() {
        if (client != null) {
            client.close();
        }
        scheduler.shutdown();
    }
}