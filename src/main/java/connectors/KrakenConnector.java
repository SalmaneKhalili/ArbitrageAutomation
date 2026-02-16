package connectors;

import com.google.gson.Gson;
import com.google.gson.JsonArray;
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

public class KrakenConnector implements ExchangeConnector {
    private static final Logger log = LoggerFactory.getLogger(KrakenConnector.class);
    private static final String KRAKEN_WS_URL = "wss://ws.kraken.com/v2";
    private static final int SCALE = 8;

    private WebSocketClient client;
    private BiConsumer<Long, Long> updateHandler;
    private String symbol;
    private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();

    @Override
    public void connect() {
        try {
            client = new WebSocketClient(new URI(KRAKEN_WS_URL)) {
                @Override
                public void onOpen(ServerHandshake handshake) {
                    log.info("Connected to Kraken");
                    sendSubscription();
                }

                @Override
                public void onMessage(String message) {
                    try {
                        JsonObject json = JsonParser.parseString(message).getAsJsonObject();
                        if (json.has("channel") && json.get("channel").getAsString().equals("ticker") &&
                                json.has("data")) {
                            JsonArray data = json.getAsJsonArray("data");
                            for (var elem : data) {
                                JsonObject tick = elem.getAsJsonObject();
                                if (tick.has("bid") && tick.has("ask")) {
                                    String bidStr = tick.get("bid").getAsString();
                                    String askStr = tick.get("ask").getAsString();

                                    long bid = new BigDecimal(bidStr).scaleByPowerOfTen(SCALE).longValue();
                                    long ask = new BigDecimal(askStr).scaleByPowerOfTen(SCALE).longValue();

                                    if (updateHandler != null) {
                                        updateHandler.accept(bid, ask);
                                    }
                                }
                            }
                        } else if (json.has("event") && json.get("event").getAsString().equals("subscriptionStatus")) {
                            log.debug("Kraken subscription status: {}", message);
                        }
                    } catch (Exception e) {
                        log.error("Error processing Kraken message: {}", message, e);
                    }
                }

                @Override
                public void onClose(int code, String reason, boolean remote) {
                    log.warn("Kraken connection closed: {} - {}", code, reason);
                    scheduleReconnect();
                }

                @Override
                public void onError(Exception ex) {
                    log.error("Kraken WebSocket error", ex);
                }
            };
            client.connect();
        } catch (Exception e) {
            log.error("Failed to connect to Kraken", e);
            scheduleReconnect();
        }
    }

    private void sendSubscription() {
        if (symbol == null || client == null || !client.isOpen()) return;
        JsonObject subMsg = new JsonObject();
        subMsg.addProperty("method", "subscribe");
        JsonObject params = new JsonObject();
        params.addProperty("channel", "ticker");
        JsonArray symbolList = new JsonArray();
        symbolList.add(symbol);
        params.add("symbol", symbolList);
        subMsg.add("params", params);
        client.send(subMsg.toString());
        log.info("Subscribed to Kraken ticker for {}", symbol);
    }

    private void scheduleReconnect() {
        scheduler.schedule(() -> {
            log.info("Reconnecting to Kraken...");
            connect();
        }, 5, TimeUnit.SECONDS);
    }

    @Override
    public void subscribe(String symbol) {
        this.symbol = symbol; // e.g., "BTC/USD"
        if (client != null && client.isOpen()) {
            sendSubscription();
        }
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