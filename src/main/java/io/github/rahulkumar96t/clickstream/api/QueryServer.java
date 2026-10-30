package io.github.rahulkumar96t.clickstream.api;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import io.github.rahulkumar96t.clickstream.serde.Json;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.format.DateTimeParseException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Supplier;
import org.apache.kafka.streams.errors.InvalidStateStoreException;

/**
 * Small JSON REST API over the local state stores (interactive queries), on the JDK {@link
 * HttpServer}.
 *
 * <pre>
 * GET /api/page-counts?page=/home&amp;from=2026-01-01T10:00:00Z&amp;to=...   counts per window
 * GET /api/page-counts?from=...&amp;to=...                                  all pages
 * GET /api/top-pages?window=2026-01-01T10:00:00Z                        ranking of one window
 * GET /api/top-pages?limit=5                                            latest rankings
 * GET /api/users/{userId}/sessions                                      sessions of one user
 * GET /health
 * </pre>
 *
 * Times are ISO-8601 instants or epoch milliseconds. {@code from}/{@code to} bound window start
 * times and default to "everything retained". A store that is not queryable yet (startup,
 * rebalance) yields {@code 503}.
 */
public final class QueryServer implements AutoCloseable {

  private final HttpServer server;
  private final ExecutorService executor;

  private QueryServer(HttpServer server, ExecutorService executor) {
    this.server = server;
    this.executor = executor;
  }

  public static QueryServer start(
      InetSocketAddress address, QueryService queries, Supplier<String> health) throws IOException {
    HttpServer server = HttpServer.create(address, 0);
    server.createContext(
        "/api/page-counts",
        exchange ->
            handle(
                exchange,
                params -> {
                  Instant from = instant(params.get("from"), Instant.EPOCH);
                  Instant to = instant(params.get("to"), Instant.ofEpochMilli(Long.MAX_VALUE));
                  String page = params.get("page");
                  return page == null
                      ? queries.allPageCounts(from, to)
                      : queries.pageCounts(page, from, to);
                }));
    server.createContext(
        "/api/top-pages",
        exchange ->
            handle(
                exchange,
                params -> {
                  String window = params.get("window");
                  if (window == null) {
                    return queries.latestTopPages(intParam(params.get("limit"), 10));
                  }
                  return queries
                      .topPages(instant(window, null))
                      .orElseThrow(() -> new NotFound("no ranking for window " + window));
                }));
    server.createContext(
        "/api/users/",
        exchange ->
            handle(
                exchange,
                params -> {
                  String path = exchange.getRequestURI().getPath();
                  String rest = path.substring("/api/users/".length());
                  if (!rest.endsWith("/sessions") || rest.length() == "/sessions".length()) {
                    throw new NotFound("unknown path " + path);
                  }
                  String userId = rest.substring(0, rest.length() - "/sessions".length());
                  return queries.sessions(URLDecoder.decode(userId, StandardCharsets.UTF_8));
                }));
    server.createContext(
        "/health", exchange -> handle(exchange, params -> Map.of("state", health.get())));
    ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor();
    server.setExecutor(executor);
    server.start();
    return new QueryServer(server, executor);
  }

  public int port() {
    return server.getAddress().getPort();
  }

  @Override
  public void close() {
    server.stop(0);
    executor.close();
  }

  @FunctionalInterface
  private interface Handler {
    Object apply(Map<String, String> params);
  }

  private static final class NotFound extends RuntimeException {
    private static final long serialVersionUID = 1L;

    NotFound(String message) {
      super(message);
    }
  }

  private static void handle(HttpExchange exchange, Handler handler) throws IOException {
    int status;
    Object body;
    if (!"GET".equals(exchange.getRequestMethod())) {
      status = 405;
      body = Map.of("error", "only GET is supported");
    } else {
      try {
        body = handler.apply(queryParams(exchange.getRequestURI().getRawQuery()));
        status = 200;
      } catch (NotFound e) {
        status = 404;
        body = Map.of("error", e.getMessage());
      } catch (IllegalArgumentException e) {
        status = 400;
        body = Map.of("error", e.getMessage());
      } catch (InvalidStateStoreException e) {
        status = 503;
        body = Map.of("error", "state store not queryable yet: " + e.getMessage());
      } catch (RuntimeException e) {
        status = 500;
        body = Map.of("error", e.toString());
      }
    }
    byte[] bytes = Json.bytes(body);
    exchange.getResponseHeaders().set("Content-Type", "application/json");
    exchange.sendResponseHeaders(status, bytes.length);
    try (OutputStream out = exchange.getResponseBody()) {
      out.write(bytes);
    }
  }

  static Map<String, String> queryParams(String rawQuery) {
    Map<String, String> params = new HashMap<>();
    if (rawQuery == null || rawQuery.isEmpty()) {
      return params;
    }
    for (String pair : rawQuery.split("&")) {
      int eq = pair.indexOf('=');
      String name = eq < 0 ? pair : pair.substring(0, eq);
      String value = eq < 0 ? "" : pair.substring(eq + 1);
      params.put(
          URLDecoder.decode(name, StandardCharsets.UTF_8),
          URLDecoder.decode(value, StandardCharsets.UTF_8));
    }
    return params;
  }

  /** Parses an ISO-8601 instant or epoch millis; {@code fallback} when absent. */
  static Instant instant(String value, Instant fallback) {
    if (value == null || value.isBlank()) {
      if (fallback == null) {
        throw new IllegalArgumentException("missing time parameter");
      }
      return fallback;
    }
    try {
      return value.chars().allMatch(Character::isDigit)
          ? Instant.ofEpochMilli(Long.parseLong(value))
          : Instant.parse(value);
    } catch (DateTimeParseException | NumberFormatException e) {
      throw new IllegalArgumentException("not an ISO-8601 instant or epoch millis: " + value);
    }
  }

  private static int intParam(String value, int fallback) {
    if (value == null) {
      return fallback;
    }
    try {
      int parsed = Integer.parseInt(value);
      if (parsed < 1) {
        throw new IllegalArgumentException("limit must be >= 1");
      }
      return parsed;
    } catch (NumberFormatException e) {
      throw new IllegalArgumentException("not a number: " + value);
    }
  }
}
