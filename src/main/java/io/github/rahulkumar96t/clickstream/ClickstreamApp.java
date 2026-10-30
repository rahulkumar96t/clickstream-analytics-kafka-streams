package io.github.rahulkumar96t.clickstream;

import io.github.rahulkumar96t.clickstream.api.ClickstreamStores;
import io.github.rahulkumar96t.clickstream.api.QueryServer;
import io.github.rahulkumar96t.clickstream.api.QueryService;
import io.github.rahulkumar96t.clickstream.topology.ClickstreamTopology;
import java.net.InetSocketAddress;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.errors.TopicExistsException;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler.StreamThreadExceptionResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Runs the topology against a real Kafka cluster and serves the REST API. Configure it with the
 * environment variables listed in {@link AppSettings}.
 */
public final class ClickstreamApp {

  private static final Logger LOG = LoggerFactory.getLogger(ClickstreamApp.class);

  private ClickstreamApp() {}

  public static void main(String[] args) throws Exception {
    AppSettings settings = AppSettings.fromEnv(System.getenv());
    createTopicsIfMissing(settings);

    KafkaStreams streams =
        new KafkaStreams(
            ClickstreamTopology.build(settings.topology()), settings.streamsProperties());
    streams.setUncaughtExceptionHandler(
        e -> {
          LOG.error("stream thread failed; replacing it", e);
          return StreamThreadExceptionResponse.REPLACE_THREAD;
        });
    QueryService queries =
        new QueryService(ClickstreamStores.of(streams), settings.topology().countWindow());
    QueryServer api =
        QueryServer.start(
            new InetSocketAddress(settings.httpPort()), queries, () -> streams.state().name());

    CountDownLatch stopped = new CountDownLatch(1);
    Runtime.getRuntime()
        .addShutdownHook(
            new Thread(
                () -> {
                  api.close();
                  streams.close();
                  stopped.countDown();
                },
                "shutdown"));
    streams.start();
    LOG.info(
        "clickstream analytics running; API on http://localhost:{}/api/top-pages",
        settings.httpPort());
    stopped.await();
  }

  /** Kafka Streams creates its internal topics but not the source and sink topics. */
  static void createTopicsIfMissing(AppSettings settings) throws InterruptedException {
    Properties props = new Properties();
    props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, settings.bootstrapServers());
    try (Admin admin = Admin.create(props)) {
      Set<String> existing = admin.listTopics().names().get();
      List<NewTopic> missing =
          settings.topology().topics().stream()
              .filter(topic -> !existing.contains(topic))
              .map(topic -> new NewTopic(topic, settings.partitions(), settings.replication()))
              .toList();
      if (!missing.isEmpty()) {
        admin.createTopics(missing).all().get();
        LOG.info("created topics {}", missing.stream().map(NewTopic::name).toList());
      }
    } catch (ExecutionException e) {
      if (!(e.getCause() instanceof TopicExistsException)) {
        throw new IllegalStateException("could not create topics", e.getCause());
      }
    }
  }
}
