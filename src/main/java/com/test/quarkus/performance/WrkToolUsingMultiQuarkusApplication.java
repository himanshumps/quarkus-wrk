package com.test.quarkus.performance;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tags;
import io.netty.channel.epoll.Epoll;
import io.netty.handler.ssl.OpenSsl;
import io.quarkus.runtime.QuarkusApplication;
import io.quarkus.runtime.annotations.QuarkusMain;
import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.subscription.MultiSubscriber;
import io.vertx.core.http.HttpMethod;
import io.vertx.core.http.HttpVersion;
import io.vertx.core.impl.cpu.CpuCoreSensor;
import io.vertx.core.net.JdkSSLEngineOptions;
import io.vertx.core.net.OpenSSLEngineOptions;
import io.vertx.ext.web.client.WebClientOptions;
import io.vertx.mutiny.core.MultiMap;
import io.vertx.mutiny.core.Vertx;
import io.vertx.mutiny.core.buffer.Buffer;
import io.vertx.mutiny.ext.web.client.HttpResponse;
import io.vertx.mutiny.ext.web.client.WebClient;
import org.reactivestreams.Subscription;
import picocli.CommandLine;

import javax.annotation.PostConstruct;
import javax.inject.Inject;
import java.text.CharacterIterator;
import java.text.MessageFormat;
import java.text.StringCharacterIterator;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalTime;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Stream;

@QuarkusMain
@CommandLine.Command(name = "demo", mixinStandardHelpOptions = true)
public class WrkToolUsingMultiQuarkusApplication implements Runnable, QuarkusApplication {
  @Inject
  CommandLine.IFactory factory;

  private Vertx vertx;

  private final MeterRegistry registry;

  @PostConstruct
  void initialize() {

        /*VertxOptions vertxOptions = new VertxOptions().setMetricsOptions(
                new MicrometerMetricsOptions()
                        .setMicrometerRegistry(registry)
                        .setEnabled(true));

        vertx = Vertx.vertx(vertxOptions);*/

    System.out.println("Is Vertx Metrics Enabled - " + vertx.isMetricsEnabled());


  }

  @CommandLine.Option(names = {"-c", "--connections"}, description = "Connections to keep open", defaultValue = "100", showDefaultValue = CommandLine.Help.Visibility.ALWAYS)
  private Integer connections;

  @CommandLine.Option(names = {"-d", "--duration"}, description = "Duration of test in seconds", defaultValue = "30", showDefaultValue = CommandLine.Help.Visibility.ALWAYS)
  private Integer durationInSec;

  @CommandLine.Option(names = {"-t", "--threads"}, description = "Number of event loop threads to use (2 * Available Processors). It is not advised to pass this property and let it use the system generated")
  private Integer threads = CpuCoreSensor.availableProcessors() * 2;

  @CommandLine.Option(names = {"-H", "--header"}, description = "Add header to request")
  private List<String> headers;

  @CommandLine.Option(names = "-L", description = "Print latency statistics")
  private boolean latency;

  @CommandLine.Option(names = "--timeout", description = "Socket/request timeout in seconds", defaultValue = "30", showDefaultValue = CommandLine.Help.Visibility.ALWAYS)
  private Integer timeoutInSec;

  @CommandLine.Parameters(index = "0", description = "The url to hit", defaultValue = "http://localhost:8080/", showDefaultValue = CommandLine.Help.Visibility.ALWAYS)
  private String url;

  MultiMap headersMultiMap = MultiMap.caseInsensitiveMultiMap();

  private final AtomicInteger requestCounter = new AtomicInteger();
  private final AtomicLong bytesCounter = new AtomicLong();
  //private MetricsService metricsService;
  /*@BuildStep(onlyIf = VertxBinderEnabled.class)
  @io.quarkus.deployment.annotations.Record(value = ExecutionTime.STATIC_INIT)
  VertxOptionsConsumerBuildItem build(MeterRegistry registry) {
    return new VertxOptionsConsumerBuildItem(new Consumer<VertxOptions>() {
      @Override
      public void accept(VertxOptions vertxOptions) {
        System.out.println("Applying vertx options");
        vertxOptions.setMetricsOptions(new MicrometerMetricsOptions().setMicrometerRegistry(registry).setEnabled(true));
      }
    }, Interceptor.Priority.APPLICATION);
  }
  static class VertxBinderEnabled implements BooleanSupplier {
    public boolean getAsBoolean() {
      return true;
    }
  }*/

  public WrkToolUsingMultiQuarkusApplication(Vertx vertx, MeterRegistry registry) {
    System.out.println("In constructor");
    this.vertx = vertx;
    this.registry = registry;
    this.registry.gauge("http_request_count", Tags.empty(), requestCounter);
  }

  @Override
  public int run(String... args) {
    return new CommandLine(this, factory).execute(args);
  }

  @Override
  public void run() {
    System.out.println("vertx.isNativeTransportEnabled(): " + vertx.isNativeTransportEnabled());
    System.out.println("OpenSsl.isAvailable(): " + OpenSsl.isAvailable());
    if (headers != null) {
      for (String header : headers) {
        headersMultiMap.add(
                header.substring(0, header.indexOf(":")).trim().toLowerCase(Locale.ROOT),
                header.substring(header.indexOf(":") + 1).trim()
        );
      }
    }
    System.out.println(MessageFormat.format("\nRunning {0} test @ {1}", LocalTime.ofSecondOfDay(durationInSec), url));
    System.out.println(MessageFormat.format("{0,number,#} event loop and {1,number,#} connections", CpuCoreSensor.availableProcessors() * 2, connections));
    WebClientOptions webClientOptions = new WebClientOptions()
            .setTryUseCompression(true)
            .setConnectTimeout((int) Duration.ofSeconds(timeoutInSec).toMillis())
            .setUserAgent("quarkus-wrk")
            .setPipelining(false)
            .setVerifyHost(false)
            .setReuseAddress(true)
            .setKeepAlive(true)
            .setPoolCleanerPeriod((int) Duration.ofSeconds(durationInSec + 5).toMillis())
            .setUseAlpn(false)
            .setMetricsName("Webclient")
            .setProtocolVersion(HttpVersion.HTTP_1_1)
            .setMaxPoolSize(connections)
            .setSslEngineOptions(OpenSsl.isAvailable() ? new OpenSSLEngineOptions().setSessionCacheEnabled(false) : new JdkSSLEngineOptions());
    //only for native transport - Better Performance
    if (Epoll.isAvailable()) {
      webClientOptions
              .setReusePort(true)
              .setTcpCork(true)
              .setTcpQuickAck(true)
              .setTcpFastOpen(true);
    }
    final WebClient webClient = WebClient.create(vertx, webClientOptions);
    //metricsService = MetricsService.create(vertx);
    Instant startTime = Instant.now();
    Instant endTime = Instant.now();
    CountDownLatch countDownLatch = new CountDownLatch(1);
    //HTTPRequestAndBodyMulti<HTTPRequestAndBody> httpRequestAndBodyHTTPRequestAndBodyMulti = new HTTPRequestAndBodyMulti<>(webClient, HttpMethod.GET, url, headersMultiMap, Buffer.buffer());
    Multi.createFrom().items(Stream.generate(new HTTPRequestAndBodySupplier(webClient, HttpMethod.GET, url, headersMultiMap, Buffer.buffer())))
            .emitOn(vertx.nettyEventLoopGroup())
            .select().first(Duration.ofSeconds(durationInSec))
            .onItem().transformToMulti(item -> item.httpRequest().send()/*.invoke(() -> System.out.println(Thread.currentThread().getName()))*/.toMulti()).withRequests(connections).merge()
            .subscribe().withSubscriber(new MultiSubscriber<HttpResponse<Buffer>>() {
              Subscription subscription;

              @Override
              public void onItem(HttpResponse<Buffer> item) {
                /*System.out.println("onItem: " + Thread.currentThread().getName());*/
                /*if (requestCounter.get() % 500 == 0)
                  System.out.println(metricsService.getMetricsSnapshot());
                */
                requestCounter.incrementAndGet();
                bytesCounter.addAndGet(item.bodyAsString().length());
                subscription.request(5);
              }

              @Override
              public void onFailure(Throwable failure) {
                failure.printStackTrace();
                countDownLatch.countDown();
              }

              @Override
              public void onCompletion() {
                countDownLatch.countDown();
              }

              @Override
              public void onSubscribe(Subscription subscription) {
                this.subscription = subscription;
                subscription.request(connections);
              }
            });
    try {
      countDownLatch.await();
      endTime = Instant.now();
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
    webClient.close();
    try {
      vertx.close().subscribeAsCompletionStage().get();
    } catch (InterruptedException | ExecutionException e) {
      e.printStackTrace();
    }

    Duration difference = Duration.between(startTime, endTime);
    System.out.println(MessageFormat.format("\n{0,number,#} requests in {1}.{2,number,#}, {3} read", requestCounter.get(), LocalTime.ofSecondOfDay(difference.getSeconds()), difference.getNano(), humanReadableByteCountSI(bytesCounter.get())));
    System.out.println(MessageFormat.format("Requests/sec: {0,number,#}", requestCounter.get() / difference.getSeconds()));
    System.out.println(MessageFormat.format("Transfer/sec: {0}", humanReadableByteCountSI(bytesCounter.get() / difference.getSeconds())));
  }

  public static String humanReadableByteCountSI(long bytes) {
    if (-1000 < bytes && bytes < 1000) {
      return bytes + " B";
    }
    CharacterIterator ci = new StringCharacterIterator("kMGTPE");
    while (bytes <= -999_950 || bytes >= 999_950) {
      bytes /= 1000;
      ci.next();
    }
    return String.format("%.1f%cB", bytes / 1000.0, ci.current());
  }
}
