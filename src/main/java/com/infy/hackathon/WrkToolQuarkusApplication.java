package com.infy.hackathon;

import io.quarkus.runtime.QuarkusApplication;
import io.quarkus.runtime.annotations.QuarkusMain;
import io.vertx.core.MultiMap;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.http.impl.headers.HeadersMultiMap;
import io.vertx.core.impl.cpu.CpuCoreSensor;
import io.vertx.ext.web.client.HttpResponse;
import io.vertx.ext.web.client.WebClient;
import io.vertx.ext.web.client.WebClientOptions;
import picocli.CommandLine;

import javax.inject.Inject;
import java.text.CharacterIterator;
import java.text.MessageFormat;
import java.text.StringCharacterIterator;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalTime;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

@QuarkusMain
@CommandLine.Command(name = "demo", mixinStandardHelpOptions = true)
public class WrkToolQuarkusApplication implements Runnable, QuarkusApplication {
  @Inject
  CommandLine.IFactory factory;

  private final Vertx vertx;

  @CommandLine.Option(names = {"-c", "--connections"}, description = "Connections to keep open", defaultValue = "20", showDefaultValue = CommandLine.Help.Visibility.ALWAYS)
  private Integer connections;

  @CommandLine.Option(names = {"-d", "--duration"}, description = "Duration of test in seconds", defaultValue = "30", showDefaultValue = CommandLine.Help.Visibility.ALWAYS)
  private Integer durationInSec;

  @CommandLine.Option(names = {"-t", "--threads"}, description = "Number of threads to use (2 * Available Processors). It is not advised to pass this property and let it use the system default")
  private Integer threads = CpuCoreSensor.availableProcessors() * 2;

  @CommandLine.Option(names = {"-H", "--header"}, description = "Add header to request")
  private List<String> headers;

  @CommandLine.Option(names = "-L", description = "Print latency statistics")
  private boolean latency;

  @CommandLine.Option(names = "--timeout", description = "Socket/request timeout in seconds", defaultValue = "30", showDefaultValue = CommandLine.Help.Visibility.ALWAYS)
  private Integer timeoutInSec;

  @CommandLine.Parameters(index = "0", description = "The url to hit", defaultValue = "http://localhost:8080/", showDefaultValue = CommandLine.Help.Visibility.ALWAYS)
  private String url;

  MultiMap headersMultiMap = new HeadersMultiMap();

  private AtomicInteger requestCounter = new AtomicInteger();
  private AtomicLong bytesCounter = new AtomicLong();

  public WrkToolQuarkusApplication(Vertx vertx) {
    this.vertx = vertx;
  }

  @Override
  public int run(String... args) throws Exception {
    return new CommandLine(this, factory).execute(args);
  }

  @Override
  public void run() {
    for (String header : headers) {
      headersMultiMap.add(
              header.substring(0, header.indexOf(":")).trim().toLowerCase(Locale.ROOT),
              header.substring(header.indexOf(":") + 1).trim()
      );
    }
    System.out.println(MessageFormat.format("Running {0} test @ {1}", LocalTime.ofSecondOfDay(durationInSec), url));
    System.out.println(MessageFormat.format("{0,number,#} threads and {1,number,#} connections", CpuCoreSensor.availableProcessors() * 2, connections));
    Instant now = Instant.now();
    List<CompletableFuture<HttpResponse<Buffer>>> listOfCompletableFuture = IntStream.range(0, 100).mapToObj(x -> request(WebClient
                    .create(vertx, new WebClientOptions()
                            .setConnectTimeout((int) Duration.ofSeconds(timeoutInSec).toMillis())
                            .setTryUseCompression(true)
                            .setVerifyHost(false)
                            .setReuseAddress(true)
                            .setReusePort(true)
                            .setTcpFastOpen(true)
                            .setTcpNoDelay(true)
                            .setTcpQuickAck(true)
                            .setKeepAlive(true)
                            //.setOpenSslEngineOptions(new OpenSSLEngineOptions().setSessionCacheEnabled(false))
                            .setMaxPoolSize(2)), now))
            .collect(Collectors.toList());
    try {
      CompletableFuture.allOf(listOfCompletableFuture.toArray(new CompletableFuture[0])).get();
    } catch (InterruptedException e) {
      e.printStackTrace();
    } catch (ExecutionException e) {
      e.printStackTrace();
    }
    System.out.println(MessageFormat.format("\n\n{0} requests in {1}s, {1} read", requestCounter.get(), durationInSec, humanReadableByteCountSI(bytesCounter.get())));
    System.out.println(MessageFormat.format("Requests/sec: {0,number,#}", ((int)(requestCounter.get()/durationInSec))));
    System.out.println(MessageFormat.format("Transfer/sec: {0}", humanReadableByteCountSI(bytesCounter.get()/durationInSec)));
  }

  public CompletableFuture<io.vertx.ext.web.client.HttpResponse<Buffer>> request(WebClient webClient, Instant instant) {
    return webClient.getAbs(url)
            .ssl(url.startsWith("https") ? true : false)
            .putHeaders(headersMultiMap)
            .send()
            .toCompletionStage()
            .toCompletableFuture()
            .thenComposeAsync(new Function<HttpResponse<Buffer>, CompletionStage<HttpResponse<Buffer>>>() {
              @Override
              public CompletionStage<io.vertx.ext.web.client.HttpResponse<Buffer>> apply(io.vertx.ext.web.client.HttpResponse<Buffer> stringHttpResponse) {
                if (Instant.now().isBefore(instant.plusSeconds(durationInSec))) {
                  requestCounter.incrementAndGet();
                  bytesCounter.addAndGet(stringHttpResponse.bodyAsString().getBytes().length);
                  return request(webClient, instant);
                } else {
                  webClient.close();
                  return CompletableFuture.<io.vertx.ext.web.client.HttpResponse<Buffer>>completedFuture(stringHttpResponse);
                }
              }
            });
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
