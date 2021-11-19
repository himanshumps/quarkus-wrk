package com.infy.hackathon;

import io.quarkus.runtime.QuarkusApplication;
import io.quarkus.runtime.annotations.QuarkusMain;
import io.vertx.core.MultiMap;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.http.impl.headers.HeadersMultiMap;
import io.vertx.core.impl.cpu.CpuCoreSensor;
import io.vertx.core.net.OpenSSLEngineOptions;
import io.vertx.ext.web.client.HttpResponse;
import io.vertx.ext.web.client.WebClient;
import io.vertx.ext.web.client.WebClientOptions;
import picocli.CommandLine;

import javax.inject.Inject;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;
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

  private AtomicInteger atomicInteger = new AtomicInteger();
  private AtomicInteger outerAtomicInteger = new AtomicInteger();

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
    System.out.println(headersMultiMap);
    System.out.println(url);
    System.out.println(connections);
    System.out.println(threads);
    System.out.println(durationInSec);
    Instant now = Instant.now();
    List<CompletableFuture<HttpResponse<Buffer>>> listOfCompletableFuture = IntStream.range(0, 100).mapToObj(x -> request(WebClient
                    .create(vertx, new WebClientOptions()
                            .setConnectTimeout((int)Duration.ofSeconds(timeoutInSec).toMillis())
                            .setTryUseCompression(true)
                            .setVerifyHost(false)
                            .setReuseAddress(true)
                            .setReusePort(true)
                            .setTcpFastOpen(true)
                            .setTcpNoDelay(true)
                            .setTcpQuickAck(true)
                            .setKeepAlive(true)
                            .setMaxPoolSize(2)
                            .setOpenSslEngineOptions(new OpenSSLEngineOptions().setSessionCacheEnabled(false))), now))
            .collect(Collectors.toList());
    try {
      CompletableFuture.allOf(listOfCompletableFuture.toArray(new CompletableFuture[0])).get();
    } catch (InterruptedException e) {
      e.printStackTrace();
    } catch (ExecutionException e) {
      e.printStackTrace();
    }
    System.out.println("Completed: " + Instant.now());
    System.out.println("Inner Counter: " + atomicInteger.get());
    System.out.println("Outer Counter: " + outerAtomicInteger.get());
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
                outerAtomicInteger.incrementAndGet();
                //System.out.println("Comparing " + Instant.now() + "    " + instant.plusSeconds(100));
                if (Instant.now().isBefore(instant.plusSeconds(10))) {
                  //System.out.println("Inner - Comparing " + Instant.now() + "    " + instant.plusSeconds(10));
                  atomicInteger.incrementAndGet();
                  return request(webClient, instant);
                } else {
                  return CompletableFuture.<io.vertx.ext.web.client.HttpResponse<Buffer>>completedFuture(stringHttpResponse);
                }
              }
            });
  }
}