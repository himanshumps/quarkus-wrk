package com.infy.hackathon;

import io.quarkus.runtime.QuarkusApplication;
import io.quarkus.runtime.annotations.QuarkusMain;
import io.vertx.core.MultiMap;
import io.vertx.core.Vertx;
import io.vertx.core.VertxOptions;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.http.impl.headers.HeadersMultiMap;
import io.vertx.core.impl.cpu.CpuCoreSensor;
import io.vertx.core.net.OpenSSLEngineOptions;
import io.vertx.ext.web.client.HttpResponse;
import io.vertx.ext.web.client.WebClient;
import io.vertx.ext.web.client.WebClientOptions;
import picocli.CommandLine;

import javax.inject.Inject;
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
public class HackthonQuarkusApplication implements Runnable, QuarkusApplication {
  @Inject
  CommandLine.IFactory factory;

  private Vertx vertx;

  @CommandLine.Option(names = {"-c", "--connections"}, description = "Connections to keep open")
  private Integer connections = 20;

  @CommandLine.Option(names = {"-d", "--duration"}, description = "Duration of test in seconds")
  private Integer durationInSec = 30;

  @CommandLine.Option(names = {"-t", "--threads"}, description = "Number of threads to use. It should be (2 * Available Processors)")
  private Integer threads = 2;

  @CommandLine.Option(names = {"-H", "--header"}, description = "Add header to request")
  private List<String> headers;

  @CommandLine.Option(names = "-L", description = "Print latency statistics")
  private boolean latency;

  @CommandLine.Option(names = "--timeout", description = "Socket/request timeout in seconds")
  private Integer timeoutInSec = 30;

  @CommandLine.Parameters(index = "0", description = "The url to hit")
  private String url;

  MultiMap headersMultiMap = new HeadersMultiMap();

  private AtomicInteger atomicInteger = new AtomicInteger();
  private AtomicInteger outerAtomicInteger = new AtomicInteger();

  @Override
  public int run(String... args) throws Exception {
    System.setProperty("vertx.disableDnsResolver", "true");
    vertx = Vertx.vertx(new VertxOptions().setEventLoopPoolSize(threads).setPreferNativeTransport(true));
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
                            .setTryUseCompression(true)
                            .setVerifyHost(false)
                            .setReuseAddress(true)
                            .setReusePort(true)
                            .setTcpFastOpen(true)
                            .setTcpNoDelay(true)
                            .setTcpQuickAck(true)
                            .setKeepAlive(true)
                            .setMaxPoolSize(2)
                            .setOpenSslEngineOptions(new OpenSSLEngineOptions().setSessionCacheEnabled(false))), url, headersMultiMap, now))
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

  public CompletableFuture<io.vertx.ext.web.client.HttpResponse<Buffer>> request(WebClient webClient, String url, MultiMap headersMultiMap, Instant instant) {
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
                  return request(webClient, url, headersMultiMap, instant);
                } else {
                  return CompletableFuture.<io.vertx.ext.web.client.HttpResponse<Buffer>>completedFuture(stringHttpResponse);
                }
              }
            });
  }
}