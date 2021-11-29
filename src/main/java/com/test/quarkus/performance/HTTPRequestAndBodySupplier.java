package com.test.quarkus.performance;

import io.vertx.core.http.HttpMethod;
import io.vertx.mutiny.core.MultiMap;
import io.vertx.mutiny.core.buffer.Buffer;
import io.vertx.mutiny.ext.web.client.WebClient;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;

public class HTTPRequestAndBodySupplier implements Supplier<HTTPRequestAndBody> {
  private final List<HTTPRequestAndBody> httpRequestAndBodyList = new ArrayList<>();
  private final int listSize;
  private int counter = 0;
  public HTTPRequestAndBodySupplier(WebClient webClient, HttpMethod httpMethod, String url, MultiMap multiMapHeaders, Buffer body) {
    httpRequestAndBodyList.add(new HTTPRequestAndBody(webClient.getAbs(url).putHeaders(multiMapHeaders), body));
    listSize = httpRequestAndBodyList.size();
  }

  @Override
  public HTTPRequestAndBody get() {
    return httpRequestAndBodyList.get(counter++ % listSize);
  }
}
