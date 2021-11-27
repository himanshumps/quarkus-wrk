package com.infy.hackathon;

import io.vertx.mutiny.core.buffer.Buffer;
import io.vertx.mutiny.ext.web.client.HttpRequest;

public record HTTPRequestAndBody(HttpRequest httpRequest, Buffer body) {
}
