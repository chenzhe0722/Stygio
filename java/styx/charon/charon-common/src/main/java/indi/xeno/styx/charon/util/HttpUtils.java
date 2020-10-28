package indi.xeno.styx.charon.util;

import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.net.http.HttpResponse.BodyHandler;
import java.util.concurrent.CompletableFuture;

import static java.net.URI.create;
import static java.net.http.HttpClient.newHttpClient;
import static java.net.http.HttpRequest.newBuilder;

public abstract class HttpUtils {

  private HttpUtils() {}

  private static final HttpClient CLIENT = newHttpClient();

  public static <T> CompletableFuture<HttpResponse<T>> httpGet(String uri, BodyHandler<T> handler) {
    HttpRequest request = newBuilder(create(uri)).GET().build();
    return CLIENT.sendAsync(request, handler);
  }
}
