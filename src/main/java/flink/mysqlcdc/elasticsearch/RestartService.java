package flink.mysqlcdc.elasticsearch;

import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.CloseableHttpResponse;

class RestartService {
    private final String restartUrl;
    private final int connectTimeout;
    private final int socketTimeout;

    public RestartService(String restartUrl, int connectTimeout, int socketTimeout) {
        this.restartUrl = restartUrl;
        this.connectTimeout = connectTimeout;
        this.socketTimeout = socketTimeout;
    }

    public void sendRequest() {
        RequestConfig config = RequestConfig.custom()
            .setConnectTimeout(connectTimeout)
            .setSocketTimeout(socketTimeout)
            .build();

        try (CloseableHttpClient httpClient = HttpClients
            .custom()
            .setDefaultRequestConfig(config)
            .build()) {
            HttpPost postRequest = new HttpPost(restartUrl);
            try (CloseableHttpResponse response = httpClient.execute(postRequest)) {} catch (Exception e) {}
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}