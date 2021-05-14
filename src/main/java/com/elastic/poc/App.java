package com.elastic.poc;

import java.io.IOException;

import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.builder.SearchSourceBuilder;

/**
 * Hello world!
 *
 */
public class App 
{
    public static void main( String[] args ) throws IOException
    {
        final CredentialsProvider provider = new BasicCredentialsProvider();
        provider.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials("name", "pass"));

        RestClientBuilder builder = RestClient.builder(new HttpHost("6c6bf535e92d458b83c4873d304b2aa8.eastus2.azure.elastic-cloud.com", 9243, "https"))
            .setHttpClientConfigCallback(new RestClientBuilder.HttpClientConfigCallback(){
                @Override
                public HttpAsyncClientBuilder customizeHttpClient(HttpAsyncClientBuilder httpClientBuilder) {
                    httpClientBuilder.disableAuthCaching();
                    return httpClientBuilder.setDefaultCredentialsProvider(provider);
                }
            });
        
        RestHighLevelClient client = new RestHighLevelClient(builder);
        
        ;
        SearchRequest request = new SearchRequest("azure_event_hub-*");
        request.searchType(SearchType.DFS_QUERY_THEN_FETCH);
        request.source(query1(client));
        SearchResponse response = client.search(request, RequestOptions.DEFAULT);
        System.out.println(response.toString());
        System.out.println("============================================\n==========================================");
        request.source(query2(client));
        response = client.search(request, RequestOptions.DEFAULT);
        System.out.println(response.toString());

        client.close();


    }

    private static SearchSourceBuilder query2(RestHighLevelClient client) throws IOException {
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder()
            .query(
                QueryBuilders
                    .boolQuery()
                    .must(QueryBuilders.matchQuery("flow_id", "cb187c48.e3971"))
                    .must(
                        QueryBuilders
                            .rangeQuery("timestamp")
                            .lte("2021-05-12T00:00:00Z")
                            .gte("2021-05-05T00:00:00Z"))
            );
        String[] includes = new String[]{"payload"};
        sourceBuilder.fetchSource(includes, null);
        return sourceBuilder;

    }

    private static SearchSourceBuilder query1(RestHighLevelClient client) throws IOException {
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder()
            .query(
                QueryBuilders
                    .rangeQuery("timestamp")
                    .lte("2021-05-12T00:00:00Z")
                    .gte("2021-05-05T00:00:00Z"));
        String[] includes = new String[]{"flow_id", "timestamp", "topic"};
        sourceBuilder.fetchSource(includes, null);
        return sourceBuilder;

    }
}
