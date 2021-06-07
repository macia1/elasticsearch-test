package elasticsearch;

import lombok.extern.log4j.Log4j2;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.logging.log4j.core.util.datetime.FastDateFormat;
import org.elasticsearch.action.admin.indices.alias.get.GetAliasesRequest;
import org.elasticsearch.action.search.ClearScrollRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchScrollRequest;
import org.elasticsearch.client.*;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.MatchQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.TermsQueryBuilder;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.builder.SearchSourceBuilder;

import java.io.IOException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;

/**
 * @Author 朝花夕誓
 * @Version 1.0
 * @Description elasticsearch 测试
 */
@Log4j2
public class Main extends ArrayList<String> {

    public static final String ELASTICSEARCH_IP = "192.168.0.22";
    public static final int ELASTICSEARCH_PORT = 9300;
    public static final RestHighLevelClient REST_HIGH_LEVEL_CLIENT;

    public static final String userName = "user";
    public static final String password = "pwd";

    static {
        HttpHost httpHost = new HttpHost(ELASTICSEARCH_IP, ELASTICSEARCH_PORT, "http");
        RestClientBuilder restClientBuilder = RestClient.builder(httpHost);
        //设置身份验证
        RestClientBuilder.HttpClientConfigCallback clientConfigCallback = httpClientBuilder -> {
            CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
            credentialsProvider.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(
                    // 用户名 密码
                    userName, password
            ));
            httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider);
            return httpClientBuilder;
        };
        restClientBuilder.setHttpClientConfigCallback(clientConfigCallback);
        REST_HIGH_LEVEL_CLIENT = new RestHighLevelClient(restClientBuilder);
        // 验证连接有效性
        try {
            REST_HIGH_LEVEL_CLIENT.ping(RequestOptions.DEFAULT);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    FastDateFormat instance = FastDateFormat.getInstance("yyyy-MM-dd HH:mm:ss");

    public long getTimeStamp(String time) throws ParseException {
        return instance.parse(time).getTime();
    }

    public static void main(String[] args) throws ParseException, IOException {
        try {
            Main main = new Main();
            // 索引前缀
            String indexesPre = "index_pre";
            // 获取时间区间
            long startTimeStamp = main.getTimeStamp("2019-09-01 00:00:00");
            long endTimeStamp = main.getTimeStamp("2021-06-01 00:00:00");

            // 获取索引
            List<String> totalIndexes = main.getTotalIndexes(indexesPre, startTimeStamp, endTimeStamp);

            totalIndexes.forEach(index->{
                try {
                    main.getEsData(index);
                } catch (IOException e) {
                    e.printStackTrace();
                }
            });
        } finally {
            REST_HIGH_LEVEL_CLIENT.close();
        }
    }

    /**
     * 且关系查询
     * @param boolQueryBuilder
     * @param strings
     */
    public void splitMustWordBuilder(BoolQueryBuilder boolQueryBuilder, String... strings){
        int i = 0;
        for (; i < strings.length; i++) {
            MatchQueryBuilder ind_full_text = QueryBuilders.matchQuery("elasticsearch_field_01", strings[i]);
            MatchQueryBuilder ind_title = QueryBuilders.matchQuery("elasticsearch_field_02", strings[i]);
            boolQueryBuilder.must(ind_full_text);
            boolQueryBuilder.must(ind_title);
        }
    }

    /**
     * 或关系查询
     * @param boolQueryBuilder
     * @param strings
     */
    public void splitShouldWordBuilder(BoolQueryBuilder boolQueryBuilder, String... strings){
        int i = 0;
        for (; i < strings.length; i++) {
            MatchQueryBuilder ind_full_text = QueryBuilders.matchQuery("ind_full_text", strings[i]);
            MatchQueryBuilder ind_title = QueryBuilders.matchQuery("ind_title", strings[i]);
            boolQueryBuilder.should(ind_full_text);
            boolQueryBuilder.should(ind_title);
        }
    }

    /**
     * 根据索引获响应内容
     * @param index
     */
    public void getEsData(String index) throws IOException {
        // bool关系
        BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery();

        TermsQueryBuilder source = QueryBuilders.termsQuery("elasticsearch_source", new String[]{"source01", "source02"});

        boolQueryBuilder.must(source);
        // 分词且查询
        splitMustWordBuilder(boolQueryBuilder, new String[]{"mast query01", "mast query02", "mast query03"});
        // 分词或查询
        splitShouldWordBuilder(boolQueryBuilder, new String[]{"should search01", "should search02"});

        // 搜索查询
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        // 放入searchSourceBuilder
        SearchSourceBuilder query = searchSourceBuilder.query(boolQueryBuilder);
        // 返回限制
        searchSourceBuilder.size(500);

        // 与指定索引建立连接
        SearchRequest searchRequest = new SearchRequest(index);
        // 游标存活时间   每个用户最大存放500个游标连接，超过就无法申请，需要申请删除游标
        searchRequest.scroll(TimeValue.timeValueMinutes(1));
        // 加入searchRequest请求头
        searchRequest.source(query);

        // 发起SearchRequest查询
        SearchResponse searchResponse = REST_HIGH_LEVEL_CLIENT.search(searchRequest, RequestOptions.DEFAULT);

        // 第一次命中处理
        elasticsearchResponseDeal(searchResponse);

        // 命中数据带回来的信息
        SearchHits hits = searchResponse.getHits();

        // 总数量不是限制的500
        long count = hits.getTotalHits().value;
        int pageSize = (int)Math.ceil(count / 500.0);

        log.info("开始 {} 索引查询, 数据总量 {}", index, count);

        // 获取游标，用于下次翻页
        String cursor = searchResponse.getScrollId();

        for (int i = 1; i < pageSize; i++) {
            log.info("pageSize is {}, i is {}", pageSize, i);
            // 放入游标进行滚动查询
            SearchScrollRequest searchScrollRequest = new SearchScrollRequest(cursor);
            searchScrollRequest.scroll(TimeValue.timeValueMinutes(1));
            // 滚动查询
            SearchResponse searchResponseScroll = REST_HIGH_LEVEL_CLIENT.scroll(searchScrollRequest, RequestOptions.DEFAULT);
            // 再次获取游标
            cursor = searchResponseScroll.getScrollId();

            elasticsearchResponseDeal(searchResponseScroll);
        }
        // 清除查询游标
        clearScroll(cursor);
    }

    public void clearScroll(String scrollId) throws IOException {
        ClearScrollRequest clearScrollRequest = new ClearScrollRequest();
        // 多个索引
        // clearScrollRequest.setScrollIds(scrollIds);
        clearScrollRequest.addScrollId(scrollId);
        REST_HIGH_LEVEL_CLIENT.clearScroll(clearScrollRequest, RequestOptions.DEFAULT);
    }


    /**
     * 响应数据处理
     * @param searchResponse 响应内容
     */
    public void elasticsearchResponseDeal(SearchResponse searchResponse){
        SearchHit[] hits = searchResponse.getHits().getHits();
        if (hits.length > 0){
            for (SearchHit hit : hits) {
                log.info("elasticsearch data ：{}", hit.getSourceAsMap().toString());
            }
        }

    }

    /**
     * 获取索引
     */
    public void getIndex(){
        GetAliasesRequest getAliasesRequest = new GetAliasesRequest();
        GetAliasesResponse alias = null;
        try {
            alias = REST_HIGH_LEVEL_CLIENT.indices().getAlias(getAliasesRequest, RequestOptions.DEFAULT);
        } catch (IOException e) {
            log.error("获取索引失败");
        }
        alias.getAliases().keySet().forEach(index->{
            this.add(index);
        });
    }

    /**
     * 拼接索引信息
     * @param indexesPre 索取前缀
     * @param start 开始时间戳
     * @param end 结束时间戳
     * @return 索引列表
     */
    public List<String> getTotalIndexes(String indexesPre, long start, long end){
        Calendar calendar = Calendar.getInstance();
        // 初始化 calendar 时间为开始时间
        calendar.setTimeInMillis(start);
        List<String> indexes = new ArrayList<>();
        // 获取索引列表
        getIndex();
        do {
            this.forEach(it->{
                int year, month;
                year = calendar.get(Calendar.YEAR);
                month = calendar.get(Calendar.MONTH) + 1;
                String cursor;
                // 小于10补充0
                cursor = year + (month < 10 ? "0" + month : Integer.toString(month));
                if (it.startsWith(indexesPre) && it.contains(cursor)){
                    indexes.add(it);
                }
            });
            calendar.add(Calendar.MONTH, 1);
        }while (calendar.getTimeInMillis() <= end);

        return indexes;
    }

}
