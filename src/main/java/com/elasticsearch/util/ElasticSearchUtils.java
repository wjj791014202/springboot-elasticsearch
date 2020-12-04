package com.elasticsearch.util;

import com.elasticsearch.Model.Es;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptType;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.BucketOrder;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.metrics.max.ParsedMax;
import org.elasticsearch.search.aggregations.metrics.sum.Sum;
import org.elasticsearch.search.aggregations.pipeline.PipelineAggregatorBuilders;
import org.elasticsearch.search.builder.SearchSourceBuilder;

import java.util.*;


public class ElasticSearchUtils {

    private static Logger logger;

    static {
        logger = LogManager.getFormatterLogger();
    }


    /**
     * 此方法在程序启动时被调用一次。
     * 数据保存到ES时已经将该数据来自topic的哪个分区写入数据中，此方法根据kafka分区号查询已写入ES中各分区当前offset的最大值，
     * 并按照（分区号，ES中该分区最大的offset）形式返回一个HashMap，下次重启程序时从各分区最大offset+1处开始消费。
     * 注意：第一次启动程序时，ES中此Index中没有相应的offset则返回（分区号，-9223372036854775808），即long类型的最小值
     *
     * @param index          ES中对应的索引
     * @param numOfPartition Kafka中topic的分区数量
     * @return
     */
    public static HashMap<Integer, Long> QueryLargestOffset(String index, int numOfPartition) {
        RestHighLevelClient client = null;
        try {
            HashMap<Integer, Long> partitionAndLargestOffset = new HashMap();
            client = ElasticSearchPoolUtil.getClient();
            //从0~numOfPartition-1（即分区号）循环查询ES中当前最大的offset
            for (int i = 0; i < numOfPartition; i++) {
                SearchRequest searchRequest = new SearchRequest(index);
                SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
                //kafkaPartition和下面的consumer_offset是ES中的字段名
                searchSourceBuilder.query(QueryBuilders.matchPhraseQuery("kafkaPartition", i));
                //maxOffset是给这个聚合取得别名，可以随意指定
                searchSourceBuilder.aggregation(AggregationBuilders.max("maxOffset").field("consumer_offset"));
                searchRequest.source(searchSourceBuilder);
                SearchResponse search = client.search(searchRequest);
                ParsedMax maxOffset = search.getAggregations().get("maxOffset");
                //这里保存为long类型，是为了在main方法中解析方便，否则会默认保存为Double类型。
                partitionAndLargestOffset.put(i, (long) maxOffset.getValue());
            }
            return partitionAndLargestOffset;
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            ElasticSearchPoolUtil.returnClient(client);
        }
        return null;
    }

    /**
     * 测试用
     *
     * @param args
     */
    public static void main(String[] args) {
        HashMap es_analytics_data = QueryLargestOffset("es_analytics_data", 3);
        System.out.println(es_analytics_data);
    }

    /**
     * 将数据插入ElasticSearch中
     *
     * @param jsonString
     * @param index
     */
    public static void indexForJson(String index, String type, String id,String jsonString) {

        try {
            IndexRequest indexRequest = new IndexRequest(index, type,id);
            indexRequest.source(jsonString, XContentType.JSON);
            RestHighLevelClient client = ElasticSearchPoolUtil.getClient();
            client.index(indexRequest);
            ElasticSearchPoolUtil.returnClient(client);
            logger.info("ES中写入数据: " + jsonString);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void updateForJson(String index, String type, String id,String jsonString) {

        try {
            UpdateRequest indexRequest = new UpdateRequest(index, type, id);
            indexRequest.doc(jsonString, XContentType.JSON);
            RestHighLevelClient client = ElasticSearchPoolUtil.getClient();
            client.update(indexRequest);
            ElasticSearchPoolUtil.returnClient(client);
            logger.info("ES中写入数据: " + jsonString);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void deleteForJson(String index, String type, String id) {

        try {
            DeleteRequest indexRequest = new DeleteRequest(index, type, id);
            RestHighLevelClient client = ElasticSearchPoolUtil.getClient();
            client.delete(indexRequest);
            ElasticSearchPoolUtil.returnClient(client);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


    public static List<Map<String, Object>> queryListFromES(String index, String type, int storeId, String storeName, String startDate, String endDate) {

        try {
            List<Map<String, Object>> list = new ArrayList<>();

            Map<String, Object> map = Collections.emptyMap();

            Script script = new Script(ScriptType.INLINE, "painless", "params._value0 > 0", map);  //提前定义好查询销量是否大于1000的脚本，类似SQL里面的having
            SearchRequest searchRequest = new SearchRequest(index, type);
            long beginTime = System.currentTimeMillis();
            RestHighLevelClient client = ElasticSearchPoolUtil.getClient();
            SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
            searchSourceBuilder.query(QueryBuilders.boolQuery()
                    .must(QueryBuilders.termQuery("store_id", storeId))  //挨个设置查询条件，没有就不加，如果是字符串类型的，要加keyword后缀
                    .must(QueryBuilders.termQuery("store_name.keyword", storeName))
                    .must(QueryBuilders.rangeQuery("pay_date.keyword").gte(startDate).lte(endDate)))
                    .aggregation(AggregationBuilders.terms("by_product_code").field("product_code.keyword").size(2000) //按货号分组，最多查500个货号.SKU直接改字段名字就可以
                            .subAggregation(AggregationBuilders.sum("quantity").field("quantity"))  //分组计算销量汇总
                            .subAggregation(AggregationBuilders.sum("amount").field("amount"))  //分组计算实付款汇总，需要加其他汇总的在这里依次加
                            .subAggregation(PipelineAggregatorBuilders.bucketSelector("sales_bucket_filter", script, "quantity"))//查询是否大于指定值
                            .order(BucketOrder.aggregation("amount", false)));

            searchRequest.source(searchSourceBuilder);
            SearchResponse sr = client.search(searchRequest);


            Terms terms = sr.getAggregations().get("by_product_code");   //查询遍历第一个根据货号分组的aggregation

            System.out.println(terms.getBuckets().size());
            for (Terms.Bucket entry : terms.getBuckets()) {
                Map<String, Object> objectMap = new HashMap<>();
                System.out.println("------------------");
                System.out.println("【 " + entry.getKey() + " 】订单数 : " + entry.getDocCount());

                Sum sum0 = entry.getAggregations().get("quantity"); //取得销量的汇总
                Sum sum1 = entry.getAggregations().get("amount"); //取得销量的汇总

                objectMap.put("product_code", entry.getKey());
                objectMap.put("quantity", sum0.getValue());
                objectMap.put("amount", sum1.getValue());
                list.add(objectMap);
            }

            long endTime = System.currentTimeMillis();
            System.out.println("查询耗时" + (endTime - beginTime) + "毫秒");
            return list;
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }


}
