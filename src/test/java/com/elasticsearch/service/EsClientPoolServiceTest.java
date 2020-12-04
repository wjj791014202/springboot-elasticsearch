package com.elasticsearch.service;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.serializer.SerializerFeature;
import com.elasticsearch.App;
import com.elasticsearch.Model.Es;
import com.elasticsearch.Model.Order;
import com.elasticsearch.util.ElasticSearchUtils;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Random;

/**
 * Created by baishuai on 2018/12/18
 */
@RunWith(SpringRunner.class)
@SpringBootTest(classes = App.class, webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
public class EsClientPoolServiceTest {


    private Order order;

    @Before
    public void setUp() throws Exception {
        Order order = new Order(1,12,"旗舰店",1,
                "trousers_01","BX001",1,299, new Date());
        this.order = order;
    }

    /**
     *
     * 插入操作
     *
     * */

    @Test
    public void testInsertById(){
        String jsonStr = JSON.toJSONString(order, SerializerFeature.WriteDateUseDateFormat);
        ElasticSearchUtils.indexForJson("search_index","search_index",order.getId()+"",jsonStr);
    }

    /**
     *
     * 更新操作
     *
     * */

//    @Test
    public void testUpdateById(){
        order.setAmount(299);
        String jsonStr = JSON.toJSONString(order, SerializerFeature.WriteDateUseDateFormat);
        ElasticSearchUtils.updateForJson("search_index","search_index",order.getId()+"",jsonStr);
    }

    /**
     *
     * 删除操作
     *
     * */

//    @Test
    public void testDeleteById(){
        ElasticSearchUtils.deleteForJson("search_index","search_index", order.getId()+"");
    }

    @Test
    public void testQuery(){
        List<Map<String, Object>> list = ElasticSearchUtils.queryListFromES("search_index","search_index",12,"旗舰店", "2018-12-01", "2020-12-31");
        System.out.println(JSON.toJSONString(list));
    }
}
