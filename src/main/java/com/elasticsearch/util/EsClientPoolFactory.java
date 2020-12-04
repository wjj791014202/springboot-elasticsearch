package com.elasticsearch.util;

import com.elasticsearch.config.ElasticSearchConfig;
import org.apache.commons.pool2.PooledObject;
import org.apache.commons.pool2.PooledObjectFactory;
import org.apache.commons.pool2.impl.DefaultPooledObject;
import org.apache.http.HttpHost;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;

/**
 * @ Author: Chengyanan
 * @ Date: 2019/7/17 18: 47
 * @ Description :EliasticSearch连接池工厂对象
 */
public class EsClientPoolFactory implements PooledObjectFactory<RestHighLevelClient> {



    @Override
    public void activateObject(PooledObject<RestHighLevelClient> arg0) throws Exception {
        //System.out.println("激活客户端");
    }

    /**
     * 销毁对象
     */
    @Override
    public void destroyObject(PooledObject<RestHighLevelClient> pooledObject) throws Exception {
        RestHighLevelClient highLevelClient = pooledObject.getObject();
        highLevelClient.close();
    }

    /**
     * 生产对象
     */
//  @SuppressWarnings({ "resource" })
    @Override
    public PooledObject<RestHighLevelClient> makeObject() throws Exception {
//      Settings settings = Settings.builder().put("cluster.name","elasticsearch").build();
        RestHighLevelClient client = null;
        try {
            /*client = new PreBuiltTransportClient(settings)
                    .addTransportAddress(new TransportAddress(InetAddress.getByName("localhost"),9300));*/
            client = new RestHighLevelClient(RestClient.builder(
                    new HttpHost(ElasticSearchConfig.instance.host, ElasticSearchConfig.instance.port, "http")
//                    new HttpHost(es_host2, es_port, "http"),
//                    new HttpHost(es_host3, es_port, "http")
            ));

        } catch (Exception e) {
            e.printStackTrace();
        }
        return new DefaultPooledObject<RestHighLevelClient>(client);
    }

    @Override
    public void passivateObject(PooledObject<RestHighLevelClient> arg0) throws Exception {
       // System.out.println("释放客户端");
    }

    @Override
    public boolean validateObject(PooledObject<RestHighLevelClient> arg0) {
        return true;
    }

}
