package com.es;

import java.net.InetAddress;

import com.google.gson.JsonObject;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.transport.client.PreBuiltTransportClient;


public class ESTest {

    public TransportClient getClient() {
        TransportClient client = null;
        try {
            Settings settings = Settings.builder().put("cluster.name", "Edison-ES").build();
            String host = "localhost";
            int port = 9300;
            client = new PreBuiltTransportClient(settings)
                    .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName(host), port));
        } catch (Exception e) {
            e.printStackTrace();
        }
        return client;
    }

    public void close(TransportClient client) {
        if (null != client) {
            client.close();
        }
    }

    /**
     * 添加索引
     */
    public void addIndex() {
        TransportClient client = getClient();
        if (null == client) {
            return;
        }
        JsonObject jsonObject = new JsonObject();
        jsonObject.addProperty("name", "java编程思想");
        jsonObject.addProperty("publishDate", "20200808");
        jsonObject.addProperty("price", 100);
        IndexResponse indexResponse = client.prepareIndex("book", "java", "1")
                .setSource(jsonObject.toString(), XContentType.JSON).get();
        System.out.println("索引名称：" + indexResponse.getIndex());
        System.out.println("类型：" + indexResponse.getType());
        System.out.println("文档id：" + indexResponse.getId());
        System.out.println("当前实例状态：" + indexResponse.status());
        close(client);
    }

    /**
     * 根据id查询
     */
    public void getById() {
        TransportClient client = getClient();
        if (null == client) {
            return;
        }
        GetResponse getResponse = client.prepareGet("book", "java", "1").get();
        System.out.println(getResponse.getSourceAsString());
        client.close();
    }

    /**
     * 根据id修改文档
     */
    public void updateById() {
        TransportClient client = getClient();
        if (null == client) {
            return;
        }
        JsonObject jsonObject = new JsonObject();
        jsonObject.addProperty("name", "java编程思想");
        jsonObject.addProperty("publishDate", "20200606");
        jsonObject.addProperty("price", 120);
        UpdateResponse updateResponse = client.prepareUpdate("book", "java", "1").setDoc(jsonObject.toString(), XContentType.JSON).get();
        System.out.println("索引名称：" + updateResponse.getIndex());
        System.out.println("类型：" + updateResponse.getType());
        System.out.println("文档id：" + updateResponse.getId());
        System.out.println("当前实例状态：" + updateResponse.status());
        client.close();
    }

    /**
     * 根据id删除文档
     */
    public void deleteById() {
        TransportClient client = getClient();
        if (null == client) {
            return;
        }
        DeleteResponse deleteResponse = client.prepareDelete("book", "java", "1").get();
        System.out.println("索引名称：");
        System.out.println("类型：");
        System.out.println("文档ID：");
        System.out.println("当前实例状态：");
    }

    public static void main(String[] args) {
        ESTest esTest = new ESTest();
        esTest.addIndex();
        esTest.getById();
        esTest.updateById();
        esTest.getById();
        esTest.deleteById();
        esTest.getById();
    }
}
