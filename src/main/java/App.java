import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Collections;
import java.util.Properties;

public class App {

    public static void main(String[]args){
        Thread thread=new Thread(new Producer());
        thread.start();
        Properties props = new Properties();
        props.put("bootstrap.servers", "nominatim.aerogpstrack.com:9092");
        props.put("group.id", "CountryCounter");
        props.put("key.deserializer",
                "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer","org.apache.kafka.common.serialization.StringDeserializer");
        //props.put("value.deserializer", GsonDeserializer.class.getName());
        //props.put(GsonDeserializer.CONFIG_VALUE_CLASS, Position.class.getName());

        KafkaConsumer<String, String> consumer =
                new KafkaConsumer<>(props);

        consumer.subscribe(Collections.singletonList("quickstart-events"));
        Gson gson=new GsonBuilder().excludeFieldsWithoutExposeAnnotation().create();
        try {
            long totalReceived=0;
            long lastCount=System.currentTimeMillis();
            while (true) {
                try {
                    ConsumerRecords<String, String> records = consumer.poll(100);
                    for (ConsumerRecord<String, String> record : records) {
                        //log.debug("topic = %s, partition = %d, offset = %d,"customer = %s, country = %s\n",record.topic(), record.partition(), record.offset(),record.key(), record.value());
//                        try {
//                            System.out.println(record.value());
//                        } catch (Exception ex) {
//                            ex.printStackTrace();
//                        }
                        totalReceived++;
                        if ((totalReceived % 1000) == 0) {
                            Position pos=gson.fromJson(record.value(),Position.class);
                            System.out.println(pos.address);
                            long duration = System.currentTimeMillis() - lastCount;
                            System.out.println("total recived:" + totalReceived + ", In:" + duration + " Milisecond");
                            lastCount = System.currentTimeMillis();
                        }
                        // System.out.println(record);
                    }
                }catch (Exception ex){
                    ex.printStackTrace();
                }
            }
        } finally {
            consumer.close();
        }
    }
}
