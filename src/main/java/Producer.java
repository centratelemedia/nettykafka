import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Serializer;

import java.util.Properties;

public class Producer implements Runnable{
    @Override
    public void run() {
        Gson gson=new GsonBuilder().excludeFieldsWithoutExposeAnnotation().create();
        Properties props=new Properties();
        props.put("bootstrap.servers", "nominatim.aerogpstrack.com:9092");
        props.put("group.id", "CountryCounter");
        props.put("key.serializer","org.apache.kafka.common.serialization.StringSerializer");
        //props.put("value.serializer","org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", GsonSerializer.class.getName());
      //  org.apache.kafka.common.serialization.Serializer<K> keySerializer, Serializer<V> valueSerializer

        Position pos=new Position();
        pos.Id=1;
        pos.Nopol="L-3744-M";
        pos.address="Helo gaessdsd dsdksdsd Haha migrated from stackoverflow but needs to migrate to unix/linux Haha migrated from stackoverflow but needs to migrate to unix/linux";
        pos.Lat=-7.373733;
        pos.Lng=122.448494;

        KafkaProducer<String,Position> producer =new KafkaProducer<>(props);
        while (true) {
            ProducerRecord<String, Position> record = new ProducerRecord<>("quickstart-events", pos);
            producer.send(record);
            try {
                Thread.sleep(1);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
    }
}
