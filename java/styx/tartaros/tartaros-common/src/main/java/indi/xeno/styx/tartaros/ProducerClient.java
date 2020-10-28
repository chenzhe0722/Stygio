package indi.xeno.styx.tartaros;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.common.serialization.Serializer;

import java.util.Properties;

public class ProducerClient<K, V> {

  private final Producer<K, V> producer;

  private ProducerClient(
      Properties properties, Serializer<K> keySerializer, Serializer<V> valueSerializer) {
    producer = new KafkaProducer<>(properties, keySerializer, valueSerializer);
  }
}
