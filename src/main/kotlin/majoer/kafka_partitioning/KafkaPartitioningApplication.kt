package majoer.kafka_partitioning

import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.runApplication

@SpringBootApplication
class KafkaPartitioningApplication

fun main(args: Array<String>) {
	runApplication<KafkaPartitioningApplication>(*args)
}
