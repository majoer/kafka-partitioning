package majoer.kafka_partitioning

import org.springframework.boot.fromApplication
import org.springframework.boot.with


fun main(args: Array<String>) {
	fromApplication<KafkaPartitioningApplication>().with(TestcontainersConfiguration::class).run(*args)
}
