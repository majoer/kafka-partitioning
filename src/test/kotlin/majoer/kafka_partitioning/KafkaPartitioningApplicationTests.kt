package majoer.kafka_partitioning

import org.apache.kafka.clients.admin.AdminClient
import org.apache.kafka.clients.admin.NewTopic
import org.apache.kafka.clients.consumer.Consumer
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.ConsumerRecords
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.kafka.common.serialization.StringSerializer
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import org.junit.jupiter.api.fail
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.context.annotation.Import
import org.testcontainers.kafka.KafkaContainer
import java.time.Duration
import java.util.*
import java.util.concurrent.CompletableFuture


@Import(TestcontainersConfiguration::class)
@SpringBootTest
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class KafkaPartitioningApplicationTests {

    @Autowired
    lateinit var kafkaContainer: KafkaContainer

    lateinit var props: Map<String, Any>

    val iterations = 10

    @BeforeAll
    fun setup() {
        props = mapOf(
            ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG to "localhost:${kafkaContainer.firstMappedPort}",
            ConsumerConfig.GROUP_ID_CONFIG to UUID.randomUUID().toString(),
            ConsumerConfig.AUTO_OFFSET_RESET_CONFIG to "earliest",
            ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG to StringDeserializer::class.java.getName(),
            ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG to StringDeserializer::class.java.getName(),
            ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG to StringSerializer::class.java.getName(),
            ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG to StringSerializer::class.java.getName(),
        )
    }

    fun pollUntilFirstRecord(
        consumer: Consumer<String, String>,
        timeout: Duration = Duration.ofSeconds(2)
    ): ConsumerRecords<String?, String?>? {
        val start = System.currentTimeMillis()
        while (System.currentTimeMillis() - start < timeout.toMillis()) {
            val records = consumer.poll(Duration.ofMillis(100))
            if (!records.isEmpty) {
                return records
            }
        }
        return null
    }

    @Test
    fun kafkaAssignsPartitions_expectMessagesToArriveOnTheRandomlyAssignedClient() {
        AdminClient.create(props).use { adminClient ->
            val newTopics = mutableSetOf<NewTopic?>(
                NewTopic("request", 2, -1),
                NewTopic("response", 2, -1)
            )
            adminClient.createTopics(newTopics).all().get()
            println("Topics $newTopics created successfully!")
        }


        val responder = CompletableFuture.runAsync {
            KafkaProducer<String, String>(props).use { producer ->
                KafkaConsumer<String, String>(props).use { consumer ->
                    consumer.subscribe(Collections.singletonList("request"))

                    while (true) {
                        val records = pollUntilFirstRecord(consumer)
                        if (records == null) {
                            println("No records, exiting responder")
                            break
                        }
                        records.forEach { record ->
                            producer.send(ProducerRecord("response", record.key(), "pong")).get()
                        }
                    }
                }
            }
        }

        val instance0 = CompletableFuture.runAsync {
            KafkaProducer<String, String>(props).use { producer ->
                KafkaConsumer<String, String>(props).use { consumer ->
                    consumer.subscribe(Collections.singletonList("response"))

                    (1..iterations).forEach { i ->
                        producer.send(ProducerRecord("request", "instance-0", "ping")).get()

                        pollUntilFirstRecord(consumer)
                            ?.forEach { println("instance-0 got: ${it.key()}, assignment: ${consumer.assignment()}") }
                            ?: fail("Expected a response on instance-0 ${consumer.assignment()}")
                    }
                }
            }
        }


        val conssumer2 = CompletableFuture.runAsync {
            KafkaProducer<String, String>(props).use { producer ->
                KafkaConsumer<String, String>(props).use { consumer ->
                    consumer.subscribe(Collections.singletonList("response"))

                    (1..iterations).forEach {
                        producer.send(ProducerRecord("request", "instance-1", "ping")).get()

                        pollUntilFirstRecord(consumer)
                            ?.forEach { println("instance-1 got: ${it.key()}, assignment: ${consumer.assignment()}") }
                            ?: fail("Expected a response on instance-1 ${consumer.assignment()}")
                    }
                }
            }
        }

        CompletableFuture.allOf(instance0, conssumer2, responder).get()
    }

    @Test
    fun fixedAssignments_keyDeterminesTarget_hitWrongInstance() {
        AdminClient.create(props).use { adminClient ->
            val newTopics = mutableSetOf<NewTopic?>(
                NewTopic("request", 2, -1),
                NewTopic("response", 2, -1)
            )
            adminClient.createTopics(newTopics).all().get()
            println("Topics $newTopics created successfully!")
        }

        val responder = CompletableFuture.runAsync {
            KafkaProducer<String, String>(props).use { producer ->
                KafkaConsumer<String, String>(props).use { consumer ->
                    consumer.subscribe(Collections.singletonList("request"))
                    while (true) {
                        val records = pollUntilFirstRecord(consumer)
                        if (records == null) {
                            println("No records, exiting responder")
                            break
                        }
                        records.forEach { record ->
                            producer.send(ProducerRecord("response", record.key(), "pong")).get()
                        }
                    }
                }
            }
        }

        val instance0 = CompletableFuture.runAsync {
            KafkaProducer<String, String>(props).use { producer ->
                KafkaConsumer<String, String>(props).use { consumer ->
                    consumer.assign(listOf(TopicPartition("response", 1)))
                    (1..iterations).forEach { i ->
                        producer.send(ProducerRecord("request", "instance-0", "ping")).get()

                        pollUntilFirstRecord(consumer)
                            ?.forEach { println("instance-0 got: ${it.key()}, assignment: ${consumer.assignment()}") }
                            ?: fail("Expected a response on instance-0 ${consumer.assignment()}")

                    }
                }
            }
        }


        val instance1 = CompletableFuture.runAsync {
            KafkaProducer<String, String>(props).use { producer ->
                KafkaConsumer<String, String>(props).use { consumer ->
                    consumer.assign(listOf(TopicPartition("response", 0)))

                    (1..iterations).forEach {
                        producer.send(ProducerRecord("request", "instance-1", "ping")).get()

                        pollUntilFirstRecord(consumer)
                            ?.forEach { println("instance-1 got: ${it.key()}, assignment: ${consumer.assignment()}") }
                            ?: fail("Expected a response on instance-1 ${consumer.assignment()}")
                    }
                }
            }
        }

        CompletableFuture.allOf(instance0, instance1, responder).get()
    }

    @Test
    fun fixedAssignment_carefullyCraftedKey_canTargetInstance() {
        AdminClient.create(props).use { adminClient ->
            val newTopics = mutableSetOf<NewTopic?>(
                NewTopic("request", 2, -1),
                NewTopic("response", 2, -1)
            )
            adminClient.createTopics(newTopics).all().get()
            println("Topics $newTopics created successfully!")
        }

        val responder = CompletableFuture.runAsync {
            KafkaProducer<String, String>(props).use { producer ->
                KafkaConsumer<String, String>(props).use { consumer ->
                    consumer.subscribe(Collections.singletonList("request"))
                    while (true) {
                        val records = pollUntilFirstRecord(consumer)
                        if (records == null) {
                            println("No records, exiting responder")
                            break
                        }
                        records.forEach { record ->
                            producer.send(ProducerRecord("response", record.key(), "pong")).get()
                        }
                    }
                }
            }
        }

        val instance0 = CompletableFuture.runAsync {
            KafkaProducer<String, String>(props).use { producer ->
                KafkaConsumer<String, String>(props).use { consumer ->
                    consumer.assign(listOf(TopicPartition("response", 1)))
                    (1..iterations).forEach { i ->
                        producer.send(ProducerRecord("request", "alpha", "ping")).get()

                        pollUntilFirstRecord(consumer)
                            ?.forEach { println("instance-0 got: ${it.key()}, assignment: ${consumer.assignment()}") }
                            ?: fail("Expected a response on instance-0 ${consumer.assignment()}")

                    }
                }
            }
        }


        val conssumer2 = CompletableFuture.runAsync {
            KafkaProducer<String, String>(props).use { producer ->
                KafkaConsumer<String, String>(props).use { consumer ->
                    consumer.assign(listOf(TopicPartition("response", 0)))

                    (1..iterations).forEach {
                        producer.send(ProducerRecord("request", "omega", "ping")).get()

                        pollUntilFirstRecord(consumer)
                            ?.forEach { println("instance-1 got: ${it.key()}, assignment: ${consumer.assignment()}") }
                            ?: fail("Expected a response on instance-1 ${consumer.assignment()}")
                    }
                }
            }
        }

        CompletableFuture.allOf(instance0, conssumer2, responder).get()
    }

    @Test
    fun randomAssignments_sendAssignmentAsKey_targetAssignmentInResponder() {
        AdminClient.create(props).use { adminClient ->
            val newTopics = mutableSetOf<NewTopic?>(
                NewTopic("request", 2, -1),
                NewTopic("response", 2, -1)
            )
            adminClient.createTopics(newTopics).all().get()
            println("Topics $newTopics created successfully!")
        }

        val responder = CompletableFuture.runAsync {
            KafkaProducer<String, String>(props).use { producer ->
                KafkaConsumer<String, String>(props).use { consumer ->
                    consumer.subscribe(Collections.singletonList("request"))
                    while (true) {
                        val records = pollUntilFirstRecord(consumer)
                        if (records == null) {
                            println("No records, exiting responder")
                            break
                        }
                        records.forEach { record ->
                            producer.send(ProducerRecord("response", record.key()!!.toInt(), record.key(), "pong"))
                                .get()
                        }
                    }
                }
            }
        }

        val instance0 = CompletableFuture.runAsync {
            KafkaProducer<String, String>(props).use { producer ->
                KafkaConsumer<String, String>(props).use { consumer ->
                    consumer.subscribe(listOf("response"))

                    (1..iterations).forEach { i ->
                        producer.send(ProducerRecord("request", "${consumer.assignment().first().partition()}", "ping"))
                            .get()

                        pollUntilFirstRecord(consumer)
                            ?.forEach { println("instance-0 got: ${it.key()}, assignment: ${consumer.assignment()}") }
                            ?: fail("Expected a response on instance-0 ${consumer.assignment()}")

                    }
                }
            }
        }


        val instance1 = CompletableFuture.runAsync {
            KafkaProducer<String, String>(props).use { producer ->
                KafkaConsumer<String, String>(props).use { consumer ->
                    consumer.subscribe(listOf("response"))

                    (1..iterations).forEach {
                        producer.send(ProducerRecord("request", "${consumer.assignment().first().partition()}", "ping"))
                            .get()

                        pollUntilFirstRecord(consumer)
                            ?.forEach { println("instance-1 got: ${it.key()}, assignment: ${consumer.assignment()}") }
                            ?: fail("Expected a response on instance-1 ${consumer.assignment()}")
                    }
                }
            }
        }

        CompletableFuture.allOf(instance0, instance1, responder).get()
    }

    @Test
    fun uniqueConsumerGroup() {
        AdminClient.create(props).use { adminClient ->
            val newTopics = mutableSetOf<NewTopic?>(
                NewTopic("request", 2, -1),
                NewTopic("response", 2, -1)
            )
            adminClient.createTopics(newTopics).all().get()
            println("Topics $newTopics created successfully!")
        }

        val responder = CompletableFuture.runAsync {
            KafkaProducer<String, String>(props).use { producer ->
                KafkaConsumer<String, String>(props).use { consumer ->
                    consumer.subscribe(Collections.singletonList("request"))
                    while (true) {
                        val records = pollUntilFirstRecord(consumer)
                        if (records == null) {
                            println("No records, exiting responder")
                            break
                        }
                        records.forEach { record ->
                            producer.send(ProducerRecord("response", record.key(), "pong")).get()
                        }
                    }
                }
            }
        }

        val instance0Props = props + mapOf(
            ConsumerConfig.GROUP_ID_CONFIG to "instance0"
        )
        val instance0 = CompletableFuture.runAsync {
            KafkaProducer<String, String>(instance0Props).use { producer ->
                KafkaConsumer<String, String>(instance0Props).use { consumer ->
                    consumer.subscribe(listOf("response"))

                    (1..iterations).forEach { i ->
                        producer.send(ProducerRecord("request", "instance-0", "ping")).get()

                        pollUntilFirstRecord(consumer)
                            ?.forEach {
                                if (it.key() == "instance-0") {
                                    println("instance-0 got: ${it.key()}, assignment: ${consumer.assignment()}")
                                }
                            }
                            ?: fail("Expected a response on instance-0 ${consumer.assignment()}")

                    }
                }
            }
        }

        val instance1Props = props + mapOf(
            ConsumerConfig.GROUP_ID_CONFIG to "instance1"
        )
        val instance1 = CompletableFuture.runAsync {
            KafkaProducer<String, String>(instance1Props).use { producer ->
                KafkaConsumer<String, String>(instance1Props).use { consumer ->
                    consumer.subscribe(listOf("response"))

                    (1..iterations).forEach {
                        producer.send(ProducerRecord("request", "instance-1", "ping")).get()

                        pollUntilFirstRecord(consumer)
                            ?.forEach {
                                if (it.key() == "instance-1") {
                                    println("instance-1 got: ${it.key()}, assignment: ${consumer.assignment()}")
                                }
                            }
                            ?: fail("Expected a response on instance-1 ${consumer.assignment()}")
                    }
                }
            }
        }

        CompletableFuture.allOf(instance0, instance1, responder).get()
    }
}
