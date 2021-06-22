package com.n11.interview;

import com.n11.interview.model.Event;
import com.n11.interview.model.GroupedUser;
import com.n11.interview.model.JoinedUsers;
import com.n11.interview.serdes.EventSerde;
import com.n11.interview.serdes.GroupedUserSerde;
import com.n11.interview.serdes.PriorityQueueSerde;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.KeyValueStore;

import java.io.FileInputStream;
import java.io.IOException;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.stream.Collectors;

public final class N11Analytics {
    public static final String INPUT_TOPIC = "test";
    public static final String OUTPUT_TOPIC_1 = "test-output-1";
    public static final String OUTPUT_TOPIC_2 = "test-output-2";
    public static final String OUTPUT_TOPIC_3 = "test-output-3";
    public static final String OUTPUT_TOPIC_4 = "test-output-4";

    static Properties getStreamsConfig(final String[] args) throws IOException {
        final Properties props = new Properties();
        if (args != null && args.length > 0) {
            try (final FileInputStream fis = new FileInputStream(args[0])) {
                props.load(fis);
            }
            if (args.length > 1) {
                System.out.println("Warning: Some command line arguments were ignored. This demo only accepts an optional configuration file.");
            }
        }
        props.putIfAbsent(StreamsConfig.APPLICATION_ID_CONFIG, "streams-n11");
        props.putIfAbsent(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.putIfAbsent(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, EventSerde.class);
        props.put(StreamsConfig.DEFAULT_WINDOWED_KEY_SERDE_INNER_CLASS, EventSerde.class);
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, EventSerde.class);
        props.put(StreamsConfig.DEFAULT_WINDOWED_VALUE_SERDE_INNER_CLASS, EventSerde.class);

        props.putIfAbsent(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        return props;
    }

    static void calculateProductViewsInWindow(final KStream<String, Event> source, Duration duration) {
        final KTable<Windowed<String>, Long> productViews = source
                .selectKey(((key, value) -> value.getEventName()))
                .filter((key, value) -> "productView".equals(key))
                .groupByKey(Grouped.with(Serdes.String(), new EventSerde()))
                .windowedBy(TimeWindows.of(duration))
                .count();

        final KStream<String, Long> productViewsForConsole = productViews
                .toStream()
                .filter((windowedEventName, count) -> count != null)
                .map((windowedEventName, count) ->
                        new KeyValue<>(windowedEventName.window().startTime().toString() + " - " + windowedEventName.window().endTime().toString(), count)
                );

        productViewsForConsole.to(OUTPUT_TOPIC_1, Produced.with(Serdes.String(), Serdes.Long()));
    }

    static void calculateUsersInWindow(final KStream<String, Event> source, Duration duration) {
        final KTable<Windowed<String>, Long> counted = source
                .map((key, value) -> new KeyValue<>(value.getUserId(), value))
                .groupByKey(Grouped.with(Serdes.String(), new EventSerde()))
                .windowedBy(TimeWindows.of(duration))
                .count();

        final KStream<Windowed<String>, Long> filtered = counted
                .toStream()
                .filter((key, value) -> value == 1);

        final KTable<Long, Long> users = filtered
                .groupBy((key, value) -> key.window().start(), Serialized.with(Serdes.Long(), Serdes.Long()))
                .count();

        final KStream<String, String> output = users
                .toStream()
                .filter((key, value) -> value != null)
                .map((key, value) -> new KeyValue<>(key.toString(), String.valueOf(value)));

        output.to(OUTPUT_TOPIC_2, Produced.with(Serdes.String(), Serdes.String()));
    }

    static void calculateGMV(final KStream<String, Event> source) {
        final KTable<String, Long> orders = source
                .selectKey(((key, value) -> value.getEventName()))
                .filter((key, value) -> "order".equals(key) && "success".equals(value.getOrderStatus()))
                .map((key, value) -> new KeyValue<>(key, value.getAmount()))
                .groupByKey(Grouped.with(Serdes.String(), Serdes.Integer()))
                .aggregate(() -> 0L, (key, value, agg) -> (Long) (value + agg), Materialized.<String, Long, KeyValueStore<Bytes, byte[]>>as("gmv")
                        .withKeySerde(Serdes.String())
                        .withValueSerde(Serdes.Long()));
        final KStream<String, String> output = orders
                .toStream()
                .map(((key, value) -> new KeyValue<String, String>(key, String.valueOf(value))));
        output.to(OUTPUT_TOPIC_3, Produced.with(Serdes.String(), Serdes.String()));
    }

    static void calculateTrendingTimeSlot(final KStream<String, Event> source, Duration entireDuration, Duration slotDuration, int topN) {
        final Comparator<GroupedUser> comparator =
                (o1, o2) -> (int) (o2.getCount() - o1.getCount());
        final KTable<Long, PriorityQueue<GroupedUser>> aggregated = source
                .map((key, value) -> new KeyValue<>(value.getUserId(), value))
                .groupByKey(Grouped.with(Serdes.String(), new EventSerde()))
                .windowedBy(TimeWindows.of(entireDuration))
                .count()
                .groupBy(
                        ((key, count) -> new KeyValue<>(key.window().start(), new GroupedUser(key.key(), count, key.window().start(), key.window().end()))),
                        Grouped.with(Serdes.Long(), new GroupedUserSerde())
                )
                .aggregate(
                        () -> new PriorityQueue<GroupedUser>(comparator),
                        (startTime, groupedUser, queue) -> {
                            queue.add(groupedUser);
                            return queue;
                        },
                        (startTime, groupedUser, queue) -> {
                            queue.remove(groupedUser);
                            return queue;
                        },

                        Materialized.with(Serdes.Long(), new PriorityQueueSerde<>(comparator, new GroupedUserSerde()))
                );
        final KTable<Long, List<GroupedUser>> topUsersTable = aggregated
                .mapValues(queue -> {
                    final List<GroupedUser> topUsers = new ArrayList<>();
                    for (int i = 0; i < topN; i++) {
                        final GroupedUser record = queue.poll();
                        if (record == null) {
                            break;
                        }
                        topUsers.add(record);
                    }
                    return topUsers;
                });

        KTable<String, GroupedUser> leftTable = topUsersTable
                .toStream()
                .filter((key, value) -> value != null)
                .flatMap((key, value) -> value.stream().map(topUser -> new KeyValue<String, GroupedUser>(topUser.getUserId(), topUser))
                        .collect(Collectors.toList()))
                .toTable(Materialized.<String, GroupedUser, KeyValueStore<Bytes, byte[]>>as("left-table")
                        .withKeySerde(Serdes.String())
                        .withValueSerde(new GroupedUserSerde()));

        final KTable<String, GroupedUser> rightTable = source
                .selectKey((key, value) -> value.getUserId())
                .groupByKey(Grouped.with(Serdes.String(), new EventSerde()))
                .windowedBy(TimeWindows.of(slotDuration))
                .count()
                .toStream()
                .map((key, value) -> new KeyValue<>(key.key(),new GroupedUser(key.key(), value, key.window().start(), key.window().end()) ) )
                .toTable(Materialized.with(Serdes.String(), new GroupedUserSerde()));

        final KTable<String, JoinedUsers> joinedUsersTable = leftTable.join(rightTable,
                (leftValue, rightValue) -> new JoinedUsers(leftValue, rightValue)
        ).filter((key, joinedUsers) -> joinedUsers != null && joinedUsers.getUser().getStarted() >= joinedUsers.getTopUser().getStarted() && joinedUsers.getUser().getEnded() <= joinedUsers.getTopUser().getEnded());

        joinedUsersTable.toStream().foreach((key, value) -> System.out.println("Joined : " + key + " - " + (value != null ? value.getUser().getStarted() + " - " + value.getUser().getCount() : "null")));

        final KTable<Long, Long> aggregatedTrendHours = joinedUsersTable
                .toStream()
                .filter((key, value) -> value != null)
                .map((key, value) -> new KeyValue<>(value.getUser().getStarted(), value.getUser()))
                .groupByKey(Grouped.with(Serdes.Long(), new GroupedUserSerde()))
                .aggregate(() -> 0L,
                        (key, value, agg) -> value.getCount() >= agg ? value.getCount() : agg,
                        Materialized.with(Serdes.Long(), Serdes.Long()));

        final KTable<Long, Long> trendTimeTable = aggregatedTrendHours.groupBy(
                ((key, value) -> new KeyValue<>(key, value)),
                Grouped.with(Serdes.Long(), Serdes.Long())
        )
                .aggregate(
                        () -> new PriorityQueue<Long>((o1, o2) -> (int) (o2 - o1)),
                        (startTime, count, queue) -> {
                            queue.add(count);
                            return queue;
                        },
                        (startTime, count, queue) -> {
                            queue.remove(count);
                            return queue;
                        },

                        Materialized.with(Serdes.Long(), new PriorityQueueSerde<>((o1, o2) -> (int) (o2 - o1), Serdes.Long()))
                )
                .mapValues(PriorityQueue::poll)
                .filter((key, value) -> value != null);

        trendTimeTable.toStream().foreach((key, value) -> System.out.println("Trend time : " + key + " : " + value));
        trendTimeTable.toStream().map((key, value) -> new KeyValue<>(String.valueOf(key), String.valueOf(value))).to(OUTPUT_TOPIC_4, Produced.with(Serdes.String(), Serdes.String()));
    }

    public static void main(final String[] args) throws IOException {
        final Properties props = getStreamsConfig(args);

        final StreamsBuilder builder = new StreamsBuilder();
        final KStream<String, Event> source = builder.stream(INPUT_TOPIC, Consumed.with(Serdes.String(), new EventSerde()));
        calculateProductViewsInWindow(source, Duration.ofSeconds(5));
        calculateUsersInWindow(source, Duration.ofSeconds(10));
        calculateGMV(source);
        calculateTrendingTimeSlot(source, Duration.ofDays(1), Duration.ofHours(1), 10);
        final KafkaStreams streams = new KafkaStreams(builder.build(), props);
        final CountDownLatch latch = new CountDownLatch(1);

        Runtime.getRuntime().addShutdownHook(new Thread("streams-n11-shutdown-hook") {
            @Override
            public void run() {
                streams.close();
                latch.countDown();
            }
        });

        try {
            streams.start();
            latch.await();
        } catch (final Throwable e) {
            System.exit(1);
        }
        System.exit(0);
    }
}