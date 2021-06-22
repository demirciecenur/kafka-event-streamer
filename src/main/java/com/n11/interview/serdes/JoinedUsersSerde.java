package com.n11.interview.serdes;


import com.fasterxml.jackson.databind.ObjectMapper;
import com.n11.interview.model.GroupedUser;
import com.n11.interview.model.JoinedUsers;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

import java.io.IOException;
import java.util.Map;

public class JoinedUsersSerde implements Serializer<JoinedUsers>, Deserializer<JoinedUsers>, Serde<JoinedUsers> {
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    @Override
    public void configure(final Map<String, ?> configs, final boolean isKey) {
    }

    @SuppressWarnings("unchecked")
    @Override
    public JoinedUsers deserialize(final String topic, final byte[] data) {
        if (data == null) {
            return null;
        }

        try {
            return OBJECT_MAPPER.readValue(data, JoinedUsers.class);
        } catch (final IOException e) {
            throw new SerializationException(e);
        }
    }

    @Override
    public byte[] serialize(final String topic, final JoinedUsers data) {
        if (data == null) {
            return null;
        }

        try {
            return OBJECT_MAPPER.writeValueAsBytes(data);
        } catch (final Exception e) {
            throw new SerializationException("Error serializing JSON message", e);
        }
    }

    @Override
    public void close() {
    }

    @Override
    public Serializer<JoinedUsers> serializer() {
        return this;
    }

    @Override
    public Deserializer<JoinedUsers> deserializer() {
        return this;
    }
}