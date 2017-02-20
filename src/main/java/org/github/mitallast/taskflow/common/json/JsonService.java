package org.github.mitallast.taskflow.common.json;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.PropertyAccessor;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.*;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.datatype.guava.GuavaModule;
import com.fasterxml.jackson.datatype.joda.JodaModule;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigRenderOptions;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufInputStream;
import io.netty.buffer.ByteBufOutputStream;
import org.github.mitallast.taskflow.common.component.AbstractComponent;
import org.github.mitallast.taskflow.common.error.Errors;
import org.github.mitallast.taskflow.operation.Operation;
import org.github.mitallast.taskflow.operation.OperationEnvironment;

import java.io.IOError;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Map;

public class JsonService extends AbstractComponent {

    private final ObjectMapper mapper;

    @Inject
    public JsonService(Config config) {
        super(config, JsonService.class);

        SimpleModule module = new SimpleModule();
        module.addSerializer(Config.class, new ConfigSerializer());
        module.addDeserializer(Config.class, new ConfigDeserializer());
        module.addSerializer(OperationEnvironment.class, new OperationEnvironmentSerializer());
        module.addDeserializer(OperationEnvironment.class, new OperationEnvironmentDeserializer());
        module.addSerializer(Errors.class, new ErrorsSerializer());
        module.addSerializer(Operation.class, new OperationSerializer());

        mapper = new ObjectMapper();
        mapper.setVisibility(PropertyAccessor.FIELD, JsonAutoDetect.Visibility.ANY);
        mapper.registerModule(module);
        mapper.registerModule(new GuavaModule());
        mapper.registerModule(new JodaModule());
    }

    public void serialize(ByteBuf buf, Object json) {
        try (OutputStream out = new ByteBufOutputStream(buf)) {
            serialize(out, json);
        } catch (IOException e) {
            throw new IOError(e);
        }
    }

    public String serialize(Object json) {
        try {
            return mapper.writeValueAsString(json);
        } catch (JsonProcessingException e) {
            throw new IOError(e);
        }
    }

    public void serialize(OutputStream out, Object json) {
        try {
            mapper.writeValue(out, json);
        } catch (IOException e) {
            throw new IOError(e);
        }
    }

    public <T> T deserialize(String data, Class<T> type) {
        return deserialize(data, type);
    }

    public <T> T deserialize(ByteBuf buf, Class<T> type) {
        try (InputStream input = new ByteBufInputStream(buf)) {
            return deserialize(input, type);
        } catch (IOException e) {
            throw new IOError(e);
        }
    }

    public <T> T deserialize(InputStream input, Class<T> type) {
        try {
            return mapper.readValue(input, type);
        } catch (IOException e) {
            throw new IOError(e);
        }
    }

    public <T> T deserialize(String data, TypeReference<T> type) {
        try {
            return mapper.readValue(data, type);
        } catch (IOException e) {
            throw new IOError(e);
        }
    }

    public <T> T deserialize(ByteBuf buf, TypeReference<T> type) {
        try (InputStream input = new ByteBufInputStream(buf)) {
            return deserialize(input, type);
        } catch (IOException e) {
            throw new IOError(e);
        }
    }

    public <T> T deserialize(InputStream input, TypeReference<T> type) {
        try {
            return mapper.readValue(input, type);
        } catch (IOException e) {
            throw new IOError(e);
        }
    }

    private static class ConfigSerializer extends JsonSerializer<Config> {

        @Override
        public void serialize(Config value, JsonGenerator gen, SerializerProvider serializers) throws IOException {
            String render = value.root().render(ConfigRenderOptions.concise());
            gen.writeRawValue(render);
        }
    }

    private static class ConfigDeserializer extends JsonDeserializer<Config> {

        @Override
        public Config deserialize(JsonParser p, DeserializationContext ctxt) throws IOException {
            String json = p.readValueAsTree().toString();
            return ConfigFactory.parseString(json);
        }
    }

    private static class OperationEnvironmentSerializer extends JsonSerializer<OperationEnvironment> {
        @Override
        public void serialize(OperationEnvironment value, JsonGenerator gen, SerializerProvider serializers) throws IOException {
            gen.writeStartObject();
            for (Map.Entry<String, String> entry : value.map().entrySet()) {
                gen.writeStringField(entry.getKey(), entry.getValue());
            }
            gen.writeEndObject();
        }
    }

    private static class OperationEnvironmentDeserializer extends JsonDeserializer<OperationEnvironment> {

        @Override
        public OperationEnvironment deserialize(JsonParser p, DeserializationContext ctx) throws IOException {
            ImmutableMap<String, String> map = p.readValueAs(new TypeReference<ImmutableMap<String, String>>() {
            });
            return new OperationEnvironment(map);
        }
    }

    private static class ErrorsSerializer extends JsonSerializer<Errors> {

        @Override
        public void serialize(Errors value, JsonGenerator gen, SerializerProvider serializers) throws IOException {
            if (value.valid()) {
                gen.writeNull();
            } else {
                gen.writeStartObject();
                if (!value.errors().isEmpty()) {
                    for (Map.Entry<String, String> entry : value.errors().entrySet()) {
                        gen.writeStringField(entry.getKey(), entry.getValue());
                    }
                }
                if (!value.nested().isEmpty()) {
                    for (Map.Entry<String, Errors> entry : value.nested().entrySet()) {
                        if (!entry.getValue().valid()) {
                            gen.writeFieldName(entry.getKey());
                            serialize(entry.getValue(), gen, serializers);
                        }
                    }
                }
                gen.writeEndObject();
            }
        }
    }

    private static class OperationSerializer extends JsonSerializer<Operation> {

        @Override
        public void serialize(Operation value, JsonGenerator gen, SerializerProvider serializers) throws IOException {
            gen.writeStartObject();
            gen.writeStringField("id", value.id());
            gen.writeFieldName("reference");
            gen.writeObject(value.reference());
            gen.writeEndObject();
        }
    }
}
