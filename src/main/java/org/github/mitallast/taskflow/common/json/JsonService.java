package org.github.mitallast.taskflow.common.json;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.PropertyAccessor;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.*;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigRenderOptions;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufOutputStream;
import org.github.mitallast.taskflow.common.component.AbstractComponent;
import org.github.mitallast.taskflow.operation.OperationEnvironment;

import java.io.IOError;
import java.io.IOException;
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

        mapper = new ObjectMapper();
        mapper.setVisibility(PropertyAccessor.FIELD, JsonAutoDetect.Visibility.ANY);
        mapper.registerModule(module);
    }

    public void serialize(ByteBuf buf, Object json) {
        OutputStream out = new ByteBufOutputStream(buf);
        try {
            mapper.writeValue(out, json);
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
        public Config deserialize(JsonParser p, DeserializationContext ctxt) throws IOException, JsonProcessingException {
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
            if (p.nextToken() == JsonToken.START_OBJECT) {
                ImmutableMap.Builder<String, String> builder = ImmutableMap.builder();
                while (p.nextToken() != JsonToken.END_OBJECT) {
                    if (p.currentToken() == JsonToken.FIELD_NAME) {
                        String key = p.getCurrentName();
                        if (p.nextToken() == JsonToken.VALUE_STRING) {
                            String value = p.getText();
                            builder.put(key, value);
                        } else {
                            throw new IllegalArgumentException("expected value string");
                        }
                    } else {
                        throw new IllegalArgumentException("expected field name");
                    }
                }
                return new OperationEnvironment(builder.build());
            } else {
                throw new IllegalArgumentException("expected start object");
            }
        }
    }
}
