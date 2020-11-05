package io.milvus.client;

import com.google.protobuf.Descriptors;
import com.google.protobuf.MessageOrBuilder;
import com.google.protobuf.TextFormat;
import io.grpc.MethodDescriptor;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import org.slf4j.Logger;

public class LoggingAdapter {
  public static final LoggingAdapter DEFAULT_LOGGING_ADAPTER = new LoggingAdapter();
  private static final AtomicLong traceId = new AtomicLong(0);

  protected LoggingAdapter() {}

  protected String getTraceId() {
    return Long.toHexString(traceId.getAndIncrement());
  }

  protected void logRequest(
      Logger logger, String traceId, MethodDescriptor method, Object message) {
    if (logger.isTraceEnabled()) {
      logger.trace(
          "TraceId: {}, Method: {}, Request: {}",
          traceId,
          method.getFullMethodName(),
          trace(message));
    } else if (logger.isInfoEnabled()) {
      logger.info(
          "TraceId: {}, Method: {}, Request: {}",
          traceId,
          method.getFullMethodName(),
          info(message));
    }
  }

  protected void logResponse(
      Logger logger, String traceId, MethodDescriptor method, Object message) {
    if (logger.isTraceEnabled()) {
      logger.trace(
          "TraceId: {}, Method: {}, Response: {}",
          traceId,
          method.getFullMethodName(),
          trace(message));
    } else if (logger.isInfoEnabled()) {
      logger.info(
          "TraceId: {}, Method: {}, Response: {}",
          traceId,
          method.getFullMethodName(),
          info(message));
    }
  }

  protected String info(Object message) {
    if (message instanceof MessageOrBuilder) {
      MessageOrBuilder msg = (MessageOrBuilder) message;
      StringBuilder output = new StringBuilder(msg.getDescriptorForType().getName());
      write((MessageOrBuilder) message, output);
      return output.toString();
    }
    return message.toString();
  }

  protected String trace(Object message) {
    if (message instanceof MessageOrBuilder) {
      return TextFormat.printer().printToString((MessageOrBuilder) message);
    }
    return message.toString();
  }

  protected void write(MessageOrBuilder message, StringBuilder output) {
    output.append(" { ");
    message.getAllFields().entrySet().stream()
        .forEach(
            e -> {
              if (e.getKey().isRepeated()) {
                output
                    .append(e.getKey().getName())
                    .append(" [ ")
                    .append(((List<?>) e.getValue()).size())
                    .append(" items ], ");
              } else if (e.getKey().isMapField()) {
                output
                    .append(e.getKey().getName())
                    .append(" { ")
                    .append(((List<?>) e.getValue()).size())
                    .append(" entries }, ");
              } else if (e.getKey().getJavaType() == Descriptors.FieldDescriptor.JavaType.MESSAGE) {
                output.append(e.getKey().getName());
                write((MessageOrBuilder) e.getValue(), output);
              } else {
                output
                    .append(TextFormat.printer().shortDebugString(e.getKey(), e.getValue()))
                    .append(", ");
              }
            });
    output.setLength(output.length() - 2);
    output.append(" } ");
  }
}
