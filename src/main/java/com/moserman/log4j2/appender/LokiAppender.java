package com.moserman.log4j2.appender;

import com.mashape.unirest.http.HttpResponse;
import com.mashape.unirest.http.Unirest;
import com.mashape.unirest.http.exceptions.UnirestException;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.commons.text.StringEscapeUtils;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.core.Filter;
import org.apache.logging.log4j.core.Layout;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.appender.AbstractAppender;
import org.apache.logging.log4j.core.config.plugins.Plugin;
import org.apache.logging.log4j.core.config.plugins.PluginAttribute;
import org.apache.logging.log4j.core.config.plugins.PluginElement;
import org.apache.logging.log4j.core.config.plugins.PluginFactory;
import org.apache.logging.log4j.core.layout.PatternLayout;
import org.apache.logging.log4j.util.TriConsumer;

import java.io.Serializable;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.concurrent.ConcurrentLinkedQueue;

@Plugin(name = "Loki", category = "Core", elementType = "appender", printObject = true)
public class LokiAppender extends AbstractAppender {

    /** The labels. */
    private String labels;

    private DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSSSSS");

    /** The target host */
    private String target;

    /** hostName where logs are from. */
    private String hostName;

    /** The labels missing. */
    private boolean labelsMissing;

    /** The target missing. */
    private boolean targetHostMissing;


    /**
     * Filter out all org.apache.http Logs below Error level.
     * @param event the processed Event
     * @return isFiltered
     */
    @Override
    public boolean isFiltered(LogEvent event) {
        if (event.getLoggerName().startsWith("org.apache.http")
                && Level.ERROR.compareTo(event.getLevel()) < 0) {
            return true;
        }
        return super.isFiltered(event);
    }

    private LokiAppender(String name, Filter filter, Layout<? extends Serializable> layout,
                         boolean ignoreExceptions, String target, String labels) {
        super(name, filter, layout, ignoreExceptions);
        this.labels = labels;
        this.target = target;
        this.targetHostMissing = StringUtils.isBlank(target);
        this.labelsMissing = StringUtils.isBlank(labels);
        try {
            this.hostName = StringEscapeUtils.escapeJson(InetAddress.getLocalHost().getHostName());
        } catch (UnknownHostException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Creates the appender.
     *
     * @param name the name
     * @param ignoreExceptions the ignore exceptions
     * @param layout the layout
     * @param filter the filter
     * @param labels the labels
     * @return the elastic log appender
     */
    @PluginFactory
    public static LokiAppender createAppender(@PluginAttribute("name") String name,
                                                    @PluginAttribute("ignoreExceptions") boolean ignoreExceptions,
                                                    @PluginElement("Layout") Layout layout,
                                                    @PluginElement("Filters") Filter filter,
                                                    @PluginElement("target") String target,
                                                    @PluginAttribute("labels") String labels) {

        if (layout == null) {
            layout = PatternLayout.newBuilder().withPattern("%m").withAlwaysWriteExceptions(false).build();
        }

        return new LokiAppender(name, filter, layout, ignoreExceptions,StringEscapeUtils.escapeJson(target), StringEscapeUtils.escapeJson(labels));
    }



    public void append(LogEvent event) {
        if (labelsMissing) {
            throw new RuntimeException("labels setting missing");
        }

        if(targetHostMissing){
            throw new RuntimeException("targetHostMissing setting missing");
        }

        String message = StringEscapeUtils.escapeJson(new String(getLayout().toByteArray(event)));
        String stacktrace = "";
        if (event.getThrown() != null) {
            stacktrace = StringEscapeUtils.escapeJson(ExceptionUtils.getStackTrace(event.getThrown()));
        }

        StackTraceElement stacktraceElement = event.getSource();
        String className = "";
        if (stacktraceElement != null) {
            className = StringEscapeUtils.escapeJson(stacktraceElement.getClassName());
        }

        ZonedDateTime timestamp = Instant.ofEpochMilli(event.getTimeMillis()).atZone(ZoneOffset.UTC);

        try {
            HttpResponse<String> response = Unirest.post(target + "/api/prom/push")
                    .header("Content-Type", "application/json")
                    .header("Accept", "*/*")
                    .body("{\"streams\": " +
                            "[{" +
                                "\"labels\": \"{"+ labels +"}\"," +
                                "\"entries\": [{ \"ts\": \"" +
                                    timestamp.format(formatter) +
                            "\", " +
                                "\"line\": \"" +
                                  "level="  + event.getLevel().toString() +
                                  " host="  + hostName + " " +
                                  " logger=" + event.getLoggerName() +
                                  " class=" + className +
                                  " message=" + message +
                                  " additional= " + getAdditional(event) +
                                  " stacktrace=" + stacktrace +
                            "\" }]" +
                            "}]}")
                    .asString();
            if(response.getStatus() != 204){
                System.out.println("Could not send event to Loki: " + response.getStatus() + " " + response.getBody() +
                        "\r\n" +   "level="  + event.getLevel().toString() +
                        " host="  + hostName + " " +
                        " logger=" + event.getLoggerName() +
                        " class=" + className +
                        " message=" + message +
                        " additional= " + getAdditional(event) +
                        " stacktrace=" + stacktrace);
            }
        } catch (UnirestException e) {
            e.printStackTrace();
        }
    }
    private String getAdditional(LogEvent event) {
        ConcurrentLinkedQueue<String> additional = new ConcurrentLinkedQueue<>();
        event.getContextData().forEach(new TriConsumer<String, String, ConcurrentLinkedQueue<String>>() {

            @Override
            public void accept(String k, String v, ConcurrentLinkedQueue<String> s) {
                s.add("\"" + StringUtils.replaceChars(k, ".", "_") + "\":\"" + StringEscapeUtils.escapeJson(v) + "\"");
            }
        }, additional);
        return StringUtils.defaultString(StringUtils.join(additional.toArray(), ","));
    }

}
