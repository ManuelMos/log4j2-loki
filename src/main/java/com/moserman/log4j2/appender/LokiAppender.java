package com.moserman.log4j2.appender;

import com.mashape.unirest.http.HttpResponse;
import com.mashape.unirest.http.Unirest;
import com.mashape.unirest.http.async.Callback;
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
import java.time.format.DateTimeFormatterBuilder;
import java.util.concurrent.ConcurrentLinkedQueue;

@Plugin(name = "Loki", category = "Core", elementType = "appender", printObject = true)
public class LokiAppender extends AbstractAppender {

    /** The labelValue. */
    private final String labelValue;
    /** The labelKey. */
    private String labelKey;

    /**  the DateTimeFormmater */
    private DateTimeFormatter formatter =  new DateTimeFormatterBuilder().appendInstant(3).toFormatter();

    /** The targetHost */
    private String targetHost;

    /** hostName where logs are from. */
    private String hostName;

    /** The labelKey missing. */
    private boolean labelKeyMissing;

    /** The labelvalue missing. */
    private boolean labelValueMissing;


    /** The targetHost missing. */
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
                         boolean ignoreExceptions, String target, String labelKey, String labelValue) {
        super(name, filter, layout, ignoreExceptions);
        this.labelKey = labelKey;
        this.labelValue = labelValue;
        this.targetHost = target;
        this.targetHostMissing = StringUtils.isBlank(target);
        this.labelKeyMissing = StringUtils.isBlank(labelKey);
        this.labelValueMissing = StringUtils.isBlank(labelValue);
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
     * @param labelKey the labelKey
     * @param labelValue the labelValue
     * @return the elastic log appender
     */
    @PluginFactory
    public static LokiAppender createAppender(@PluginAttribute("name") String name,
                                                    @PluginAttribute("ignoreExceptions") boolean ignoreExceptions,
                                                    @PluginElement("Layout") Layout layout,
                                                    @PluginElement("Filters") Filter filter,
                                                    @PluginAttribute("targetHost") String targetHost,
                                                    @PluginAttribute("labelKey") String labelKey,
                                                    @PluginAttribute("labelValue") String labelValue) {

        if (layout == null) {
            layout = PatternLayout.newBuilder().withPattern("%m").withAlwaysWriteExceptions(false).build();
        }

        return new LokiAppender(name, filter, layout, ignoreExceptions, targetHost, labelKey, labelValue);
    }



    public void append(LogEvent event) {
        if (labelKeyMissing) {
            throw new RuntimeException("labelKey setting missing");
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

        Unirest.post(targetHost + "/api/prom/push")
                    .header("Content-Type", "application/json")
                    .header("Accept", "*/*")
                    .body("{\"streams\": " +
                            "[{" +
                                "\"labels\": \"{"+ labelKey +"=\\\""+ labelValue+ "\\\"}\"," +
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
                    .asStringAsync(new Callback<String>() {
                        @Override
                        public void completed(HttpResponse<String> response) {

                        }

                        @Override
                        public void failed(UnirestException e) {
                            System.out.println("Could not send event to Loki: " + e.getCause() + " " + e.getMessage()+
                                    "\r\n" +   "level="  + event.getLevel().toString() +
                                    " host="  + hostName + " " +
                                    " logger=" + event.getLoggerName() +
                                    " message=" + message +
                                    " additional= " + getAdditional(event));
                        }

                        @Override
                        public void cancelled() {

                        }
        });

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
