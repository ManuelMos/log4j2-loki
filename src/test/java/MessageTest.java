import com.mashape.unirest.http.HttpResponse;
import com.mashape.unirest.http.Unirest;
import com.mashape.unirest.http.exceptions.UnirestException;
import org.junit.Assert;
import org.junit.Test;

import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;

public class MessageTest {

    @Test
    public void testPost (){
        DateTimeFormatter formatter = new DateTimeFormatterBuilder().appendInstant(3).toFormatter();
        OffsetDateTime utc = OffsetDateTime.now(ZoneOffset.UTC);
        try {
            HttpResponse<String> response = Unirest.post( "http://localhost:3100/api/prom/push")
                    .header("Content-Type", "application/json")
                    .header("Accept", "*/*")
                    .body("{\"streams\": " +
                            "[{" +
                            "\"labels\": \"{"+ "foooo=\\\"bar\\\"" +"}\"," +
                            "\"entries\": [{ \"ts\": \"" +
                            formatter.format(utc) +
                            "\", " +
                            "\"line\": " +
                            "\"level="  + "test" +
                            " message=" + "yeah" +
                            "\" }]" +
                            "}]}")
                    .asString();

            if(response.getStatus() != 204){
                Assert.fail(response.getBody());
            }
        } catch (UnirestException e) {
            e.printStackTrace();
        }
    }
}
