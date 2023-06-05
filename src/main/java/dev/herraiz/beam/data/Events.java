/*
Copyright 2023 Google.
Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

	http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package dev.herraiz.beam.data;

import dev.herraiz.protos.Events.MyDummyEvent;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Random;
import org.apache.beam.sdk.values.TimestampedValue;
import org.joda.time.Instant;

public class Events {

    /**
     * Generate a list of messages
     *
     * @param numEvents Number of messages to generate
     * @param msgKey The key for the messages to be generated
     * @param testEpoch The oldest possible timestamp for the values.
     * @return A list of timestamped values, starting from the testEpoch
     */
    public static List<TimestampedValue<MyDummyEvent>> generateData(
            int numEvents, String msgKey, Instant testEpoch) {
        Random r = new Random();
        List<TimestampedValue<MyDummyEvent>> events = new ArrayList<>();

        for (int k = 0; k < numEvents; k++) {
            // Generate events shifted ~1 sec from each other
            Long shift = r.longs(100, 2999).findFirst().getAsLong();
            Long ts = testEpoch.plus(shift).plus(1000 * (k + 1)).getMillis();

            MyDummyEvent event =
                    MyDummyEvent.newBuilder()
                            .setMsgKey(msgKey)
                            .setValue(r.nextInt(10)) // Random value
                            .setEventTimestamp(ts)
                            .build();

            TimestampedValue<MyDummyEvent> tsval =
                    TimestampedValue.of(event, new org.joda.time.Instant(ts));

            events.add(tsval);
        }

        return events;
    }

    public static class MyDummyEventComparator implements Comparator<MyDummyEvent> {

        @Override
        public int compare(MyDummyEvent o1, MyDummyEvent o2) {
            Instant instant1 = Instant.ofEpochMilli(o1.getEventTimestamp());
            Instant instant2 = Instant.ofEpochMilli(o2.getEventTimestamp());
            return instant1.compareTo(instant2);
        }
    }
}
