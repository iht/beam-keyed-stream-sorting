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

package dev.herraiz.utils;

import dev.herraiz.protos.Events.MyDummyEvent;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.TimestampedValue;
import org.joda.time.Instant;

public class TestUtils {
    /**
     * Estimate watermark with a heuristics similar to PubSub.
     *
     * <p>We need a backlog of events (maybe already acked) to estimate the watermark. Don't feed
     * late events to this method, as late events should not alter the watermark.
     *
     * @param events The backlog of all on time events to be processed.
     * @param alreadySeen The number of messages that have been already acked, in timestamp order.
     * @return An instant, which is an estimation of the watermark for this subscription.
     */
    public Instant estimateWatermark(
            List<TimestampedValue<KV<String, MyDummyEvent>>> events, Integer alreadySeen) {
        if (alreadySeen >= events.size()) alreadySeen = events.size() - 1;

        List<Instant> ts = new ArrayList<>();
        for (int k = 0; k < events.size(); k++) ts.add(events.get(k).getTimestamp());
        Collections.sort(ts);

        return ts.get(alreadySeen);
    }
}
