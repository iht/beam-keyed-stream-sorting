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

package dev.herraiz.beam.transform;

import com.google.auto.value.AutoValue;
import dev.herraiz.protos.Events.MyDummyEvent;

import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import org.apache.beam.sdk.extensions.protobuf.ProtoCoder;
import org.apache.beam.sdk.state.*;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.Max;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TimestampedValue;
import org.joda.time.Duration;
import org.joda.time.Instant;

public class SortWithState {

    @AutoValue
    public abstract static class Transform
            extends PTransform<
                    PCollection<KV<String, MyDummyEvent>>,
                    PCollection<KV<String, Iterable<MyDummyEvent>>>> {

        public abstract int sessionGap();

        public static Transform withSessionDuration(int duration) {
            return new AutoValue_SortWithState_Transform.Builder().sessionGap(duration).build();
        }

        @AutoValue.Builder
        public abstract static class Builder {
            public abstract Transform.Builder sessionGap(int d);

            public abstract Transform build();
        }

        @Override
        public PCollection<KV<String, Iterable<MyDummyEvent>>> expand(
                PCollection<KV<String, MyDummyEvent>> input) {
            return input.apply(
                    "Sort with state", ParDo.of(new RecoverSessionDoFn(this.sessionGap())));
        }
    }

    private static class RecoverSessionDoFn
            extends DoFn<KV<String, MyDummyEvent>, KV<String, Iterable<MyDummyEvent>>> {

        @StateId("holdingUpAfterLastMsg")
        private final StateSpec<ValueState<Boolean>> currentlyHoldingUpSpec = StateSpecs.value();

        @StateId("currentKey")
        private final StateSpec<ValueState<String>> currentKeySpec = StateSpecs.value();

        @StateId("elementsOrderedList")
        private final StateSpec<OrderedListState<MyDummyEvent>> eventsOrderedListSpec =
                StateSpecs.orderedList(ProtoCoder.of(MyDummyEvent.class));

        // The maximum element timestamp seen so far.
        @StateId("maxTimestampSeen")
        private final StateSpec<CombiningState<Long, long[], Long>> maxTimestampSpec =
                StateSpecs.combining(Max.ofLongs());

        @TimerId("gapTimer")
        private final TimerSpec gapTimerSpec = TimerSpecs.timer(TimeDomain.EVENT_TIME);

        private final int sessionGap;

        public RecoverSessionDoFn(int sessionGap) {
            this.sessionGap = sessionGap;
        }

        @ProcessElement
        public void processElement(
                @Element KV<String, MyDummyEvent> element,
                @Timestamp Instant elementTimestamp,
                @StateId("currentKey") ValueState<String> currentKeyState,
                @AlwaysFetched @StateId("holdingUpAfterLastMsg")
                        ValueState<Boolean> currentlyHoldingUpState,
                @StateId("elementsOrderedList") OrderedListState<MyDummyEvent> eventsListState,
                @StateId("maxTimestampSeen") CombiningState<Long, long[], Long> maxTimestampState,
                @TimerId("gapTimer") Timer gapTimer,
                OutputReceiver<KV<String, Iterable<MyDummyEvent>>> receiver) {
            // Update state
            currentKeyState.write(element.getKey());
            eventsListState.add(TimestampedValue.of(element.getValue(), elementTimestamp));
            maxTimestampState.add(elementTimestamp.getMillis());
            // Check if we have met the conditions to close the session
            boolean isLastMsg = element.getValue().getIsLastMsg();
            // Messages coming out of order after the last msg will not verify the condition,
            // so we need a state variable to remember that we have seen the last msg
            boolean currentlyHoldingUp =
                    Optional.ofNullable(currentlyHoldingUpState.read()).orElse(false);
            boolean sessionEndFound = currentlyHoldingUp || isLastMsg;

            if (sessionEndFound) {
                currentlyHoldingUpState.write(true);
                gapTimer.withOutputTimestamp(Instant.ofEpochMilli(maxTimestampState.read()))
                        .offset(Duration.standardSeconds(sessionGap))
                        .setRelative();
            }
        }

        @OnTimer("gapTimer")
        public void onGapTimer(
                @AlwaysFetched @StateId("currentKey") ValueState<String> currentKeyState,
                @StateId("holdingUpAfterLastMsg") ValueState<Boolean> currentlyHoldingUpState,
                @AlwaysFetched @StateId("elementsOrderedList")
                        OrderedListState<MyDummyEvent> orderedListState,
                @AlwaysFetched @StateId("maxTimestampSeen")
                        CombiningState<Long, long[], Long> maxTimestampState,
                OutputReceiver<KV<String, Iterable<MyDummyEvent>>> receiver) {

            String key = currentKeyState.read();
            List<MyDummyEvent> events =
                    StreamSupport.stream(orderedListState.read().spliterator(), false)
                            .map(ts -> ts.getValue())
                            .collect(Collectors.toList());

//            Collections.shuffle(events);

            receiver.outputWithTimestamp(
                    KV.of(key, events), Instant.ofEpochMilli(maxTimestampState.read()));

            currentKeyState.clear();
            currentlyHoldingUpState.clear();
            orderedListState.clear();
            maxTimestampState.clear();
        }
    }
}
