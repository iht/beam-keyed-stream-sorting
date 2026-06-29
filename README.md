# Dealing with Order in Streams Using Apache Beam

This repository contains code examples and companion resources for the Beam Summit 2023 presentation: **"Dealing with order in streams using Apache Beam"** by Israel Herraiz.

## 📌 Talk & Resources

* **Speaker:** Israel Herraiz
* **Beam Summit Session:** [Dealing with order in streams using Apache Beam](https://beamsummit.org/sessions/2023/dealing-with-order-in-streams-using-apache-beam/)
* **Video Recording:** [Watch on YouTube](https://www.youtube.com/watch?v=ES_P7GuxOxM)
* **Presentation Slides:** Available in this repository at [`docs/[BeamSummit2023] Dealing with order in streams using Apache Beam.pdf`](docs/%5BBeamSummit2023%5D%20Dealing%20with%20order%20in%20streams%20using%20Apache%20Beam.pdf)

---

## 💡 Purpose & Overview

In distributed stream processing, data frequently arrives out of order and late due to network latency, system retries, or distributed architecture. When business logic requires applying temporal computations (such as order-dependent session state or event sequences) across keyed streams, recovering event-time order is critical.

This project demonstrates four distinct pattern implementations in Apache Beam (Java SDK) to restore and handle event order on keyed streams.

---

## 🛠️ Sorting Strategies Implemented

Each transform in `src/main/java/dev/herraiz/beam/transform/` demonstrates a different architectural approach:

### 1. `SortWithWindows`
* **File:** [`SortWithWindows.java`](src/main/java/dev/herraiz/beam/transform/SortWithWindows.java)
* **Approach:** Uses session windowing (`Sessions.withGapDuration(...)`) combined with `GroupByKey`. Once all elements for a window are aggregated by key, an in-memory sort operation (`events.sort(...)`) is performed inside a `DoFn`.
* **Use Case:** Straightforward pattern suitable for moderate volume window sizes where in-memory buffering per key is acceptable.

### 2. `SortWithState`
* **File:** [`SortWithState.java`](src/main/java/dev/herraiz/beam/transform/SortWithState.java)
* **Approach:** Utilizes Apache Beam's State and Timer API (`OrderedListState`). Elements are buffered into state ordered by timestamp as they arrive. An event-time timer triggers outputting accumulated elements after a configured session gap.
* **Use Case:** Flexible stateful processing when windowing semantics don't align with custom session boundary or termination logic.

### 3. `SortWithMapState`
* **File:** [`SortWithMapState.java`](src/main/java/dev/herraiz/beam/transform/SortWithMapState.java)
* **Approach:** Leverages the `@RequiresTimeSortedInput` annotation alongside `MapState<Integer, MyDummyEvent>`. The annotation instructs the runner to buffer and feed elements to `@ProcessElement` in strictly sorted event-time order per key.
* **Use Case:** Efficient stateful processing where elements are processed in sequence without needing manual sorting in memory.

### 4. `SortWithAnnotations`
* **File:** [`SortWithAnnotations.java`](src/main/java/dev/herraiz/beam/transform/SortWithAnnotations.java)
* **Approach:** Combines `@RequiresTimeSortedInput` with standard `ValueState<List<MyDummyEvent>>`. Because elements are guaranteed to arrive ordered by timestamp, appending them sequentially to state preserves order.
* **Use Case:** Simplified state management leveraging runner-level timestamp ordering.

---

## 🚀 How to Use This Repository

### Companion Learning Workflow
1. **Watch the Talk:** Watch the [Beam Summit 2023 Video Presentation](https://www.youtube.com/watch?v=ES_P7GuxOxM).
2. **Review the Slides:** Open the presentation slides in [`docs/`](docs/%5BBeamSummit2023%5D%20Dealing%20with%20order%20in%20streams%20using%20Apache%20Beam.pdf) to follow the conceptual diagrams and performance considerations.
3. **Inspect the Code:** Compare the slide concepts with the Java transforms in `src/main/java/dev/herraiz/beam/transform/`.
4. **Run Unit Tests:** Observe how out-of-order streams are simulated and verified using Beam's `TestStream`.

### Prerequisites
* **Java Development Kit (JDK):** Version 25 or higher.
* **Gradle:** Included via wrapper (`./gradlew`).

### Running Tests
To compile the project and run all unit tests verifying the sorting strategies:
```bash
./gradlew test
```

Unit tests are located in [`SortingTests.java`](src/test/java/dev/herraiz/beam/transform/SortingTests.java). They use `TestStream` to generate disordered test data and verify that each transform correctly outputs ordered streams.
