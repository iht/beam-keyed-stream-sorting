# Sample streaming pipeline

This repository contains a streaming Dataflow pipeline reading data from Pubsub and recovering the sessions from
potentially unordered data, by using a common key to all the points received for the same vehicle.

Then we apply both windowing and stateful DoFns, to show how we can recover sessions and to compare the performance of
both approaches.

## Data input

We are using here a public PubSub topic with data, so we don't need to setup our own to run this pipeline.

The topic is `projects/pubsub-public-data/topics/taxirides-realtime`.

That topic contains messages from the NYC Taxi Ride dataset. Here is a sample of the data contained in a message in
that topic:

```json
{
"ride_id": "328bec4b-0126-42d4-9381-cb1dbf0e2432",
"point_idx": 305,
"latitude": 40.776270000000004,
"longitude": -73.99111,
"timestamp": "2020-03-27T21:32:51.48098-04:00",
"meter_reading": 9.403651,
"meter_increment": 0.030831642,
"ride_status": "enroute",
"passenger_count": 1
}
```

But the messages also contain metadata, that is useful for streaming pipelines.  In this case, the messages contain an
attribute of name `ts`, which contains  the same timestamp as the field of name `timestamp` in the data. Remember that
PubSub treats the data as just a string of bytes (in topics with no schema), so it does not *know* anything about the
data itself. The metadata fields are normally used to publish messages with specific ids and/or timestamps.

To inspect the messages from this topic, you can create a subscription, and then  pull some messages.

To create a subscription, use the gcloud cli utility (installed by default in  the Cloud Shell):

```
export TOPIC=projects/pubsub-public-data/topics/taxirides-realtime
gcloud pubsub subscriptions create taxis --topic $TOPIC
```

To pull messages:

```gcloud pubsub subscriptions pull taxis --limit 3```

or if you have jq (for pretty printing of JSON)

```gcloud pubsub subscriptions pull taxis --limit 3 | grep " {" | cut -f 2 -d ' ' | jq```

## Run job

First set the environment variables by running

`source ./scripts/01_set_env_variables.sh`

and then run the job with

`./scripts/02_run_dataflow.sh`
