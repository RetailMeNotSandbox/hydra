Hydra
==========

A reactive service that crawls through your data and provides a fully hydrated changefeed for all your data.

API
-------

All endpoints require the `key` header to be present, and the value of the header to be one of the values in the `hydra.apiKeys`
configuration.  Failure to meet the requirement will result in a `403 Forbidden` response.

    GET /resource/{resourceType}/{resourceId}

^ Returns the specified [resource](http://jsonapi.org/format/#document-resource-objects) as a
[JSON API Document](http://jsonapi.org/format/#document-top-level)

    GET /resource/{resourceType}?id=...&id=...

^ Returns all the specified [resources](http://jsonapi.org/format/#document-resource-objects) as a
[JSON API Document](http://jsonapi.org/format/#document-top-level)

    PUT /resource/{resourceType}/{resourceId}
    {
      "data": {
        "type": "{resourceType}",
        "id": "{resourceId}",
        "attributes": {
          "fizz": "buzz",
          "foo": "bar"
        },
        "relationships": {
          "hello": {
            "data": {"type": "some_other_type", "id": "world"}
          }
        }
      }
    }

^ Takes a [JSON API Document](http://jsonapi.org/format/#document-top-level) and stores the primary
[resource](http://jsonapi.org/format/#document-resource-objects) as a node in the graph and its
[relationships](http://jsonapi.org/format/#document-resource-object-relationships) as a directed edge in the graph.  The
referenced resource need not already be in Hydra's datastore (ie, Hydra has no problems with an edge pointing to nothing)

    DELETE /resource/{resourceType}/{resourceId}

^ Removes the [resource](http://jsonapi.org/format/#document-resource-objects) with the given type and id

    GET /changefeed

^ Returns a [JSON API Document](http://jsonapi.org/format/#document-top-level) listing the various changefeeds that have
been registered with Hydra.

    POST /changefeed
    {
      "data": {
        "type": "changefeed",
        "id": "secondary",
        "attributes": {
          "typeFilter": ["mcguffin"]
        },
        "relationships": {
          "parent": {
            "data": {"type": "changefeed", "id": "parent"}
          }
        }
      }
    }

^ Registers a new changefeed with Hydra.  The `typeFilter` attribute and the `parent` relationship are optional.  If
`typeFilter` is set, the created changefeed will only emit events for resources of the same type (case-sensitive).  If
the `parent` relationship is set, the created changefeed will only emit events once the parent changefeed has ack'd to or
beyond their seq number. Simply leave off the `relationships` attribute to use the default "global" parent.

    GET /changefeed/{id}

^ Returns a [JSON API Document](http://jsonapi.org/format/#document-top-level) with the specific changefeed requested.

    DELETE /changefeed/{id}

^ Deletes the specific changefeed.  Any open streaming connections for that changefeed will be closed.

    GET /changefeed/{id}/stream?bufferSize=1000

^ Opens a `Transfer-Encoding: chunked` stream of events for the specified changefeed.  Up to `bufferSize` events will be
emitted before the client must ack to receive more (See `POST /changefeed/{id}/ack`).  The default value for
`bufferSize` is 1,000.  Any value between 1 and 10,000 can be used.  The stream starts from the previously ack'd seq
number.  If there have been no acks for this changefeed, it starts at the beginning.  The changefeed will not emit every
event.  If the consumer is behind the latest events, any new events for a resource will result in older events for the
same resource being discarded.  The primary philosophy here is to ensure that each changefeed sees at least one event
whenever a resource has been updated (or a resource that this resource depends on is updated, and so on and so on).
Since it's assumed that the consumers of the changefeed only care about the latest value of the Resource and not the
intermediate values, Hydra aggressively discards irrelevant events.

Events are emitted as JSON objects, separated by a newline `\n` character.  There are three types of events:

* `event`, which contain the actual resource identifier and sequence number
* `keepalive`, which are emitted after 10 seconds of silence
* `error`, which are emitted when the stream has encountered an error.  The stream will likely close right after seeing
  a error event.

It's helpful to visualize this in curl:

```
$ curl --raw -i -H "key: foo" localhost:9000/changefeed/primary/stream
HTTP/1.1 200 OK
Transfer-Encoding: chunked
Content-Type: text/plain; charset=utf-8
Date: Thu, 02 Feb 2017 21:49:01 GMT

5f
{"eventType":"event","data":{"type":"parentMcguffin","id":"25U4RORO5NEM3HQ2E3LWANYWAI","seq":31}}

5f
{"eventType":"event","data":{"type":"parentMcguffin","id":"CNJYW2JYUBFQRHYEILBCEMXLUY","seq":32}}

59
{"eventType":"event","data":{"type":"mcguffin","id":"EQLXBICJ4RDTNGK3YQEBCKY3WY","seq":33}}

1a
{"eventType":"keepalive"}

59
{"eventType":"event","data":{"type":"mcguffin","id":"EQLXBICJ4RDTNGK3YQEBCKY3WY","seq":34}}

0
```

In this particular example, the `primary` changefeed did not have a `typeFilter`,
and the fetcher was rigged to die to provide an example `error` message.

    POST /changefeed/{id}/ack?ack=123

^ Acknowledge that the changefeed consumer has finished processing all events up to the specified sequence number.

Acking allows for several things to happen:

* Emit new events on the specified changefeed if that stream's buffer is full.
* Emit new events on children of this changefeed if those streams are waiting for new events.
* Snapshot the state of the consumer.  If the consumer needs to stop or reconnect to Hydra, Hydra will only emit events
  starting from just after the last ack'd seq number.


Dependencies
--------------
* Postgres 9.5
* Scala 2.11/SBT


Running Locally
---------------
* `vagrant up` in the repository root
* `vagrant ssh`
* `cd /vagrant`
* `sbt run`

To keep the `/vagrant` directory synced, from another terminal run `vagrant rsync-auto`.

Note that, for local Hydra development, Vagrant is much easier than docker-compose. This is due to the nature of the
Play dev server. Apparently, Play can only be run in development mode via the SBT or Activator console. By the time
you have an SBT-packaged binary (i.e. what we have), you are stuck. At least, that is the idea I have gotten from my
research on the matter.

## Regenerating Tables.scala
If you've updated the schema by creating a new sql file in `conf/evolutions/default`, go ahead and apply the evolution.
Then, from a terminal run:

```
$ sbt console
Welcome to Scala version 2.11.6 (Java HotSpot(TM) 64-Bit Server VM, Java 1.8.0_25).
Type in expressions to have them evaluated.
Type :help for more information.

scala> core.db.CustomizedCodeGenerator.main("/path/to/repo/root" + "/app", "dal")
```
