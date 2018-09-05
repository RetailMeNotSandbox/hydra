# Data Model

## Resources
Hydra is fundamentally a directed graph of resources: a resource *r1* may refer to one or more other resources *r2*, *r3*, ..., *rN*. These other resources are considered "parent" resources of *r1*. By referring to them, *r1* has declared an existential dependency on them. Conversely, *r1* is considered the "child" of each of *r2*, *r3*, ..., *rN*.

Formally:

* Define the relation *Parent* so that *Parent(rI, rJ)* iff there is a relationship from *rI* to *rJ*.
* Define the relation *Ancestor* recursively:

  If *Parent(rI, rJ)*, then *Ancestor(rI, rJ)*.
  If *Ancestor(rI, rJ)* and *Parent(rJ, rK)*, then *Ancestor(rI, rK)*.

* Then, using this notation, here is the main conceit of Hydra: consumers who are interested in changes to a resource will also be interested in changes to any of its ancestors.

Resource data is stored in the `resource` table, using the PostgreSQL JSONB type to directly encode the JSON-API representation of each resource. The *Parent* relation is expressed by JSON-API relationships. (These are stored in a separately indexed column for fast traversal, as well as a separate table `resource_reference`, kept in sync via database trigger.)

## Sequence numbers
We associate each resource with a _sequence number_. Loosely speaking, the sequence number is like a "last-updated" timestamp on the `resource` table. But the value of the sequence number isn't coupled to the clock. Instead, we require:

1. the value must globally increase across the whole `resource` table as edits are made.
2. the value must strictly increase: no two resources can have the same sequence number.

Now, a consumer interested in *r1* will want to process events up to and including the current sequence number for *r1*. Moreover, it will want to process events up to and including the current sequence number of all *r1*'s ancestor resources, since *r1* existentially depends on them. Hence, if you care about *r1*, you'll want to process up to and including

*maxRelevantSeq(r) := max { seq(r_i) | Ancestor(r, r_i) }*

For each resource, Hydra keeps this aggregation pre-computed in a table called `change_history`.

## Changefeeds
Each Hydra consumer creates a row in the `changefeed` table with a unique id and set of resource types that it cares about. Then, as long as the consumer is subscribed, it will receive the latest version of each resource it cares about. As it receives the resources, it can perform actions on them and mark its progress by acknowledging ("acking") each sequence number. In the `changefeed` table, Hydra records the highest acknowledged number ("maxAck") of each consumer.


# Components

## Application
* Resource controller: handles Create/Read/Update/Delete operations on resources. Updates `resource`.
* Changefeed controller: handles CRUD operations on changefeeds. More importantly, this is the piece that receives subscription requests. For each subscription request, it creates a ChangefeedSource actor to fulfill it, and wires up a stream to send the ChangefeedSource's output to the client via a chunked HTTP response.
* ChangefeedSource: Creates a ChangefeedFetcher for the desired changefeed. Holds a buffer of events from the ChangefeedFetcher, which it streams on demand.
* ChangefeedFetcher: Subscribes to the AckSubscriptionManager, to be notified when its changefeed has been acked. When ChangefeedFetcher is notified by the AckSubscriptionManager, it reads events from `change_history` and forwards them to its parent (the ChangefeedSource).
* AckSubscriptionManager: for each changefeed-specific subscriber, creates an asynchronous PostgreSQL connection to listen on the desired changefeed's [PostgreSQL notification channel](https://www.postgresql.org/docs/9.5/static/sql-notify.html). When a notification comes in on the notification channel, AckSubscriptionManager forwards it on to the subscriber.
* ScratchExpander: performs "expansion" from `change_history_to_expand` into `change_history`. For each resource-specific sequence bump in `change_history_to_expand`, ScratchExpander updates the maximum relevant sequence numbers in `change_history` for
    * the affected resource
    * all its child resources

## Database
* `resource`: the main table (together with `resource_reference`, an implementation detail that makes graph traversal faster). A trigger inserts into `change_history_to_expand` whenever this table is updated.
* `change_history_to_expand`: a staging area for `change_history`. ScratchExpander reads from this table.
* `change_history`: the maximum relevant sequence number for each resource
* `changefeed`: tracks the state of changefeeds: the filter for pertinent events, and the highest sequence number that has been acknowledged by the consumer. Whenever a changefeed is acked, a trigger notifies the corresponding notification channel.

# Stack
* [Play Framework](https://www.playframework.com/documentation/2.5.x/ScalaHome) as the web server implementation
* [Slick](http://slick.lightbend.com/doc/3.1.0/) for database queries/ORM-type functionality
* [slick-pg](https://github.com/tminglei/slick-pg/tree/v0.14.4) for the Slick backend
