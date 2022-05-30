# Asset Management Service (AMS)

AMS is an alternative service to Blockchain's Transaction/Account balance service, but it's centralized database(MySQL8).

We used to use blockchain as the storage and management of user assets.Now we have 4 millions+ registered users, there are some problems to suit our future needs. 

* DB
  * [MySQL8 JSON](https://dev.mysql.com/doc/refman/8.0/en/json.html) for JSON data.
* API
  * [Sanic]() for asynchronous api services.
  * [encode databases[aiomysql]](https://github.com/encode/databases) for asynchronous MySQL support.
* Reverse Proxy and load balancer
  * [Traefik](https://github.com/traefik/traefik) with Docker for automatically and dynamically proxy and load balancer.

### Why not blockchain?
#### Expensive
With more users, the monthly expenses are getting higher and higher.

Our blockchain base consumes a lot of disk and memory resources, it's a high monthly cost bill from AMS.

This is the main reason.
#### Slow
Broadcast a transaction to blockchain is very slow. Our users need to wait *minutes* to confirm their asset transfer is done.

### Why not PostgreSQL or MongoDB?
Expensive, compared with AWS MySQL by our boss and OPs :|


## Features
* API
  * High availability
  * Idempotent
  * High concurrency
  * Preventing injection attacks
  * ~0 downtime
  * Microservices
  * No ORM
* DB
  * High concurrency
  * Transaction
  * JSON support
  * Split tables automatically by datetime or mod or both
* Asset
  * Validate Account and Transaction by customized hash
  * Single and bulk transactions transfer
  * Send warning messages to telegram group

Architecture
![Architecture](http://processon.com/chart_image/62443d2ae0b34d0730e8a9c1.png)