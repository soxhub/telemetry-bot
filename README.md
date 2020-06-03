# Telemetry Bot
A telemetry exporter that scrapes Prometheus and Jaeger and saves it  
to a compact Postgres-based time series database.

![Fact Core](doc/bot.png)



## Development
How do I do `X` in this rust project?

 - __surf:__ http client
 - __sqlx:__ database client
 - __async-std:__ async IO (files, sockets, etc)
