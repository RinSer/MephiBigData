CREATE KEYSPACE flights
    WITH REPLICATION = {
        'class' : 'SimpleStrategy',
        'replication_factor' : 1
    };
USE flights;
CREATE TABLE counts (
    time timestamp,
    departure text,
    arrival text,
    count bigint,
    PRIMARY KEY ((departure, arrival), time)
);