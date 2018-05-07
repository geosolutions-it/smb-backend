CREATE TABLE public.testing
(
    elevation real,
    sessionid bigint,
    "timestamp" bigint,
    accelerationx real,
    accelerationy real,
    accelerationz real,
    accuracy real,
    batconsumptionperhour real,
    batterylevel real,
    devicebearing real,
    devicepitch real,
    deviceroll real,
    gps_bearing real,
    humidity real,
    lumen real,
    pressure real,
    proximity real,
    speed real,
    temperature real,
    vehiclemode integer,
    serialversionuid bigint,
    color bigint
)
WITH (
    OIDS = FALSE
)
TABLESPACE pg_default;

SELECT AddGeometryColumn('testing', 'the_geom', 4326, 'POINT', 2 );

ALTER TABLE public.testing
    OWNER to lambda;