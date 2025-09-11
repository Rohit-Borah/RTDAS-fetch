-- CREATE TABLE IF NOT EXISTS rtdas_aws_master (
--     stationID VARCHAR(50) PRIMARY KEY,
--     name TEXT,
--     location TEXT,
--     longitude NUMERIC,
--     latitude NUMERIC,
--     type VARCHAR(20),
--     zone VARCHAR(50)
-- );

-- CREATE TABLE IF NOT EXISTS rtdas_awlr_master (
--     stationID VARCHAR(50) PRIMARY KEY,
--     name TEXT,
--     location TEXT,
--     longitude NUMERIC,
--     latitude NUMERIC,
--     type VARCHAR(20),
--     zone VARCHAR(50)
-- );

-- CREATE TABLE IF NOT EXISTS rtdas_arg_master (
--     stationID VARCHAR(50) PRIMARY KEY,
--     name TEXT,
--     location TEXT,
--     longitude NUMERIC,
--     latitude NUMERIC,
--     type VARCHAR(20),
--     zone VARCHAR(50)
-- );

CREATE TABLE IF NOT EXISTS rtdas_master (
    stationID VARCHAR(50) PRIMARY KEY,
    name TEXT,
    location TEXT,
    longitude NUMERIC,
    latitude NUMERIC,
    type VARCHAR(20),   -- AWS / AWLR / ARG
    zone VARCHAR(50),
    source VARCHAR(10)  -- To track which API (AWS/AWLR/ARG)
);
