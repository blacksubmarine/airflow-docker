
CREATE SCHEMA IF NOT EXISTS Device;

CREATE TABLE device.laptops_details (
    Id SERIAL PRIMARY KEY,
    Company VARCHAR(200) NOT NULL,
    TypeName VARCHAR(100) NOT NULL,
    Inches NUMERIC(4,1) NOT NULL,
    ScreenResolution VARCHAR(100) NOT NULL,
    Cpu VARCHAR(100) NOT NULL,
    Ram VARCHAR(100) NOT NULL,
    Memory VARCHAR(100) NOT NULL,
    Gpu VARCHAR(100) NOT NULL,
    OpSys VARCHAR(100) NOT NULL,
    Weight NUMERIC(5,2) NOT NULL,
    Price NUMERIC(10,2) NOT NULL
);

SELECT
    Id,
    Company,
    TypeName,
    Inches,
    ScreenResolution,
    Cpu,
    Ram,
    Memory,
    Gpu,
    OpSys,
    Weight,
    Price
FROM
    device.laptops_details;
