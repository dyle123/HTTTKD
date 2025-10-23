Create DB if not exists HTTTKD;
go
use HTTTKD;
go

create schema stg;
create schema dw;
create schema nds;
go

CREATE TABLE stg.Airlines(
  IATA_CODE NVARCHAR(10),
  AIRLINE   NVARCHAR(255)
);

CREATE TABLE stg.Airports (                  
    IATA_CODE NVARCHAR(10),             -- mã sân bay 3 ký tự (ATL, BOS, …)
    AIRPORT NVARCHAR(255),              -- tên sân bay
    CITY NVARCHAR(255),                 -- thành phố
    STATE NVARCHAR(50),                 -- bang (GA, MA, TX, …)
    COUNTRY NVARCHAR(50),               -- quốc gia
    LATITUDE FLOAT,                     -- vĩ độ
    LONGITUDE FLOAT                     -- kinh độ
);

CREATE TABLE stg.Flights (
    [DATE] DATE NULL,
    [AIRLINE] NVARCHAR(255) NULL,
    [FLIGHT_NUMBER] NVARCHAR(20) NULL,
    [TAIL_NUMBER] NVARCHAR(20) NULL,
    [ORIGIN_AIRPORT] NVARCHAR(10) NULL,
    [DESTINATION_AIRPORT] NVARCHAR(10) NULL,
    [SCHEDULED_DEPARTURE] INT NULL,
    [DEPARTURE_TIME] INT NULL,
    [DEPARTURE_DELAY] INT NULL,
    [TAXI_OUT] INT NULL,
    [WHEELS_OFF] INT NULL,
    [SCHEDULED_TIME] INT NULL,
    [ELAPSED_TIME] INT NULL,
    [AIR_TIME] INT NULL,
    [DISTANCE] INT NULL,
    [WHEELS_ON] INT NULL,
    [TAXI_IN] INT NULL,
    [SCHEDULED_ARRIVAL] INT NULL,
    [ARRIVAL_TIME] INT NULL,
    [ARRIVAL_DELAY] INT NULL,
    [DIVERTED] BIT NULL,
    [CANCELLED] BIT NULL,
    [CANCELLATION_REASON] NVARCHAR(50) NULL,
    [AIR_SYSTEM_DELAY] INT NULL,
    [SECURITY_DELAY] INT NULL,
    [AIRLINE_DELAY] INT NULL,
    [LATE_AIRCRAFT_DELAY] INT NULL,
    [WEATHER_DELAY] INT NULL,
    [CREATED] DATETIME NULL,
    [MODIFIED] DATETIME NULL
);


CREATE TABLE Data_Flow (
    ID INT IDENTITY(1,1) PRIMARY KEY,
    Name NVARCHAR(100),          -- Tên package hoặc bảng (vd: 'Airlines')
    CET DATETIME NULL,           -- Current Extract Time
    LSET DATETIME NULL,          -- Last Successful Extract Time
    LastStatus NVARCHAR(50) NULL -- SUCCESS / FAIL
);
INSERT INTO Data_Flow (Name, CET, LSET, LastStatus)
VALUES 
('Airlines', NULL, NULL, NULL),
('Airports', NULL, NULL, NULL),
('Flights', NULL, NULL, NULL);


