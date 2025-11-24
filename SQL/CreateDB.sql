CREATE DATABASE HTTTKD_STAGE
CREATE DATABASE HTTTKD_NDS
CREATE DATABASE HTTTKD_DDS
CREATE DATABASE HTTTKD_DQ_Metadata
CREATE DATABASE HTTTKD_ETL_Metadata
go

------------------------
--ETL_PROCESSING_METADATA
------------------------------------
USE HTTTKD_ETL_Metadata
GO
select* from packageTable
select* from data_flowTable
select* from sourceTable



CREATE TABLE packageTable 
(
    package_key INT PRIMARY KEY IDENTITY(1,1),
    package_name NVARCHAR(255),
    description NVARCHAR(1000),
    schedule NVARCHAR(255)        -- Ví dụ: 'Runs daily at 3 a.m.'
);


CREATE TABLE statusTable (
    status_key INT PRIMARY KEY,
    status_name NVARCHAR(50)      -- e.g., 'Unknown', 'Success', 'Failed', 'In Progress'
);
INSERT INTO statusTable VALUES
(0, 'Unknown'),
(1, 'Success'),
(2, 'Failed'),
(3, 'In Progress');


CREATE TABLE sourceTable (
	sourceID INT PRIMARY KEY IDENTITY(1,1),
	sourceName NVARCHAR(50),
	sourcePath NVARCHAR(200),
	lastExtract DATETIME
);



CREATE TABLE data_flowTable (
    flow_key INT PRIMARY KEY IDENTITY(1,1),
    flow_name NVARCHAR(255),
    description NVARCHAR(1000),
    source INT,
    target NVARCHAR(255),
    transformation NVARCHAR(1000),
    package_key INT,
    status_key INT,
    LSET DATETIME,        -- Last Successful Extract Time
    CET DATETIME,         -- Current Extract Time
    FOREIGN KEY (package_key) REFERENCES packageTable(package_key),
    FOREIGN KEY (status_key) REFERENCES statusTable(status_key)
);








------------------------------------
--DQ_METADATA
------------------------------------
USE HTTTKD_DQ_Metadata

CREATE TABLE rule_type (--phân loại rule
    rule_type_id CHAR(1) PRIMARY KEY,
    rule_type_name NVARCHAR(100)
);
INSERT INTO rule_type VALUES ('E', 'Error'), ('W', 'Warning');



CREATE TABLE rule_category (-- phân loại nhóm rule
    rule_cat_id CHAR(1) PRIMARY KEY,
    rule_cat_name NVARCHAR(255)
);
INSERT INTO rule_category VALUES
('I', 'Incoming data validation rules'),
('C', 'Cross-reference validation rules'),
('D', 'Internal DW validation rules');



CREATE TABLE rule_risk_level (
    level INT PRIMARY KEY,
    description NVARCHAR(255)
);
INSERT INTO rule_risk_level VALUES
(1, 'No business impact'),
(2, 'Minor impact'),
(3, 'Moderate impact'),
(4, 'Major impact'),
(5, 'Severe damage to business financial positions');


CREATE TABLE rule_status (-- trạng thái của rule
    status CHAR(1) PRIMARY KEY,
    description NVARCHAR(50)
);
INSERT INTO rule_status VALUES
('A', 'Active'),
('D', 'Decommissioned');


CREATE TABLE rule_action (-- hành động khi rule vi phạm
    action CHAR(1) PRIMARY KEY,
    description NVARCHAR(50)
);
INSERT INTO rule_action VALUES
('R', 'Reject'),
('A', 'Allow'),
('F', 'Fix');



CREATE TABLE DQ_Rules (--Bảng trung tâm lưu toàn bộ rule, liên kết đến các bảng nhỏ phía trên.
    rule_key INT PRIMARY KEY IDENTITY(1,1),
    rule_name NVARCHAR(255),
    description NVARCHAR(1000),
    rule_type_id CHAR(1),
    rule_cat_id CHAR(1),
    risk_level INT,
    status CHAR(1),
    action CHAR(1),
    create_timestamp DATETIME,
    update_timestamp DATETIME,
    FOREIGN KEY (rule_type_id) REFERENCES rule_type(rule_type_id),
    FOREIGN KEY (rule_cat_id) REFERENCES rule_category(rule_cat_id),
    FOREIGN KEY (risk_level) REFERENCES rule_risk_level(level),
    FOREIGN KEY (status) REFERENCES rule_status(status),
    FOREIGN KEY (action) REFERENCES rule_action(action)
);



CREATE TABLE recipient_type (
    type CHAR(1) PRIMARY KEY,
    description NVARCHAR(50)
);

CREATE TABLE DQ_Log (
    dq_log_id INT IDENTITY(1,1) PRIMARY KEY,

    table_name NVARCHAR(100) NOT NULL,        -- Flights, Flights_detail, ...
    column_name NVARCHAR(100) NULL,           -- Cột gây lỗi

    business_key NVARCHAR(200) NULL,          -- DATE + AIRLINE + FLIGHT_NUMBER
    source_id INT NULL,                       -- từ sourceTable

    rule_key INT NULL,                        -- liên kết tới DQ_Rules
    error_type NVARCHAR(50),                  -- Conversion|Lookup|Constraint

    error_message NVARCHAR(2000),             -- thông báo lỗi
    raw_value NVARCHAR(500) NULL,             -- giá trị gây lỗi (DATE='ABC', DELAY='N/A')
    full_row NVARCHAR(MAX) NULL,              -- lưu full row Stage (dạng text), không cần JSON

    created_at DATETIME DEFAULT GETDATE(),

    FOREIGN KEY (rule_key) REFERENCES DQ_Rules(rule_key)
);
GO

INSERT INTO recipient_type VALUES
('I', 'Individual'),
('G', 'Group');






------------------------------------
--STAGE
------------------------------------
USE HTTTKD_STAGE
GO
select* from Airlines
select* from Airports
select top 5* from Flights
select count(*) from Flights


CREATE TABLE Airlines(
  IATA_CODE NVARCHAR(10),
  AIRLINE   NVARCHAR(255)
);
GO

CREATE TABLE Airports (                  
    IATA_CODE NVARCHAR(10),             -- mã sân bay 3 ký tự (ATL, BOS, …)
    AIRPORT NVARCHAR(255),              -- tên sân bay
    CITY NVARCHAR(255),                 -- thành phố
    STATE NVARCHAR(50),                 -- bang (GA, MA, TX, …)
    COUNTRY NVARCHAR(50),               -- quốc gia
    LATITUDE NVARCHAR(10),                     -- vĩ độ
    LONGITUDE NVARCHAR(10)                     -- kinh độ
);
GO
CREATE TABLE Flights (

    [DATE] NVARCHAR(20) NULL,
    [AIRLINE] NVARCHAR(10) NULL,
    [FLIGHT_NUMBER] NVARCHAR(50) NULL,
    [TAIL_NUMBER] NVARCHAR(50) NULL,
    [ORIGIN_AIRPORT] NVARCHAR(50) NULL,
    [DESTINATION_AIRPORT] NVARCHAR(50) NULL,
    [SCHEDULED_DEPARTURE] NVARCHAR(50)  NULL,
    [DEPARTURE_TIME] NVARCHAR(50)  NULL,
    [DEPARTURE_DELAY] NVARCHAR(50)  NULL,
    [TAXI_OUT] INT NULL,                 -- thời gian di chuyển ra đường băng (phút)
    [WHEELS_OFF] INT NULL,               -- thời điểm cất cánh (phút từ 0h)
    [SCHEDULED_TIME] INT NULL,           -- tổng thời gian dự kiến (phút)
    [ELAPSED_TIME] INT NULL,             -- tổng thời gian thực tế (phút)
    [AIR_TIME] INT NULL,                 -- thời gian bay trên không (phút)
    [DISTANCE] INT NULL,                 -- quãng đường bay (dặm)
    [WHEELS_ON] INT NULL,                -- thời điểm hạ cánh (phút từ 0h)
    [TAXI_IN] INT NULL,                  -- thời gian taxi vào gate (phút)
    [SCHEDULED_ARRIVAL] INT NULL,        -- giờ đến dự kiến
    [ARRIVAL_TIME] INT NULL,             -- giờ đến thực tế
    [ARRIVAL_DELAY] INT NULL,            -- độ trễ khi đến (phút)
    [DIVERTED] BIT NULL,                 -- chuyến bay bị đổi hướng (1/0)
    [CANCELLED] BIT NULL,                -- chuyến bay bị hủy (1/0)
    [CANCELLATION_REASON] NVARCHAR(50) NULL, -- lý do hủy (A/B/C/D)
    [AIR_SYSTEM_DELAY] INT NULL,         -- delay do hệ thống không lưu
    [SECURITY_DELAY] INT NULL,           -- delay do an ninh
    [AIRLINE_DELAY] INT NULL,            -- delay do hãng hàng không
    [LATE_AIRCRAFT_DELAY] INT NULL,      -- delay do máy bay đến trễ
    [WEATHER_DELAY] INT NULL,            -- delay do thời tiết
    [CREATED] DATETIME NULL,             -- thời điểm tạo bản ghi (ETL timestamp)
    [MODIFIED] DATETIME NULL             -- thời điểm cập nhật gần nhất
);
GO


------------------------------------
--NDS
------------------------------------
use HTTTKD_NDS
go
--select* from AirlinesNDS


CREATE TABLE AirportsNDS (
    Airport_SK INT PRIMARY KEY IDENTITY(1,1), 
    IATA_CODE NVARCHAR(10) NOT NULL,
    AIRPORT NVARCHAR(255),
    CITY NVARCHAR(255),
    STATE NVARCHAR(50),
    COUNTRY NVARCHAR(50),
    LATITUDE DECIMAL(9, 6),
	LONGITUDE DECIMAL(9, 6),
	UpdatedAt DATETIME
);
GO


CREATE TABLE AirlinesNDS (
    Airline_SK INT IDENTITY(1,1) PRIMARY KEY, -- Surrogate Key (Khóa tự tăng của hệ thống mình)
    Airline_IATA NVARCHAR(10),                -- Business Key (Mã từ hệ thống nguồn)
    Airline_Name NVARCHAR(255),
    SourceID INT,                             -- Lấy từ bảng sourceTable bên Metadata
    CreatedDate DATETIME DEFAULT GETDATE(),
    UpdatedDate DATETIME DEFAULT GETDATE()
);
GO
------------------------------------------------------------------------------------------------------------
---NDS CHO FLIGHT (COPY TỪ ĐÂY)
------------------------------------------------------------------------------------------------------------
USE HTTTKD_NDS
GO

-- Xóa các bảng cũ để tránh nhầm lẫn
IF OBJECT_ID('NDS_Flight_Detail', 'U') IS NOT NULL DROP TABLE NDS_Flight_Detail;
IF OBJECT_ID('NDS_Flights', 'U') IS NOT NULL DROP TABLE NDS_Flights;

CREATE TABLE NDS_Flights (
    Flight_SK INT IDENTITY(1,1) PRIMARY KEY,
    
    -- 1. THÔNG TIN QUẢN LÝ & KHÓA
    SourceID INT,
    Flight_BK NVARCHAR(200),    -- Business Key (DATE + AIRLINE + FLIGHT_NUMBER + ORIGIN)
    
    -- 2. THÔNG TIN ĐỊNH DANH (Dimensions) -> Dùng SK để chuẩn hóa
    [DATE] DATE,
    Airline_SK INT FOREIGN KEY REFERENCES AirlinesNDS(Airline_SK),
    Origin_Airport_SK INT FOREIGN KEY REFERENCES AirportsNDS(Airport_SK),
    Dest_Airport_SK INT FOREIGN KEY REFERENCES AirportsNDS(Airport_SK),
    
    FLIGHT_NUMBER NVARCHAR(50),
    TAIL_NUMBER NVARCHAR(50),
    
    -- 3. THÔNG TIN THỜI GIAN (SCHEDULED & ACTUAL)
    SCHEDULED_DEPARTURE NVARCHAR(10),
    DEPARTURE_TIME NVARCHAR(10),
    SCHEDULED_ARRIVAL INT,      -- Gộp từ bảng Detail qua
    ARRIVAL_TIME INT,           -- Gộp từ bảng Detail qua
    
    -- 4. CÁC METRICS ĐO ĐẠC (Gộp từ bảng Detail qua)
    DEPARTURE_DELAY INT,
    ARRIVAL_DELAY INT,
    TAXI_OUT INT,
    WHEELS_OFF INT,
    SCHEDULED_TIME INT,
    ELAPSED_TIME INT,
    AIR_TIME INT,
    DISTANCE INT,
    WHEELS_ON INT,
    TAXI_IN INT,
    
    -- 5. TRẠNG THÁI & NGUYÊN NHÂN
    DIVERTED BIT,
    CANCELLED BIT,
    CANCELLATION_REASON NVARCHAR(50),
    
    -- 6. CHI TIẾT DELAY
    AIR_SYSTEM_DELAY INT,
    SECURITY_DELAY INT,
    AIRLINE_DELAY INT,
    LATE_AIRCRAFT_DELAY INT,
    WEATHER_DELAY INT,

    -- 7. METADATA
    CreatedDate DATETIME DEFAULT GETDATE(),
    LastUpdatedDate DATETIME NULL
);

ALTER TABLE NDS_Flights
ALTER COLUMN [SCHEDULED_DEPARTURE] INT NULL;
ALTER TABLE NDS_Flights
ALTER COLUMN [DEPARTURE_TIME] INT NULL;
ALTER TABLE NDS_Flights
ALTER COLUMN [DEPARTURE_DELAY] INT NULL;
-- Tạo Index bắt buộc để ETL nhanh (Upsert)
CREATE UNIQUE INDEX UX_NDS_Flights_BK ON NDS_Flights(Flight_BK);
GO
----------------------------------------------------------------------------------------------------------------------
-- CHO TỚI ĐÂY--
----------------------------------------------------------------------------------------------------------------------

------------------------------------
--DDS
------------------------------------
use HTTTKD_DDS
go

CREATE TABLE DimDate (
    Date_SK INT IDENTITY(1,1) PRIMARY KEY,
    FullDate DATE,
    Day INT,
    Month INT,
    MonthName NVARCHAR(20),
    Quarter INT,
    Year INT,
	CreatedDate DATETIME DEFAULT GETDATE(),
    UpdatedDate DATETIME DEFAULT GETDATE()
);
-- là System-generated Dimensions nên cần tạo ra 
DECLARE @StartDate DATE = '2015-01-01';
DECLARE @EndDate DATE = '2025-12-31';

WHILE @StartDate <= @EndDate
BEGIN
    INSERT INTO DimDate (FullDate, Day, Month, MonthName, Quarter, Year)
    VALUES (
        @StartDate,
        DAY(@StartDate),
        MONTH(@StartDate),
        DATENAME(MONTH, @StartDate),
        DATEPART(QUARTER, @StartDate),
        YEAR(@StartDate)
    )

    SET @StartDate = DATEADD(DAY, 1, @StartDate)
END
-------------\\------------------------

CREATE TABLE DimTime (
    Time_SK INT IDENTITY(1,1) PRIMARY KEY,
    Hour INT,
    Minute INT,
    Period NVARCHAR(10),  -- Morning / Afternoon / Evening / Night,
	CreatedDate DATETIME DEFAULT GETDATE(),
    UpdatedDate DATETIME DEFAULT GETDATE()
);
--Tương tự như DimDate
DECLARE @Hour INT = 0
DECLARE @Minute INT = 0

WHILE @Hour < 24
BEGIN
    SET @Minute = 0

    WHILE @Minute < 60
    BEGIN
        INSERT INTO DimTime (Hour, Minute, Period)
        VALUES (
            @Hour,
            @Minute,
            CASE 
                WHEN @Hour < 12 THEN 'Morning'
                WHEN @Hour < 17 THEN 'Afternoon'
                WHEN @Hour < 21 THEN 'Evening'
                ELSE 'Night'
            END
        )

        SET @Minute = @Minute + 1
    END

    SET @Hour = @Hour + 1
END

-------------\\------------------------

CREATE TABLE DimCancellationReason (
    Reason_SK INT IDENTITY(1,1) PRIMARY KEY,
    Reason_Code NVARCHAR(10),
    Description NVARCHAR(255),
	CreatedDate DATETIME DEFAULT GETDATE(),
    UpdatedDate DATETIME DEFAULT GETDATE()
);
INSERT INTO DimCancellationReason (Reason_Code, Description, CreatedDate) VALUES
('A', 'Airline/Carrier', getdate()),
('B', 'Weather', getdate()),
('C', 'National Air System', getdate()),
('D', 'Security', getdate());

--------------------------==========================================
--------------------------==========================================


CREATE TABLE DimAirline (
    Airline_SK INT IDENTITY(1,1) PRIMARY KEY,
    Airline_IATA NVARCHAR(10),
    Airline_Name NVARCHAR(255),
	CreatedDate DATETIME DEFAULT GETDATE(),
    UpdatedDate DATETIME DEFAULT GETDATE()
);	


CREATE TABLE DimAirport (
    Airport_SK INT IDENTITY(1,1) PRIMARY KEY,
    IATA_CODE NVARCHAR(10),
    AIRPORT NVARCHAR(255),
    CITY NVARCHAR(255),
    STATE NVARCHAR(50),
    COUNTRY NVARCHAR(50),
	CreatedDate DATETIME DEFAULT GETDATE(),
    UpdatedDate DATETIME DEFAULT GETDATE()
);


CREATE TABLE FactFlight (
    Flight_SK INT IDENTITY(1,1) PRIMARY KEY,

    -- Dimension Keys
    Date_SK INT,
    Airline_SK INT,
    Origin_Airport_SK INT,
    Dest_Airport_SK INT,
    Scheduled_Departure_Time_SK INT,
    Scheduled_Arrival_Time_SK INT,
    Cancellation_Reason_SK INT NULL,

    -- Flight Information
    Flight_Number NVARCHAR(50),
    Cancelled BIT,
    Diverted BIT,

    -- Delay Measures
    Departure_Delay INT,
    Arrival_Delay INT,

    Air_System_Delay INT,
    Security_Delay INT,
    Airline_Delay INT,
    Late_Aircraft_Delay INT,
    Weather_Delay INT,

    -- Performance Measures
    Distance INT,
    Taxi_Out INT,
    Taxi_In INT,
    Air_Time INT,
    Scheduled_Time INT,
    Elapsed_Time INT,

    -- Derived Performance
    Is_OnTime_Departure BIT,
    Is_OnTime_Arrival BIT,
    Is_Delay_15 BIT,

    CreatedDate DATETIME DEFAULT GETDATE(),
    UpdatedDate DATETIME DEFAULT GETDATE(),

    FOREIGN KEY (Date_SK) REFERENCES DimDate(Date_SK),
    FOREIGN KEY (Airline_SK) REFERENCES DimAirline(Airline_SK),
    FOREIGN KEY (Origin_Airport_SK) REFERENCES DimAirport(Airport_SK),
    FOREIGN KEY (Dest_Airport_SK) REFERENCES DimAirport(Airport_SK),
    FOREIGN KEY (Scheduled_Departure_Time_SK) REFERENCES DimTime(Time_SK),
    FOREIGN KEY (Scheduled_Arrival_Time_SK) REFERENCES DimTime(Time_SK),
    FOREIGN KEY (Cancellation_Reason_SK) REFERENCES DimCancellationReason(Reason_SK)
);




