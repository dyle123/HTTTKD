DROP DATABASE HTTTKD_STAGE;
DROP DATABASE HTTTKD_NDS;
DROP DATABASE HTTTKD_DDS;
DROP DATABASE HTTTKD_DQ_Metadata;
DROP DATABASE HTTTKD_ETL_Metadata;

CREATE DATABASE HTTTKD_STAGE
CREATE DATABASE HTTTKD_NDS
CREATE DATABASE HTTTKD_DDS
CREATE DATABASE HTTTKD_DQ_Metadata
CREATE DATABASE HTTTKD_ETL_Metadata
go

------------------------------------
--ETL_PROCESSING_METADATA
------------------------------------
USE HTTTKD_ETL_Metadata
GO
/*
select* from packageTable
select* from data_flowTable
select* from sourceTable
*/

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

INSERT INTO recipient_type VALUES
('I', 'Individual'),
('G', 'Group');


USE HTTTKD_DQ_Metadata;
GO

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




------------------------------------
--STAGE
------------------------------------
/*USE HTTTKD_STAGE
GO



--select* from Airlines
--select* from Airports
/*select top 5* from Flights
select count(*) from Flights
DROP TABLE Airlines;
DROP TABLE Airports;
*/
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
--DROP TABLE Flights;
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
    
);
*/
USE HTTTKD_STAGE
GO

-- Xóa bảng cũ để tạo lại
DROP TABLE IF EXISTS Flights_detail;
DROP TABLE IF EXISTS Flights;
DROP TABLE IF EXISTS Airports;
DROP TABLE IF EXISTS Airlines;
GO

---------------------------------------------------
-- 1. Bảng Airlines (Giữ nguyên)
---------------------------------------------------
CREATE TABLE Airlines(
  IATA_CODE NVARCHAR(10),
  AIRLINE   NVARCHAR(255)
);
GO

---------------------------------------------------
-- 2. Bảng Airports (Giữ nguyên)
---------------------------------------------------
CREATE TABLE Airports (                   
    IATA_CODE NVARCHAR(10),             -- mã sân bay 3 ký tự (ATL, BOS, …)
    AIRPORT NVARCHAR(255),              -- tên sân bay
    CITY NVARCHAR(255),                 -- thành phố
    STATE NVARCHAR(50),                 -- bang (GA, MA, TX, …)
    COUNTRY NVARCHAR(50),               -- quốc gia
    LATITUDE NVARCHAR(10),              -- vĩ độ
    LONGITUDE NVARCHAR(10)              -- kinh độ
);
GO

---------------------------------------------------
-- 3. Bảng Flights (Chứa thông tin định danh)
---------------------------------------------------
CREATE TABLE Flights (
    -- Thêm Identity ID để dễ quản lý trong Stage (Option)
    [Stage_Flight_ID] INT IDENTITY(1,1) PRIMARY KEY, 
    
    [DATE] NVARCHAR(20) NULL,
    [AIRLINE] NVARCHAR(10) NULL,
    [FLIGHT_NUMBER] NVARCHAR(50) NULL,
    [TAIL_NUMBER] NVARCHAR(50) NULL,
    [ORIGIN_AIRPORT] NVARCHAR(50) NULL,
    [DESTINATION_AIRPORT] NVARCHAR(50) NULL,
    [SCHEDULED_DEPARTURE] NVARCHAR(50)  NULL,
    [DEPARTURE_TIME] NVARCHAR(50)  NULL,
    [DEPARTURE_DELAY] NVARCHAR(50)  NULL
);
GO

---------------------------------------------------
-- 4. Bảng Flights_detail (Đã SỬA: Thêm cột Khóa)
---------------------------------------------------
CREATE TABLE Flights_detail (
    -- [QUAN TRỌNG] Thêm các cột này để JOIN được với bảng Flights
    [DATE] NVARCHAR(20) NULL,
    [AIRLINE] NVARCHAR(10) NULL,
    [FLIGHT_NUMBER] NVARCHAR(50) NULL,
    [ORIGIN_AIRPORT] NVARCHAR(50) NULL,

    -- Các cột Metrics cũ
    [TAXI_OUT] INT NULL,                 -- thời gian di chuyển ra đường băng
    [WHEELS_OFF] INT NULL,               -- thời điểm cất cánh
    [SCHEDULED_TIME] INT NULL,           -- tổng thời gian dự kiến
    [ELAPSED_TIME] INT NULL,             -- tổng thời gian thực tế
    [AIR_TIME] INT NULL,                 -- thời gian bay trên không
    [DISTANCE] INT NULL,                 -- quãng đường bay
    [WHEELS_ON] INT NULL,                -- thời điểm hạ cánh
    [TAXI_IN] INT NULL,                  -- thời gian taxi vào gate
    [SCHEDULED_ARRIVAL] INT NULL,        -- giờ đến dự kiến
    [ARRIVAL_TIME] INT NULL,             -- giờ đến thực tế
    [ARRIVAL_DELAY] INT NULL,            -- độ trễ khi đến
    [DIVERTED] BIT NULL,                 -- chuyến bay bị đổi hướng
    [CANCELLED] BIT NULL,                -- chuyến bay bị hủy
    [CANCELLATION_REASON] NVARCHAR(50) NULL, 
    [AIR_SYSTEM_DELAY] INT NULL,         
    [SECURITY_DELAY] INT NULL,           
    [AIRLINE_DELAY] INT NULL,            
    [LATE_AIRCRAFT_DELAY] INT NULL,      
    [WEATHER_DELAY] INT NULL,            
    [CREATED] DATETIME DEFAULT GETDATE(),            
    [MODIFIED] DATETIME NULL             
);
GO

-- Tạo Index để JOIN nhanh hơn (Optional nhưng nên làm)
CREATE INDEX IX_Flights_Key ON Flights([DATE], [AIRLINE], [FLIGHT_NUMBER], [ORIGIN_AIRPORT]);
CREATE INDEX IX_Detail_Key ON Flights_detail([DATE], [AIRLINE], [FLIGHT_NUMBER], [ORIGIN_AIRPORT]);
GO
use HTTTKD_NDS;
select count (*) from Flights;
select top 10* from NDS_Flights;
GO
--drop table Flights_detail;
CREATE TABLE Flights_detail (
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

USE HTTTKD_NDS;
GO
DROP TABLE IF EXISTS NDS_Flights;
select count(*) from NDS_Flights;
-- Flights (NDS) với Surrogate Key, source_id, last_updated
USE HTTTKD_NDS;
GO
DROP TABLE NDS_Flights;
truncate table NDS_Flights;
CREATE TABLE NDS_Flights (
    Flight_SK INT IDENTITY(1,1) PRIMARY KEY,
    SourceID INT,                       -- sẽ được add trong Derived Column
    Flight_BK NVARCHAR(200),            -- Business Key (DATE + AIRLINE + FLIGHT_NUMBER)
    [DATE] DATE NULL,
    AIRLINE NVARCHAR(10),
    FLIGHT_NUMBER NVARCHAR(50),
    TAIL_NUMBER NVARCHAR(50),
    ORIGIN_AIRPORT NVARCHAR(50),
    DESTINATION_AIRPORT NVARCHAR(50),
    SCHEDULED_DEPARTURE INT NULL,
    DEPARTURE_TIME INT NULL,
    DEPARTURE_DELAY INT NULL,
    CreatedDate DATETIME DEFAULT GETDATE(),
    LastUpdatedDate DATETIME NULL
);
ALTER TABLE NDS_Flights
ALTER COLUMN SourceID NVARCHAR(50);
GO
DROP TABLE IF EXISTS NDS_Flight_Detail;
-- Flight detail (NDS) có FK đến FlightSK (nullable nếu chưa xác định)
drop table NDS_Flight_Detail;
CREATE TABLE NDS_Flight_Detail (
    Detail_SK INT IDENTITY(1,1) PRIMARY KEY,
    Flight_SK INT FOREIGN KEY REFERENCES NDS_Flights(Flight_SK),

    TAXI_OUT INT,
    WHEELS_OFF INT,
    SCHEDULED_TIME INT,
    ELAPSED_TIME INT,        
    AIR_TIME INT,
    DISTANCE INT,
    WHEELS_ON INT,
    TAXI_IN INT,
    SCHEDULED_ARRIVAL INT,
    ARRIVAL_TIME INT,
    ARRIVAL_DELAY INT,
    DIVERTED BIT,
    CANCELLED BIT,
    CANCELLATION_REASON NVARCHAR(50),
    AIR_SYSTEM_DELAY INT,
    SECURITY_DELAY INT,
    AIRLINE_DELAY INT,
    LATE_AIRCRAFT_DELAY INT,
    WEATHER_DELAY INT,

    CreatedDate DATETIME DEFAULT GETDATE(),
    LastUpdatedDate DATETIME NULL
);
USE HTTTKD_NDS;
GO
Delete from NDS_Flights;
select count(*) from NDS_Flights;
SELECT TOP 10 * FROM NDS_Flights;
select * from NDS_Flight_Detail;
-- Optional: index cho lookup theo business key
CREATE UNIQUE INDEX UX_NDS_Flights_BK ON NDS_Flights(business_key);
GO










