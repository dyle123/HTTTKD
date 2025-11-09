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
    
);
GO

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












