Create DB if not exists HTTTKD;
go
use HTTTKD;
go

create schema stg;
create schema dw;
go

CREATE TABLE stg.Airlines(
  IATA_CODE NVARCHAR(8),
  AIRLINE   NVARCHAR(200)
);

CREATE TABLE stg.Airports (
    Airport_ID INT,                     -- cột đầu tiên: ID (20, 39, 51, …)
    IATA_CODE NVARCHAR(10),             -- mã sân bay 3 ký tự (ATL, BOS, …)
    AIRPORT NVARCHAR(255),              -- tên sân bay
    CITY NVARCHAR(255),                 -- thành phố
    STATE NVARCHAR(50),                 -- bang (GA, MA, TX, …)
    COUNTRY NVARCHAR(50),               -- quốc gia
    LATITUDE FLOAT,                     -- vĩ độ
    LONGITUDE FLOAT                     -- kinh độ
);



select* from stg.Airlines