CREATE TABLE [dbo].[EDL_PE_REPORT_K49] (

	[SRC_FILE_NAME] varchar(100) NULL, 
	[COMPANY] varchar(10) NULL, 
	[DIVISION] varchar(10) NULL, 
	[STATE] varchar(10) NULL, 
	[DEPARTMENT_NUMBER] varchar(10) NULL, 
	[ITEM_NUMBER] varchar(20) NULL, 
	[MEGA_CHANNEL] varchar(50) NULL, 
	[MEGA_CHANNEL_NAME] varchar(200) NULL, 
	[PRODUCT_GRADE] varchar(50) NULL, 
	[PG_DESCRIPTION] varchar(200) NULL, 
	[EXTRACT_DATE] bigint NULL, 
	[CREATED_DT] datetime2(3) NULL, 
	[CREATED_BY] varchar(100) NULL, 
	[UPDATED_DT] datetime2(3) NULL, 
	[UPDATED_BY] varchar(100) NULL, 
	[REC_COUNTER] bigint NULL
);