CREATE TABLE [dbo].[EDL_HANA_REF_FEES_MASTER] (

	[FEE_CD] varchar(20) NOT NULL, 
	[FEE_CD_DESC] varchar(100) NULL, 
	[FEE_TYP_CD] varchar(20) NULL, 
	[CREATED_DT] datetime2(3) NULL, 
	[CREATED_BY] varchar(80) NULL, 
	[UPDATED_DT] datetime2(3) NULL, 
	[SOURCE_SYSTEM_CD] decimal(10,0) NULL
);