CREATE TABLE Agents
(
	AgentId INT IDENTITY(1, 1) NOT NULL,
	AgentName NVARCHAR(250) NOT NULL,
	AgentINN VARCHAR(19) NULL
)
GO

CREATE TABLE Deals
(
	DealId INT IDENTITY(1, 1) NOT NULL,
	DealNumber VARCHAR(29) NOT NULL,
	DealDate DATE NOT NULL,
	BuyerAgentId INT NOT NULL,
	SellerAgentId INT NOT NULL,
	BuyerWoodVolume real NOT NULL,
	SellerWoodVolume real NOT NULL
)
GO

ALTER TABLE Agents
ADD CONSTRAINT PK_Agents_AgentId PRIMARY KEY CLUSTERED (AgentId)
GO

ALTER TABLE Deals
ADD CONSTRAINT PK_Deals_DealId PRIMARY KEY CLUSTERED (DealId)
GO

ALTER TABLE Deals
WITH CHECK ADD CONSTRAINT FK_Deals_BuyerAgentId FOREIGN KEY (BuyerAgentId) 
REFERENCES Agents (AgentId)
ON UPDATE NO ACTION
ON DELETE NO ACTION
GO

ALTER TABLE Deals
WITH CHECK ADD CONSTRAINT FK_Deals_SellerAgentId FOREIGN KEY (SellerAgentId) 
REFERENCES Agents (AgentId)
ON UPDATE NO ACTION
ON DELETE NO ACTION
GO

CREATE UNIQUE INDEX INDEX_Agents_Name_INN on Agents(AgentName, AgentINN)
GO

--CREATE UNIQUE INDEX INDEX_Deals_Number_BuyerId_SellerId on Deals(DealNumber, BuyerAgentId, SellerAgentId)
--GO

USE LesegaisParsed
GO

CREATE TYPE MyData AS TABLE 
(
sellerName nvarchar(250),
sellerInn varchar(19),
buyerName nvarchar(250),
buyerInn varchar(19),
woodVolumeBuyer real,
woodVolumeSeller real,
dealDate Date,
dealNumber varchar(29),
__typename varchar(30)
); 
GO


--Процедура с простым INSERT:
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

CREATE PROC Proc_MyData
@MyData_table MyData READONLY
AS
BEGIN
SET NOCOUNT ON;

INSERT INTO Agents
SELECT DISTINCT sellerName, sellerInn FROM @MyData_table AS s
WHERE NOT EXISTS (SELECT [AgentName], [AgentINN] FROM Agents WHERE (Agents.[AgentName] = s.sellerName AND Agents.[AgentINN] = s.sellerInn))

INSERT INTO Agents
SELECT DISTINCT buyerName, buyerInn FROM @MyData_table AS s
WHERE NOT EXISTS (SELECT [AgentName], [AgentINN] FROM Agents WHERE (Agents.[AgentName] = s.buyerName AND Agents.[AgentINN] = s.buyerInn))

INSERT INTO Deals 
SELECT DISTINCT 
	dealNumber, 
	dealDate, 
	(SELECT AgentId FROM Agents WHERE AgentName = s.buyerName AND AgentINN = s.buyerInn), 
	(SELECT AgentId FROM Agents WHERE AgentName = s.sellerName AND AgentINN = s.sellerInn), 
	woodVolumeBuyer, 
	woodVolumeSeller  
FROM @MyData_table AS s
WHERE NOT EXISTS 
(SELECT DealNumber, BuyerAgentId, SellerAgentId FROM Deals 
WHERE (Deals.DealNumber = s.dealNumber 
AND Deals.BuyerAgentId = (SELECT AgentId FROM Agents WHERE AgentName = s.buyerName AND AgentINN = s.buyerInn)
AND Deals.SellerAgentId = (SELECT AgentId FROM Agents WHERE AgentName = s.sellerName AND AgentINN = s.sellerInn)))

RETURN @@ROWCOUNT

END

GO

--Процедура с MERGE, но надо доработать:
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

CREATE PROC Proc_MyData
@MyData_table MyData READONLY
AS
BEGIN
SET NOCOUNT ON;

MERGE INTO Agents WITH (HOLDLOCK) AS ag
USING @MyData_table AS tab
ON (ag.agentName = tab.sellerName AND ag.AgentINN = tab.sellerInn)
WHEN MATCHED THEN
UPDATE SET ag.agentName = tab.sellerName, ag.AgentINN = tab.sellerInn
WHEN NOT MATCHED THEN
INSERT (AgentName, AgentINN)
VALUES (tab.sellerName, tab.sellerInn);

MERGE INTO Agents WITH (HOLDLOCK) AS ag
USING @MyData_table tab
ON (ag.AgentName = tab.buyerName AND ag.AgentINN = tab.buyerInn)
WHEN MATCHED THEN
UPDATE SET ag.AgentName = tab.buyerName, ag.AgentINN = tab.buyerInn
WHEN NOT MATCHED THEN 
INSERT (AgentName, AgentINN)
VALUES (tab.buyerName, tab.buyerInn);

MERGE INTO Deals WITH (HOLDLOCK) AS dl
USING @MyData_table tab
ON (dl.DealNumber = tab.dealNumber 
AND dl.BuyerAgentId = (SELECT AgentId FROM Agents WHERE AgentName = tab.buyerName AND AgentINN = tab.buyerInn)
AND dl.SellerAgentId = (SELECT AgentId FROM Agents WHERE AgentName = tab.sellerName AND AgentINN = tab.sellerInn))
WHEN NOT MATCHED THEN 
INSERT (DealNumber, DealDate, BuyerAgentId, SellerAgentId, BuyerWoodVolume, SellerWoodVolume)
VALUES ( 
	dealNumber, 
	dealDate, 
	(SELECT AgentId FROM Agents WHERE AgentName = tab.buyerName AND AgentINN = tab.buyerInn), 
	(SELECT AgentId FROM Agents WHERE AgentName = tab.sellerName AND AgentINN = tab.sellerInn),	
	woodVolumeBuyer, 
	woodVolumeSeller);

END
GO