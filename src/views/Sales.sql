SELECT
  Sales.ID,
  Sales.Created AS Date,
  Sales.Amount,
  Sales.Currency,
  --
  Sales.Amount * ExchangeRates.Rate_To_AUD AS Amount_AUD,
  Sales.Amount * ExchangeRates.Rate_To_GBP AS Amount_GBP,
  Sales.Amount * ExchangeRates.Rate_To_USD AS Amount_USD,
  --
  Sales.Transaction_Type,
  Sales.Status,
  Sales.Invoice_Number,
  Sales.Invoice_Credit_Quantity,
  --
  Sales.Team_ID,
  Team.Name AS Team_Name,
  Team.Organisation_ID AS Organisation_ID,
  Org.Name AS Organisation_Name,
  Team.Operational_Office as Operational_Office,
  IF
    (Team.Name IS NULL, NULL, CONCAT(
      IF
        (Org.Name IS NULL
          OR Org.Name = Team.Name, "", CONCAT(Org.Name, " | ")), Team.Name, " | ", Sales.Team_ID)) AS Team_Key,
  IF (Org.Name IS NULL, NULL, CONCAT(Org.Name, " | ", Team.Organisation_ID)) AS Organisation_Key,
  --
  (Study.Askable_Plus IS TRUE)
  OR (Project.Askable_Plus IS TRUE) AS Askable_Plus,
  --
  User.Name AS User

FROM `askable-operations-test.operations_data_warehouse_test.sales` AS Sales
LEFT JOIN `askable-operations-test.operations_data_warehouse_test.teams` AS Team ON Team.ID = Sales.Team_ID
LEFT JOIN `askable-operations-test.operations_data_warehouse_test.organisations` AS Org ON Org.ID = Team.Organisation_ID
LEFT JOIN `askable-operations-test.operations_data_warehouse_test.studies` AS Study ON Study.ID = Sales.Study_ID
LEFT JOIN `askable-operations-test.operations_data_warehouse_test.projects` AS PROJECT ON Project.ID = Study.Project_ID
LEFT JOIN `askable-operations-test.operations_data_warehouse_test.users` AS User ON User.ID = Sales.User_ID
LEFT JOIN `askable-operations-test.operations_data_warehouse_test.exchange_rates` AS ExchangeRates ON ExchangeRates.ID = CONCAT(FORMAT_TIMESTAMP("%F", Sales.Created), "_", Sales.Currency)

WHERE Sales.Currency IS NOT NULL