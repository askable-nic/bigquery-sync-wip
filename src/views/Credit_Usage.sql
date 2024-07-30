SELECT
  CreditActivity.ID,
  CreditActivity.Created AS Date,
  CreditActivity.Credit_Amount AS Credits,
  CreditActivity.Type,
  CreditActivity.Credit_Refund_Type,
  --
  CreditActivity.Team_ID,
  Team.Name AS Team_Name,
  Team.Organisation_ID AS Organisation_ID,
  Org.Name AS Organisation_Name,
IF
  (Team.Name IS NULL, NULL, CONCAT(
    IF
      (Org.Name IS NULL
        OR Org.Name = Team.Name, "", CONCAT(Org.Name, " | ")), Team.Name, " | ", CreditActivity.Team_ID)) AS Team_Key,
IF
  (Org.Name IS NULL, NULL, CONCAT(Org.Name, " | ", Team.Organisation_ID)) AS Organisation_Key,
  --
  (Study.Askable_Plus IS TRUE)
  OR (Project.Askable_Plus IS TRUE) AS Askable_Plus,
  --
  User.Name AS User,
  AdminUser.Name AS Admin_User,
  CreditActivity.Comment
FROM
  `askable-operations-test.operations_data_warehouse_test.credit_activity` AS CreditActivity
LEFT JOIN `askable-operations-test.operations_data_warehouse_test.teams` AS Team ON Team.ID = CreditActivity.Team_ID
LEFT JOIN `askable-operations-test.operations_data_warehouse_test.organisations` AS Org ON Org.ID = Team.Organisation_ID
LEFT JOIN `askable-operations-test.operations_data_warehouse_test.studies` AS Study ON Study.ID = CreditActivity.Study_ID
LEFT JOIN `askable-operations-test.operations_data_warehouse_test.projects` AS PROJECT ON Project.ID = CreditActivity.Project_ID
LEFT JOIN `askable-operations-test.operations_data_warehouse_test.users` AS User ON User.ID = CreditActivity.User_ID
LEFT JOIN `askable-operations-test.operations_data_warehouse_test.users` AS AdminUser ON AdminUser.ID = CreditActivity.Admin_User_ID
WHERE
  CreditActivity.Usage IS TRUE