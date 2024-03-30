-- Databricks notebook source
SET spark.databricks.delta.commitValidation.enabled = false;


-- COMMAND ----------

-- MAGIC %python
-- MAGIC dbutils.fs.rm('dbfs:/user/hive/warehouse/employeesm' , True)
-- MAGIC dbutils.fs.rm('dbfs:/user/hive/warehouse/employeeupdates', True)
-- MAGIC dbutils.fs.rm('dbfs:/user/hive/warehouse/targettable', True)
-- MAGIC dbutils.fs.rm('dbfs:/user/hive/warehouse/sourcetable', True)
-- MAGIC

-- COMMAND ----------

drop table if exists EmployeesM;
drop table if exists EmployeeUpdates;
drop table if exists targettable;
drop table if exists sourcetable;

-- COMMAND ----------

CREATE or replace TABLE EmployeesM (
    EmployeeID INT ,
    FirstName VARCHAR(50),
    LastName VARCHAR(50),
    Salary DECIMAL(10, 2)
);

-- Insert sample data
INSERT INTO EmployeesM VALUES (1, 'John', 'Doe', 50000.00);
INSERT INTO EmployeesM VALUES (2, 'Jane', 'Smith', 60000.00);
INSERT INTO EmployeesM VALUES (3, 'Bob', 'Johnson', 55000.00);


-- COMMAND ----------


CREATE or replace TABLE EmployeeUpdates (
    EmployeeID INT,
    FirstName VARCHAR(50),
    LastName VARCHAR(50),
    NewSalary DECIMAL(10, 2)
);

-- Insert sample data
INSERT INTO EmployeeUpdates VALUES (1, 'John', 'Doe', 52000.00);
INSERT INTO EmployeeUpdates VALUES (2, 'Jane', 'Smith', 62000.00);
INSERT INTO EmployeeUpdates VALUES (4, 'Alice', 'Williams', 58000.00);


-- COMMAND ----------

select *from EmployeesM

-- COMMAND ----------

select *from EmployeeUpdates

-- COMMAND ----------

merge into targettable
using sourcetable
on targettable.eno = sourcetable.eno
when matched then
     update set salary = newsalary +100
when not matched then 
 insert (eno,ename, esalary) value(sourcetable.eno, sourcetable.ename, sourcetable.esal)

-- COMMAND ----------

MERGE INTO EmployeesM
USING EmployeeUpdates 
ON EmployeesM.EmployeeID = EmployeeUpdates.EmployeeID
WHEN MATCHED THEN 
    UPDATE SET Salary = EmployeeUpdates.NewSalary
WHEN NOT MATCHED THEN
    INSERT (EmployeeID, FirstName, LastName, Salary)
    VALUES (EmployeeUpdates.EmployeeID, EmployeeUpdates.FirstName, EmployeeUpdates.LastName, EmployeeUpdates.NewSalary);

-- COMMAND ----------

-- MAGIC %python
-- MAGIC from delta.tables import *
-- MAGIC

-- COMMAND ----------

select *from EmployeesM

-- COMMAND ----------

-- MAGIC %python
-- MAGIC t4 = DeltaTable.forName(spark , 'EmployeesM').toDF()

-- COMMAND ----------

-- MAGIC %python
-- MAGIC t1 = DeltaTable.forName(spark , 'EmployeesM')
-- MAGIC t2 = DeltaTable.forName(spark , 'EmployeeUpdates').toDF()

-- COMMAND ----------

-- MAGIC %python
-- MAGIC t3 = t4.join(t2)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC t3.explain('extended')

-- COMMAND ----------

-- MAGIC %python
-- MAGIC t5 = t4.join(t2 , ["EmployeeID"] , 'inner')

-- COMMAND ----------

-- MAGIC %python
-- MAGIC t5.explain('codegen')

-- COMMAND ----------

-- MAGIC %python
-- MAGIC t5.explain('formatted')

-- COMMAND ----------

-- MAGIC %python
-- MAGIC t5.explain('extended')

-- COMMAND ----------

-- MAGIC %python
-- MAGIC t5.display()

-- COMMAND ----------

-- MAGIC %python
-- MAGIC from pyspark.sql.functions import broadcast
-- MAGIC
-- MAGIC t6 = t4.join(broadcast(t2) , ["EmployeeID"] , 'inner')

-- COMMAND ----------

-- MAGIC %python
-- MAGIC t6.explain('extended')

-- COMMAND ----------

-- MAGIC %python
-- MAGIC t1.alias("target").merge(
-- MAGIC     source=t2.alias("source"), condition="target.EmployeeID = source.EmployeeID"
-- MAGIC ).whenMatchedUpdate(set={"salary": "source.NewSalary"}).whenNotMatchedInsert(
-- MAGIC     values={
-- MAGIC         "EmployeeID": "source.EmployeeID",
-- MAGIC         "FirstName": "source.FirstName",
-- MAGIC         "LastName": "source.LastName",
-- MAGIC         "Salary": "source.NewSalary",
-- MAGIC     }
-- MAGIC ).execute()

-- COMMAND ----------

-- MAGIC %python
-- MAGIC t2.explain('extended')

-- COMMAND ----------



-- COMMAND ----------

MERGE INTO EmployeesM
USING EmployeeUpdates 
ON EmployeesM.EmployeeID = EmployeeUpdates.EmployeeID
WHEN MATCHED THEN 
    UPDATE SET Salary = EmployeeUpdates.NewSalary
WHEN NOT MATCHED THEN
    INSERT (EmployeeID, FirstName, LastName, Salary)
    VALUES (EmployeeUpdates.EmployeeID, EmployeeUpdates.FirstName, EmployeeUpdates.LastName, EmployeeUpdates.NewSalary);


-- COMMAND ----------

-- Target table
CREATE TABLE TargetTable (
    EmployeeID INT ,
    FirstName VARCHAR(50),
    LastName VARCHAR(50),
    Salary DECIMAL(10, 2),
    DeletedCount INT
);

-- Insert some sample data into the target table
INSERT INTO TargetTable VALUES
    (1, 'John', 'Doe', 50000.00, 0),
    (2, 'Jane', 'Smith', 60000.00, 0),
    (3, 'Bob', 'Johnson', 55000.00, 0);

-- Source table
CREATE TABLE SourceTable (
    EmployeeID INT ,
    FirstName VARCHAR(50),
    LastName VARCHAR(50),
    NewSalary DECIMAL(10, 2)
);

-- Insert some sample data into the source table
INSERT INTO SourceTable VALUES
    (1, 'John', 'Doe', 52000.00),
    (2, 'Jane', 'Smith', 62000.00),
    (4, 'Alice', 'Williams', 58000.00);


-- COMMAND ----------



-- COMMAND ----------

MERGE INTO TargetTable AS target
USING SourceTable AS source
ON target.EmployeeID = source.EmployeeID
WHEN MATCHED THEN 
    UPDATE SET target.Salary = source.NewSalary
WHEN NOT MATCHED BY SOURCE AND target.Salary < 60000.00 THEN
    UPDATE SET target.Salary = target.Salary * 1.1, target.DeletedCount = target.DeletedCount + 1
WHEN NOT MATCHED BY SOURCE AND target.Salary >= 60000.00 THEN
    DELETE;


-- COMMAND ----------

SELECT *FROM SourceTable

-- COMMAND ----------

SELECT *FROM TargetTable

-- COMMAND ----------

     |  DeltaMergeBuilder(spark: pyspark.sql.session.SparkSession, jbuilder: 'JavaObject')
     |  
     |  Builder to specify how to merge data from source DataFrame into the target Delta table.
     |  Use :py:meth:`delta.tables.DeltaTable.merge` to create an object of this class.
     |  Using this builder, you can specify any number of ``whenMatched``, ``whenNotMatched`` and
     |  ``whenNotMatchedBySource`` clauses. Here are the constraints on these clauses.
     |  
     |  - Constraints in the ``whenMatched`` clauses:
     |  
     |    - The condition in a ``whenMatched`` clause is optional. However, if there are multiple
     |      ``whenMatched`` clauses, then only the last one may omit the condition.
     |  
     |    - When there are more than one ``whenMatched`` clauses and there are conditions (or the lack
     |      of) such that a row satisfies multiple clauses, then the action for the first clause
     |      satisfied is executed. In other words, the order of the ``whenMatched`` clauses matters.
     |  
     |    - If none of the ``whenMatched`` clauses match a source-target row pair that satisfy
     |      the merge condition, then the target rows will not be updated or deleted.
     |  
     |    - If you want to update all the columns of the target Delta table with the
     |      corresponding column of the source DataFrame, then you can use the
     |      ``whenMatchedUpdateAll()``. This is equivalent to::
     |  
     |          whenMatchedUpdate(set = {
     |            "col1": "source.col1",
     |            "col2": "source.col2",
     |            ...    # for all columns in the delta table
     |          })
     |  
     |  - Constraints in the ``whenNotMatched`` clauses:
     |  
     |    - The condition in a ``whenNotMatched`` clause is optional. However, if there are
     |      multiple ``whenNotMatched`` clauses, then only the last one may omit the condition.
     |  
     |    - When there are more than one ``whenNotMatched`` clauses and there are conditions (or the
     |      lack of) such that a row satisfies multiple clauses, then the action for the first clause
     |      satisfied is executed. In other words, the order of the ``whenNotMatched`` clauses matters.
     |  
     |    - If no ``whenNotMatched`` clause is present or if it is present but the non-matching source
     |      row does not satisfy the condition, then the source row is not inserted.
     |  
     |    - If you want to insert all the columns of the target Delta table with the
     |      corresponding column of the source DataFrame, then you can use
     |      ``whenNotMatchedInsertAll()``. This is equivalent to::
     |  
     |          whenNotMatchedInsert(values = {
     |            "col1": "source.col1",
     |            "col2": "source.col2",
     |            ...    # for all columns in the delta table
     |          })
     |  
     |  - Constraints in the ``whenNotMatchedBySource`` clauses:
     |  
     |    - The condition in a ``whenNotMatchedBySource`` clause is optional. However, if there are
     |      multiple ``whenNotMatchedBySource`` clauses, then only the last ``whenNotMatchedBySource``
     |      clause may omit the condition.
     |  
     |    - Conditions and update expressions  in ``whenNotMatchedBySource`` clauses may only refer to
     |      columns from the target Delta table.
     |  
     |    - When there are more than one ``whenNotMatchedBySource`` clauses and there are conditions (or
     |      the lack of) such that a row satisfies multiple clauses, then the action for the first
     |      clause satisfied is executed. In other words, the order of the ``whenNotMatchedBySource``
     |      clauses matters.
     |  
     |    - If no ``whenNotMatchedBySource`` clause is present or if it is present but the
     |      non-matching target row does not satisfy any of the ``whenNotMatchedBySource`` clause
     |      condition, then the target row will not be updated or deleted.
     |  
     |  Example 1 with conditions and update expressions as SQL formatted string::
     |  
     |      deltaTable.alias("events").merge(
     |          source = updatesDF.alias("updates"),
     |          condition = "events.eventId = updates.eventId"
     |        ).whenMatchedUpdate(set =
     |          {
     |            "data": "updates.data",
     |            "count": "events.count + 1"
     |          }
     |        ).whenNotMatchedInsert(values =
     |          {
     |            "date": "updates.date",
     |            "eventId": "updates.eventId",
     |            "data": "updates.data",
     |            "count": "1",
     |            "missed_count": "0"
     |          }
     |        ).whenNotMatchedBySourceUpdate(set =
     |          {
     |            "missed_count": "events.missed_count + 1"
     |          }
     |        ).execute()
     |  
     |  Example 2 with conditions and update expressions as Spark SQL functions::
     |  
     |      from pyspark.sql.functions import *
     |  
     |      deltaTable.alias("events").merge(
     |          source = updatesDF.alias("updates"),
     |          condition = expr("events.eventId = updates.eventId")
     |        ).whenMatchedUpdate(set =
     |          {
     |            "data" : col("updates.data"),
     |            "count": col("events.count") + 1
     |          }
     |        ).whenNotMatchedInsert(values =
     |          {
     |            "date": col("updates.date"),
     |            "eventId": col("updates.eventId"),
     |            "data": col("updates.data"),
     |            "count": lit("1"),
     |            "missed_count": lit("0")
     |          }
     |        ).whenNotMatchedBySourceUpdate(set =
     |          {
     |            "missed_count": col("events.missed_count") + 1
     |          }
     |        ).execute()
     |  
     |  .. versionadded:: 0.4
     |  
     |  Methods defined here:
     |  
     |  __init__(self, spark: pyspark.sql.session.SparkSession, jbuilder: 'JavaObject')
     |      Initialize self.  See help(type(self)) for accurate signature.
     |  
     |  execute(self) -> None
     |      Execute the merge operation based on the built matched and not matched actions.
     |      
     |      See :py:class:`~delta.tables.DeltaMergeBuilder` for complete usage details.
     |      
     |      .. versionadded:: 0.4
     |  
     |  whenMatchedDelete(self, condition: Union[str, pyspark.sql.column.Column, NoneType] = None) -> 'DeltaMergeBuilder'
     |      Delete a matched row from the table only if the given ``condition`` (if specified) is
     |      true for the matched row.
     |      
     |      See :py:class:`~delta.tables.DeltaMergeBuilder` for complete usage details.
     |      
     |      :param condition: Optional condition of the delete
     |      :type condition: str or pyspark.sql.Column
     |      :return: this builder
     |      
     |      .. versionadded:: 0.4
     |  
     |  whenMatchedUpdate(self, condition: Union[str, pyspark.sql.column.Column, NoneType] = None, set: Optional[Dict[str, Union[str, pyspark.sql.column.Column]]] = None) -> 'DeltaMergeBuilder'
     |      Update a matched table row based on the rules defined by ``set``.
     |      If a ``condition`` is specified, then it must evaluate to true for the row to be updated.
     |      
     |      See :py:class:`~delta.tables.DeltaMergeBuilder` for complete usage details.
     |      
     |      :param condition: Optional condition of the update
     |      :type condition: str or pyspark.sql.Column
     |      :param set: Defines the rules of setting the values of columns that need to be updated.
     |                  *Note: This param is required.* Default value None is present to allow
     |                  positional args in same order across languages.
     |      :type set: dict with str as keys and str or pyspark.sql.Column as values
     |      :return: this builder
     |      
     |      .. versionadded:: 0.4
     |  
     |  whenMatchedUpdateAll(self, condition: Union[str, pyspark.sql.column.Column, NoneType] = None) -> 'DeltaMergeBuilder'
     |      Update all the columns of the matched table row with the values of the  corresponding
     |      columns in the source row. If a ``condition`` is specified, then it must be
     |      true for the new row to be updated.
     |      
     |      See :py:class:`~delta.tables.DeltaMergeBuilder` for complete usage details.
     |      
     |      :param condition: Optional condition of the insert
     |      :type condition: str or pyspark.sql.Column
     |      :return: this builder
     |      
     |      .. versionadded:: 0.4
     |  
     |  whenNotMatchedBySourceDelete(self, condition: Union[str, pyspark.sql.column.Column, NoneType] = None) -> 'DeltaMergeBuilder'
     |      Delete a target row that has no matches in the source from the table only if the given
     |      ``condition`` (if specified) is true for the target row.
     |      
     |      See :py:class:`~delta.tables.DeltaMergeBuilder` for complete usage details.
     |      
     |      :param condition: Optional condition of the delete
     |      :type condition: str or pyspark.sql.Column
     |      :return: this builder
     |      
     |      .. versionadded:: 2.3
     |  
     |  whenNotMatchedBySourceUpdate(self, condition: Union[str, pyspark.sql.column.Column, NoneType] = None, set: Optional[Dict[str, Union[str, pyspark.sql.column.Column]]] = None) -> 'DeltaMergeBuilder'
     |      Update a target row that has no matches in the source based on the rules defined by ``set``.
     |      If a ``condition`` is specified, then it must evaluate to true for the row to be updated.
     |      
     |      See :py:class:`~delta.tables.DeltaMergeBuilder` for complete usage details.
     |      
     |      :param condition: Optional condition of the update
     |      :type condition: str or pyspark.sql.Column
     |      :param set: Defines the rules of setting the values of columns that need to be updated.
     |                  *Note: This param is required.* Default value None is present to allow
     |                  positional args in same order across languages.
     |      :type set: dict with str as keys and str or pyspark.sql.Column as values
     |      :return: this builder
     |      
     |      .. versionadded:: 2.3
     |  
     |  whenNotMatchedInsert(self, condition: Union[str, pyspark.sql.column.Column, NoneType] = None, values: Optional[Dict[str, Union[str, pyspark.sql.column.Column]]] = None) -> 'DeltaMergeBuilder'
     |      Insert a new row to the target table based on the rules defined by ``values``. If a
     |      ``condition`` is specified, then it must evaluate to true for the new row to be inserted.
     |      
     |      See :py:class:`~delta.tables.DeltaMergeBuilder` for complete usage details.
     |      
     |      :param condition: Optional condition of the insert
     |      :type condition: str or pyspark.sql.Column
     |      :param values: Defines the rules of setting the values of columns that need to be updated.
     |                     *Note: This param is required.* Default value None is present to allow
     |                     positional args in same order across languages.
     |      :type values: dict with str as keys and str or pyspark.sql.Column as values
     |      :return: this builder
     |      
     |      .. versionadded:: 0.4
     |  
     |  whenNotMatchedInsertAll(self, condition: Union[str, pyspark.sql.column.Column, NoneType] = None) -> 'DeltaMergeBuilder'
     |      Insert a new target Delta table row by assigning the target columns to the values of the
     |      corresponding columns in the source row. If a ``condition`` is specified, then it must
     |      evaluate to true for the new row to be inserted.
     |      
     |      See :py:class:`~delta.tables.DeltaMergeBuilder` for complete usage details.
     |      
     |      :param condition: Optional condition of the insert
     |      :type condition: str or pyspark.sql.Column
     |      :return: this builder
     |      
     |      .. versionadded:: 0.4

-- COMMAND ----------

-- MAGIC %python
-- MAGIC df = spark.table('EmployeesM')

-- COMMAND ----------

-- Create EmployeesM table
CREATE or Replace TABLE EmployeesMC1 (
    EmployeeID INT ,
    FirstName VARCHAR(50),
    LastName VARCHAR(50),
    NewSalary DECIMAL(10, 2),
    Status VARCHAR(50)
);

-- Create EmployeeUpdates table
CREATE TABLE EmployeeUpdatesC1 (
    EmployeeID INT ,
    FirstName VARCHAR(50),
    LastName VARCHAR(50),
    NewSalary DECIMAL(10, 2)
);

-- COMMAND ----------



-- Insert data into EmployeesM table
INSERT INTO EmployeesMC1 VALUES
    (1, 'John', 'Doe', 50000.00, NULL),
    (2, 'Jane', 'Smith', 60000.00, NULL),
    (3, 'Bob', 'Johnson', 55000.00, NULL);

-- Insert data into EmployeeUpdates table
INSERT INTO EmployeeUpdatesC1 VALUES
    (1, 'John', 'Doe', 52000.00),
    (2, 'Jane', 'Smith', 58000.00), -- Decrease salary for Jane
    (4, 'Alice', 'Williams', 60000.00); -- New employee with higher salary


-- COMMAND ----------

merge into EmployeesMC1 using EmployeeUpdatesC1 on EmployeesMC1.EmployeeID = EmployeeUpdatesC1.EmployeeID
when Matched then 
     update set 
      EmployeeID = EmployeeUpdatesC1.EmployeeID,
      FirstName  = EmployeeUpdatesC1.FirstName,
      LastName   = EmployeeUpdatesC1.LastName,
      NewSalary  = EmployeeUpdatesC1.NewSalary
when Not Matched then
     insert (EmployeeID , FirstName , LastName , NewSalary) values( EmployeeUpdatesC1.EmployeeID,
                                                                    EmployeeUpdatesC1.FirstName,
                                                                    EmployeeUpdatesC1.LastName,
                                                                    EmployeeUpdatesC1.NewSalary)

