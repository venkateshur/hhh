// Function to update a struct to match the target schema
def updateStructType(struct: Column, currentSchema: StructType, targetSchema: StructType): Column = {
  val updatedFields = targetSchema.fields.map { targetField =>
    if (currentSchema.fieldNames.contains(targetField.name)) {
      struct.getField(targetField.name).cast(targetField.dataType).as(targetField.name)
    } else {
      lit(null).cast(targetField.dataType).as(targetField.name)
    }
  }
  struct(updatedFields: _*)
}

// Function to get ArrayType columns from a schema
def getArrayTypeColumns(schema: StructType): Seq[String] = {
  schema.fields.collect {
    case StructField(name, ArrayType(_: StructType, _), _) => name
  }
}

// Get ArrayType columns from the target schema
val arrayTypeColumns = getArrayTypeColumns(targetSchema)

// Update DataFrame columns based on the target schema
val updatedDf = arrayTypeColumns.foldLeft(df) { (tempDf, colName) =>
  val currentColType = tempDf.schema(colName).dataType
  val targetColType = targetSchema(colName).dataType

  (currentColType, targetColType) match {
    case (ArrayType(currentStructType: StructType, _), ArrayType(targetStructType: StructType, _)) =>
      tempDf.withColumn(colName, transform(col(colName), element => updateStructType(element, currentStructType, targetStructType)))
    case _ => tempDf
  }
}

updatedDf.printSchema()
updatedDf.show(false)
