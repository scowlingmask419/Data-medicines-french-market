import org.apache.spark.sql.types.{LongType, DateType, FloatType, StringType,StructType,StructField}

val cisCipSchema = StructType(Array(
    StructField("CIS", LongType, false),
    StructField("CIP7", LongType, false),
    StructField("Libellé", StringType, false),
    StructField("Statut admin", StringType, false),
    StructField("AMM", StringType, false),
    StructField("Date", DateType, false),
    StructField("CIP13", LongType, false),
    StructField("Collectivités", StringType, false),
    StructField("Taux de remboursement", StringType),
    StructField("Prix de base", FloatType, false),
    StructField("Prix final", FloatType, false),
    StructField("Taxe", FloatType, false),
    StructField("Info", StringType)
))
