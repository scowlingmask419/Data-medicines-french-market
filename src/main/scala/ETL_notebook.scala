// Databricks notebook source
// DBTITLE 1,Loading data

// - QUESTION 1

//File location and type
var file_location = "/FileStore/tables/cipUCDfev23.nt"
var file_type = "csv"

// CSV options
// A nous de le fournir -> seulement trois colonnes (n-tuples)
var infer_schema = "false"
var first_row_is_header = "false"
var delimiter = " "

// - Regex pour l'extraction du cip13
val extractNumericRegex = "[0-9]{7,13}"

// The applied options are for CSV files. For other file types, these will be ignored.
var df = spark.read.format(file_type)
  .option("delimiter", delimiter)
  .load(file_location).toDF("Subject", "Predicate", "Object", "IGNORED")

// - Définition des prédicats associés aux objets voulus
val nom = "<http://www.w3.org/2000/01/rdf-schema#label>"
val labo = "<http://www.data.esante.gouv.fr/CIP/UCD/titulaire>"
val ephMRA = "<http://www.data.esante.gouv.fr/CIP/UCD/ephMRA>"
val ucd13 = "<http://www.w3.org/2004/02/skos/core#notation>"
//val ucd13 = "<http://www.data.esante.gouv.fr/CIP/UCD/composedBy>"

val laboDf = df.select(df("Subject").alias("cip13"), df("Object").alias("labo")).where(df("Predicate") === labo).toDF
  .withColumn("cip13", regexp_extract(col("cip13"), extractNumericRegex, 0).cast(LongType))
val nomDf = df.select(df("Subject").alias("cip13"), df("Object").alias("nom")).where(df("Predicate") === nom).toDF
  .withColumn("cip13", regexp_extract(col("cip13"), extractNumericRegex, 0).cast(LongType))
val ephmraDf = df.select(df("Subject").alias("cip13"), df("Object").alias("ephMRA")).where(df("Predicate") === ephMRA).toDF
  .withColumn("cip13", regexp_extract(col("cip13"), extractNumericRegex, 0).cast(LongType))
val ucd13Df = df.select(df("Subject").alias("cip13"), df("Object").alias("ucd13")).where(df("Predicate") === ucd13).toDF
  .withColumn("cip13", regexp_extract(col("cip13"), extractNumericRegex, 0).cast(LongType))
  .withColumn("ucd13", regexp_extract(col("ucd13"), extractNumericRegex, 0).cast(LongType))

// - Jointure naturelle sur "cip13"
val df2 = nomDf.join(laboDf, Seq("cip13"))
  .join(ephmraDf, Seq("cip13"))
  .join(ucd13Df, Seq("cip13"))

// - Problème : la jointure avec ephmraDf produit une table vide pour val ucd13 = "<http://www.data.esante.gouv.fr/CIP/UCD/composedBy>"
//   Il semblerait que l'ensemble des CIP13 de ucd13Df
//   est disjoint de l'ensemble des CIP13 de ephmraDf
// println(ucd13Df.count) // 48127
// println(ucd13Df.except(ephmraDf).count) // 48127
/*
val df2WithoutsEphmra = ucd13Df.join(nomDf, Seq("cip13"))
  .join(laboDf, Seq("cip13"))
  .join(ephmraDf, Seq("cip13"))
*/

df2.show(false)

// COMMAND ----------

// DBTITLE 1,Amyr cont'd
//import org.apache.spark.sql.{Row, SparkSession}

// File location and type
val file_location = "/FileStore/tables/cisCip_utf-8/cisCip_utf_8.txt"
var delimiter = "\t"

val regexExtractComma = "\\,"
val encoding = "UTF-8"
val charset = "iso-8859-15"

/*
Datetime type
TimestampType: Represents values comprising values of fields year, month, day, hour, minute, and second, with the session local time-zone. The timestamp value represents an absolute point in time.
DateType: Represents values comprising values of fields year, month and day, without a time-zone.
*/

val cisCipSchema = StructType(Array(
    StructField("CIS", IntegerType, false),
    StructField("CIP7", IntegerType, false),
    StructField("Libellé", StringType),
    StructField("Statut_admin", StringType),
    StructField("AMM", StringType),
    StructField("Date", StringType),
    StructField("CIP13", LongType, false),
    StructField("Collectivités", StringType),
    StructField("Taux_de_remboursement", StringType),
    StructField("Prix_de_base", StringType),
    StructField("Prix_final", StringType),
    StructField("Taxe", StringType),
    StructField("Info", StringType)
))

// Built-in functions : https://spark.apache.org/docs/2.3.0/api/sql/index.html

// - Commande pour convertir le fichier initial "cisCip.txt" en UTF-8 : iconv -f "windows-1252" -t "UTF-8" cisCip.txt > cisCip_utf-8.txt .
val cisCipDF = spark.read.option("delimiter", delimiter)
  .option("encoding", encoding)
  .option("charset", charset)
  .option("header", true)
  .option("mode", "DROPMALFORMED")
  //.option("badRecordsPath","/FileStore/tables/badRecordsPath")
  .schema(cisCipSchema)
  .csv(file_location)

val badRecords = spark.read.option("delimiter", delimiter)
  .option("encoding", encoding)
  .option("charset", charset)
  .option("header", true)
  .schema(cisCipSchema)
  .csv("/FileStore/tables/badRecordsPath")

//badRecords.show
  
// - Remplacement des "," par des "." ;
// - Conversion des String en Double ;
// - Correction du prix final (Arithmétique sur des types flottants peut-être pas la bonne façon de procéder ...) ;
val cisCipDF2 = cisCipDF.withColumn("Date", to_date(col("Date"), "dd/MM/yyyy"))
  .withColumn("Prix_de_base", when(col("Prix_de_base").isNotNull, regexp_replace(col("Prix_de_base"), regexExtractComma, ".").cast(DoubleType)).otherwise(null))
  .withColumn("Prix_final", when(col("Prix_final").isNotNull, regexp_replace(col("Prix_final"), regexExtractComma, ".").cast(DoubleType)).otherwise(null))
  .withColumn("Taxe", when(col("Taxe").isNotNull, regexp_replace(col("Taxe"), regexExtractComma, ".").cast(DoubleType)).otherwise(null))
  .withColumn("Prix_final", when(col("Taxe").isNotNull && col("Prix_de_base").isNotNull, col("Prix_de_base") + col("Taxe")).otherwise(null))
  .na.drop

// - Mise à null pour les prix en-dessous de 0 ;
// - Suppression des lignes où un enregistrement est null.
val cisCipDF3 = cisCipDF2.withColumn("Prix_de_base", when(col("Prix_de_base") >= 0, col("Prix_de_base")).otherwise(null))
  .withColumn("Prix_final", when(col("Prix_final") >= 0, col("Prix_final")).otherwise(null))
  .withColumn("Taxe", when(col("Taxe") >= 0, col("Taxe")).otherwise(null))
  .na.drop

//cisCipDF3.show
//cisCipDF3.select(col("AMM")).show(false)
                                     
val purgedDataCount = cisCipDF3.count // 789
// Récupération uniquement des lignes incohérentes/corrompues
val corruptedDataCount = cisCipDF.except(cisCipDF3).count // 20843


// COMMAND ----------

// DBTITLE 1,Séance 3: Chargement des données de cisCip.txt

// File location and type
val file_location = "/FileStore/tables/cisCip_utf-8/cisCip_utf_8.txt"
var delimiter = "\t"

val regexExtractComma = "\\,"
val encoding = "UTF-8"
val charset = "iso-8859-15"

/*
Datetime type
TimestampType: Represents values comprising values of fields year, month, day, hour, minute, and second, with the session local time-zone. The timestamp value represents an absolute point in time.
DateType: Represents values comprising values of fields year, month and day, without a time-zone.
*/

// Définition du schéma des données
val cisCipSchema = StructType(Array(
    StructField("CIS", IntegerType, false),
    StructField("CIP7", IntegerType, false),
    StructField("Libellé", StringType),
    StructField("Statut_admin", StringType),
    StructField("AMM", StringType),
    StructField("Date", DateType),
    StructField("CIP13", LongType, false),
    StructField("Collectivités", StringType),
    StructField("Taux_de_remboursement", StringType),
    StructField("Prix_de_base", StringType),
    StructField("Prix_final", StringType),
    StructField("Taxe", StringType),
    StructField("Info", StringType),
))

// Commande pour convertir le fichier initial "cisCip.txt" en UTF-8 : iconv -f "windows-1252" -t "UTF-8" cisCip.txt > cisCip_utf-8.txt .
val cisCipDF = spark.read.option("delimiter", delimiter)
  .option("encoding", encoding)
  .option("charset", charset)
  .schema(cisCipSchema)
  .option("badRecordsPath","/FileStore/tables/badRecordsPath")
  .option("dateformat", "dd/MM/yyyy")
  .csv(file_location)

val badRecords = spark.read.option("delimiter", delimiter)
  .option("encoding", encoding)
  .option("charset", charset)
  .option("header", true)
  .schema(cisCipSchema)
  .csv("/FileStore/tables/badRecordsPath")

// Valider le format de cis, cip7 et cip13 ne sont pas nuls et sont des entiers
// Les champs qui ne correspondent pas au schéma sont placé dans badReccords, puis changé en null
// Ici on a: java.lang.NumberFormatException: For input string: "381354B",68049089	381354B
// 381354B contient des lettres, ne respecte pas le schéma, ligne placée dans badReccords
// champ mis à null, levé en dessous
// Filtre CIS
println("Corruption CIS null")
val cisCipDF2_1 = cisCipDF.filter(col("CIS").isNotNull)
val nullCis = cisCipDF.select("CIP7", "CIP13").except(cisCipDF2_1.select("CIP7", "CIP13"))
nullCis.show()

// Filtre CIP7
println("Corruption CIP7 null")
val cisCipDF2_2 = cisCipDF2_1.filter(col("CIP7").isNotNull)
val nullCip7 = cisCipDF2_1.select("CIS", "CIP13").except(cisCipDF2_2.select("CIS", "CIP13"))
nullCip7.show()

// Filtre CIP13
println("Corruption CIP13 null")
val cisCipDF2_3 = cisCipDF2_2.filter(col("CIP13").isNotNull)
val nullCip13 = cisCipDF2_2.select("CIS", "CIP7").except(cisCipDF2_3.select("CIS", "CIP7"))
nullCip13.show()

val cisCipDF2 = cisCipDF2_3

// Remplacement des "," par des "." ;
// Conversion des String en Double ;
val cisCipDF3_1 = cisCipDF2
  .withColumn("Prix_de_base", when(col("Prix_de_base").isNotNull, regexp_replace(col("Prix_de_base"), regexExtractComma, ".").cast(DoubleType)).otherwise(0.00))
  .withColumn("Prix_final", when(col("Prix_final").isNotNull, regexp_replace(col("Prix_final"), regexExtractComma, ".").cast(DoubleType)).otherwise(0.00))
  .withColumn("Taxe", when(col("Taxe").isNotNull, regexp_replace(col("Taxe"), regexExtractComma, ".").cast(DoubleType)).otherwise(0.00))

// Enlève les valeurs négatives: Prix_de_base
println("Corruption prix de base négatifs")
val cisCipDF3_2 = cisCipDF3_1.filter(col("Prix_de_base").isNull || col("Prix_de_base") >= 0.0)
val negPrixBase = cisCipDF3_1.select("CIS", "CIP7", "CIP13").except(cisCipDF3_2.select("CIS", "CIP7", "CIP13"))
negPrixBase.show()

// Enlève les valeurs négatives: Taux
println("Corruption taux négatifs")
val cisCipDF3_3 = cisCipDF3_2.filter(col("Taxe").isNull || col("Taxe") >= 0.0)
val negTaxe = cisCipDF3_2.select("CIS", "CIP7", "CIP13").except(cisCipDF3_3.select("CIS", "CIP7", "CIP13"))
negTaxe.show()

// Enlève les valeurs négatives: Taux
println("Corruption prix finaux négatifs")
val cisCipDF3_4 = cisCipDF3_3.filter(col("Prix_final").isNull || col("Prix_final") >= 0.0)
val negPrixFinal = cisCipDF3_3.select("CIS", "CIP7", "CIP13").except(cisCipDF3_4.select("CIS", "CIP7", "CIP13"))
negPrixFinal.show()

// Correction du prix final uniquement lorsqu'il existe une valeur pour Taxe et Prix_de_base;
val cisCipDF3 = cisCipDF3_4.withColumn("Prix_final", when(col("Taxe") > 0.0 && col("Prix_de_base") > 0.0, myround(col("Prix_de_base") + col("Taxe"), 2)))
cisCipDF3.show()

/* 
// Verifications
val filtre = cisCipDF.select("CIS", "CIP7", "CIP13").except(cisCipDF3.select("CIS", "CIP7", "CIP13")).count // 20843

val a = cisCipDF.count()   // 20844
val b = cisCipDF2.count()  // 20843
val c = cisCipDF3.count()  // 20841

//cisCipDF2.filter(col("CIS") === 66770635).show()
//cisCipDF3.filter(col("CIS") === 66770635).show()
*/

// Q3: en fonction de la répartition des données, est-il possible de supprimer le symbole ‘%’ de la
// colonne taux de remboursement. Présenter clairement votre méthode. 
val cisCipQ3_4 = cisCipQ3_3.withColumn("Taux_de_remboursement", regexp_replace($"Taux_de_remboursement", "%", ""))
cisCipQ3_4.show()


// COMMAND ----------

// DBTITLE 1,Lecture de badRecords
// File location and type

val badPath_location_1 = "/FileStore/tables/badRecordsPath/20230311T205734/bad_records/part-00000-25756648-1970-48dc-a51b-ecb095caffad"
val badPath_location_2 = "/FileStore/tables/badRecordsPath/20230311T205734/bad_records/part-00000-2d5ca802-291d-4a3c-89c0-13e01bd62ee8"
val badPath_location_3 = "/FileStore/tables/badRecordsPath/20230311T205734/bad_records/part-00000-c0e3ecb6-8311-40db-9d5b-93f946d13578"

val badRecords1 = spark.read.json(badPath_location_1)
val badRecords2 = spark.read.json(badPath_location_2)
val badRecords3 = spark.read.json(badPath_location_3)

println("======= Begin =======")
badRecords1.select("reason", "record").take(100).foreach(println)
badRecords2.select("reason", "record").take(100).foreach(println)
badRecords3.select("reason", "record").take(100).foreach(println)
//badRecords4.select("reason").take(100).foreach(println)
//badRecords5.select("reason").take(100).foreach(println)
println("======= End =======")

// COMMAND ----------

// DBTITLE 1,Question 4: filtrage des données
//Vous devez effectuer plusieurs filtres:
// ne garder que le prix final
val cisCipQ3_1 = cisCipDF3.drop(col("Prix_de_base"))
val cisCipQ3_2 = cisCipQ3_1.drop(col("Taxe"))

// ne garder que les tuples dont le statut correspond à une présentation active
// ne garder que les médicaments dont l’état correspond à une déclaration de commercialisation
val cisCipQ3_3 = cisCipQ3_2.filter(col("Statut_admin") === "Présentation active" && col("AMM") === "Déclaration de commercialisation")

// Vérifications
//cisCipQ3_2.count() 20841
//cisCipQ3_3.count() 17420

// COMMAND ----------

// DBTITLE 1,Chargement des données cisCompo.txt
val cisComp = spark.read.parquet("/FileStore/tables/cisCompo")

// COMMAND ----------

// DBTITLE 1,Chargement des données de cis.zip
val q4 = spark.read.parquet("/FileStore/tables/q4").toDF("cis", "product")
//q4.show(false)

// COMMAND ----------

// DBTITLE 1,Q4: Sous-ensemble
/** Vous ne devez considérer qu’un sous-ensemble des compositions: celui comportant les médicaments qui ont
 * été   sélectionnés   depuis   le   fichier  cisCip.txt,   aux   médicaments   qui   ne   présentent   pas   une   voie
 * d’injection, perfusion au sens large (attention, réaliser une sélection fine) et avec une nature ‘SA’.  
 */
// Seulement les médicaments sélectionnés depuis le fihcier cisCip.txt

// médicaments   qui   ne   présentent   pas   une   voie d’injection, perfusion au sens large
val inject = cisComp.filter(not(col("ref").like("%inject%") || col("forme").like("%inject%")))
//println(inject.count())

val perfs = inject.filter(not(col("ref").like("%perf%") || col("forme").like("%perf%")))
//println(perfs.count())

val seringue = perfs.filter(not(col("ref").like("%seringue%") || col("forme").like("%seringue%")))
//println(seringue.count())

val stylo = seringue.filter(not(col("ref").like("%stylo%") || col("forme").like("%stylo%")))
//println(stylo.count())

val vaccin = stylo.filter(not(col("ref").like("%vaccin%") || col("forme").like("%vaccin%")))
//println(vaccin.count())

val cisComp_filtered = vaccin

// avec une nature 'SA'
val subset_SA = cisComp_filtered.filter(col("nature") === "SA")

val q4_join = subset_SA.join(q4, "cis")
val subset = cisCipQ3_4.join(q4_join, "cis")


// DBTITLE 1,Question 4/5 Séance 5
 val df = spark.read.json("/FileStore/tables/speclass.json")
  .withColumnRenamed("cip", "cip7")

// - Aucun enregistrement n'est composé d'un champ à null
val df2 = df.filter(col("cip7").isNotNull && col("idclass").isNotNull)

val df2_join = df2.join(subset, "cip7")

df2_join.write.parquet("/FileStore/tables/ml_data2")
