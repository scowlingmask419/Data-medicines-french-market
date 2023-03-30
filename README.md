# Projet Introduction à la Data Science

## Membres

XU Frédéric
CHEVREUIL Cédric
JAAFRI Amyr

## Introduction

Ce projet a été réalisé sur Databricks Community.

## Séance 1

### Question Q1: Motiver le format RDF sélectionné et le choix de l’abstraction Spark

Format sélectionné n-triplet
- Format très bien supporté par Spark
- déjà manipulé

Afin d'effectuer la conversion de rdf/xml en n-triples, nous avons choisi d'utilsier l'utilitaire rdf2rdf.
https://github.com/architolk/rdf2rdf

En effet, ce dernier permet d'effectuer simplement des conversion d'un model de RDF vers un autre.

Etant donné que les `RDD` sont conçu pour des jeux de données non-structurés, et que
l'utilisation de `DataSet` n'est utile que pour profiter du paradigme fonctionnel, nous avons choisi
d'utiliser l'abstraction `DataFrame`. Nous motivons ce choix pour deux raisons :

1. On souhaite appliquer un schéma à notre jeu de données
2. On souhaite profiter de l'optimisateur de requêtes `Catalyst`


### Question Q2: combien avez-vous de tuples dans votre résultat.

On à la fin nous obtenons 793591 tuples

## Séances 2 et 3

### Question Q3: en fonction de la répartition des données, est-il possible de supprimer le symbole ‘%’ de la colonne taux de remboursement. Présenter clairement votre méthode.

Avant toute chose, l'encodage du fichier n'était pas correct. En effet, les caractères accentués n'étaient pas représentés correctement. Avant d'importer le document dans Databricks et commencer à travailler dessus, nous avons dû en changer l'encodage, et ainsi passer du Windows 1252 à UTF-8. Pour cela nous avons utilisé la commande suivante:
```
iconv -f "windows-1252" -t "UTF-8" cisCip.txt > cisCip_utf-8.txt
```

Afin de garantir la qualité des données, nous avons dans un premier temps défini un schéma. Ce dernier nous permet d'assurer le respect des types, et des formats. Ainsi, dès lors que le schéma de données attends un long, toutes chaînes de caractères contenant une lettre ou un symbole sera marquée en erreur, et retirée du jeu de données. De la même manière, une date mal formée sera également retirée.

Une fois les données chargées, et le schéma appliqué, nous avons continué de nettoyer les données.

Nous avons commencé par vérifier la validité des cis, cip7 et cip13.
Ces champs nous permettant d'identifier les médicaments, ils doivent être non null.
À la suite de ce traitement, nous avons trouvé qu'un CIP7 était mal formé, et nous en avons supprimé la ligne.

Nous nous sommes ensuite intéressés aux prix, à savoir les colonnes prix de base, taux et prix final.
Cette fois-ci, avant de pouvoir contrôler la données, il nous a fallu la convertir en Double. Cette conversion n'a pas pu être automatisée à l'aide du schéma étant donné que son format n'est pas correct. En effet, des ',' sont utilisés à la place des '.'.
Nous avons donc utilisé `regex_replace` afin de corriger le format, et enchaîné avec une conversion. Si le cast n'était pas possible à cause d'un caractère alphanumérique, le champ est alors remplacé par un montant nul (0.0).
Suite à quoi nous avons retiré toutes les lignes comportant des montants négatifs, il y en avait 2.

Nous avons ensuite appliqué une correction sur le prix final sans retirer les lignes incorrecte. Ainsi, pour toutes les lignes comportant un prix de base et un taux, nous avons défini le prix final comme étant la somme du prix de base + taux.

De la même manière que pour remplacer les ',' par des '.',  est possible de supprimer le symbole '%' avec la méthode `regex_replace`. 

## Séances 4 et 5

### Question Q4: Combien de tuples garder vous depuis la source cisCompo?

Suite au traitement de filtrage que nous avons effectué à l'aide des fichiers cisCompo et Cis, nous obtenons à la fin un nombre total 23502 enregistrements sur les 32561 présent au départ.

### Question Q5: Proposez une modélisation (sans perte d’information) sous la forme d’un diagramme entité association des données des différentes tables.

<img src="./src/resources/images/ETL/Diagramme_ETL.png"
    alt="Diagramme ETL Q5"
    width="700"
/>

_Diagramme entité association des données des différentes tables_

En particulier, nous justifions le choix d'une association ternaire entre  `Drug`, `Ingredient` et `Form` à partir de l'exemple suivant :

Le `Doliprane` est un médicament qui contient du paracétamol (i.e., le principe actif), et plusieurs
substances pour rendre la présentation finale efficace.

Le `Dafalgan`, quant à lui, est un médicament contenant aussi du paracétamol, mais avec plusieurs autres substances.
Le choix des substances différentes, justifié pour plusieurs raisons propres au laboratoire
(e.g., coût de production, produire une autre présentation, etc.), va aboutir à une présentation différente ou pas.

Ainsi, prendre du `Doliprane` ou du `Dafalgan` représente fondamentalement la même chose bien que la présentation puisse
être différente. 

En outre, c'est la possibilité de plusieurs présentations (à l'exception des molécules brevetées), qui est derrière
l'existence des médicaments génériques.

### Question 6 Q6: idClass: 117, 372 et molécules: 94037 (nicotine), 2202 (paracétamol)

En préambule, on ne veut garder que les colonnes `idClass` et `codeSub` dont
les lignes contiennent certaines valeurs, soit :

* `idClass`: 117, 372 et `codeSub`: 94037 (nicotine), 2202 (paracétamol)
* `idClass`: 117, 372 et `codeSub`: 94037, 2202, 2092 (ibuprofène)
* `idClass`: 117, 372, 394 et `codeSub`: 94037, 2202, 2092, 1014
* `idClass`: 117, 372, 394, 204 et `codeSub`: 94037, 2202, 2092, 1014, 24245

D'où les lignes de codes suivantes :

    val df_filtered_id = df.filter(
        col("idclass") === 117 || 
        col("idclass") === 372 || 
        col("idclass") === 394 || 
        col("idclass") === 56 || 
        col("idclass") === 422 || 
        col("idclass") === 372
    )
    val df_filtered_code = df_filtered_id.filter(
        col("codeSub") === 94037 || 
        col("codeSub") === 2202 || 
        col("codeSub") === 2092 || 
        col("codeSub") === 1014 || 
        col("codeSub") === 24563 || 
        col("codeSub") === 23957 || 
        col("codeSub") === 30981
    )

Puis, à l'aide de la méthode `randomSplit`, on divise les données précédemments récoltés
pour nourrir notre jeu de d'entrainement et de test (resp., 70% pour `trainingData`, 30% pour `testData`),
comme suit :

    val Array(trainingData, testData) = data.randomSplit(Array(0.7, 0.3))

Ensuite, puisque `idClass` est notre label, on souhaite définir la colonne en tant que
variable de catégorie et produire un modèle (i.e., `Estimator`) 
pour notre jeu de données d'entrainement.

    val sIndexer = new StringIndexer()
        .setInputCol("idclass")
        .setOutputCol("idclassLabel")
        .fit(trainingData)

De plus, `Spark` n'acceptant que les colonnes vectorisées pour l'algorithme de notre modèle
et `codeSub` étant notre feature, nous devons transformer cette dernière en un vecteur binaire
(cf., `OneHotEncoder`).

    val ohe = new OneHotEncoder()
        .setInputCol("codeSub")
        .setOutputCol("codeOHE")

Par suite, nous avons besoin de fusionner notre label et notre feature (cf., `VectorAssembler`) en un
unique vecteur.

    val assembler = new VectorAssembler()
        .setInputCols(Array("codeOHE"))
        .setOutputCol("features")

Avant dernière étape, créer notre arbre de décision sur la variable cible `idClass`.

    val tree = new DecisionTreeClassifier()
        .setLabelCol("idclassLabel")

Enfin, nous construisons notre pipeline ML et les étapes qui lui sont afférentes 
(i.e., `Transformer`, `Transformer`, `Estimator`) 
par lequelles va transiter tout notre flux de données,
et aboutir à la génération du modèle de notre arbre de décision.

    val pipeline = new Pipeline()
        .setStages(Array(sIndexer, ohe, assembler, tree))

On construit notre modèle.

    val model = pipeline
        .fit(trainingData)

On génère les prédictions à partir du modèle.

    val predictions = model.transform(testData)

Etant donné, qu'il existe plusieurs `idClass`, on doit opérer à partir d'une classification par classes multiples
(i.e., `MulticlassClassificationEvaluator`).

    val evaluator = new MulticlassClassificationEvaluator()
        .setLabelCol("idclassLabel")
        .setPredictionCol("prediction")
        .setMetricName("accuracy")

Finalement, on évalue la précision obtenue.

    val accuracy = evaluator.evaluate(predictions)


<img src="./src/resources/images/ML/tree_q6.png"
    alt="tree Q6"
    width="700"
/>

_Représentation de l'arbre décisionnel de la qestion 6_

Pour cette question et les suivantes, nous avons pris une répartition données entraînement / données de test respectivement de 0.7, 0.3.

Nous obtenons une précision de 1.0 à notre jeu de test.

La précision de la prédiction générée à partir de notre jeu d'entraînement est également de 1.0.



### Q7 : idClass: 117, 372 et molécules: 94037, 2202, 2092 (ibuprofène)

<img src="./src/resources/images/ML/tree_q7.png"
    alt="tree Q7"
    width="700"
/>

_Représentation de l'arbre décisionnel de la question 7_

Nous obtenons une précision de 1.0 à notre jeu de test.

La précision de la prédiction générée à partir de notre jeu d'entraînement est également de 1.0.

### Q8 : idClass: 117, 372, 394 et molécules: 94037, 2202, 2092, 1014

<img src="./src/resources/images/ML/tree_q8.png"
    alt="tree Q8"
    width="700"
/>

_Représentation de l'arbre décisionnel de la question 8_

Nous obtenons une précision de 1.0 à notre jeu de test.

La précision de la prédiction générée à partir de notre jeu d'entraînement est également de 1.0.

### Q9 : idClass: 117, 372, 394, 204 et molécules: 94037, 2202, 2092, 1014, 24245
<img src="./src/resources/images/ML/tree_q9.png"
    alt="tree Q9"
    width="700"
/>

_Représentation de l'arbre décisionnel de la question 9_

Nous obtenons une précision de 0.9868421052631579 à notre jeu de test.

La précision de la prédiction générée à partir de notre jeu d'entraînement est quant à elle de 0.9577464788732394. On peut remarquer que celle-ci est inférieur à celle générée à partir de notre jeu de test.

### Q10
La précision de la prédiction du modèle peut diminuer si un même codeSub est partagé entre deux idClass différents. En effet, chaque fois que la machine rencontrera ce codeSub il y aura une ambiguïté quant à l'idClass à choisir.

On retrouve ce cas de figure pour les question 8 et 9. 

Pour la question 9, la précision était de 0.9868421052631579 car le codeSub 2092 est lié à deux idClass différents, avec respectivement une quantité de 9 et de 62. Ainsi, lorsque la machine rencontre ce codeSub elle ne sait pas à quel idClass il correspond et cela peut impliquer des erreurs. Comme la majeur partie des codeSub 2092 sont liées à l'idClass 117, cela implique un faible taux d'erreur qui représente ~0.02% d'erreur dans notre cas.

Si toutefois, on conserve une précision de 1.0 pour la question 8, malgré le fait que le codeSub 2202 soit partagé entre les idClass 394 et 117, c'est parce qu'il n'existe qu'un seul couple 2202/394. Ainsi, deux cas de figure sont possible. Soit le couple fait partie du jeu d'entraînement, et le modèle ne lui apporte que peu d'importance, soit il fait partie du jeu de test, la prédiction sera erroné pour ce couple, mais son impacte est minime.

### Q11 Proposer 3 paires molécule, idClass qui peuvent être ajoutées à l’expérimentation et qui maintiendront une haute qualité de la prédiction.
Afin de proposer des paires molécules, idClass à ajouter dans l'expérimentation, sans amoindrir la qualité de la prédiction, il est impératif de sélectionner des codeSub qui sont exclusif à cet idClass, ou tout du moins qu'il ne soit pas déjà lié à un idClass présent dans notre jeu de données. Inversement, il faut également faire attention à ce que l'idClass qu'on sélectionne n'ai pas de lien avec un codeSub déjà présent dans le jeu de données. Nous nous assurons ainsi qu'à chaque fois que la machine rencontrera un codeSub, il n'y ait aucune ambiguïté possible quant à son idClass.
En suivant ce principe, nous pouvons sélectionné:
 - idClass: 56, 422, 372
 - molécules: 24563, 23957, 30981

Afin de nous assurer de la conservation de la qualité, nous nous sommes basé sur le filtre de la question 8. Ainsi, si la qualité de la prédiction est de moins de 1, notre proposition est incorrecte.

En testant notre jeu de données nous obtenons bien une qualité de 1.0. Notre proposition est valide.

<img src="./src/resources/images/ML/tree_q11.png"
    alt="tree Q11"
    width="700"
/>

_Représentation de l'arbre décisionnel de la question 11_