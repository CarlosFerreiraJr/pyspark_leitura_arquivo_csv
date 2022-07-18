## Leitura de arquivo CSV usando o Python e o PySpark

Obs.: O arquivo original de entrada possui 108 Mb. Sendo assim, o mesmo foi editado para conter uma quantidade menor de linhas

### Passo a passo para instalação do PySpark
```
Instaladores:
  Anaconda: https://www.anaconda.com/products/individual
  Spark:    https://spark.apache.org/downloads.html
  Java JDK: https://www.oracle.com/java/technologies/javase-downloads.html   
  Hadoop:   https://hadoop.apache.org/release/3.2.2.html
  
Os programas Anaconda e Java JDK possuem instaladores próprios.
O Spark e o Hadoop devem ser descompactados nos diretórios, C:\Spark e C:\Hadoop respectivamente

Baixar os arquivos winutils.exe e winutils.pdb do Github abaixo e salvar na pasta C:\Hadoop
https://github.com/steveloughran/winutils/tree/master/hadoop-3.0.0/bin  

Tutorias de Instalação do Prof. Grimaldo Oliveira 
  https://www.youtube.com/watch?v=WpIDLm9ow2o    --- Aula 01 - Conceitos Básicos de PySpark
  https://www.youtube.com/watch?v=qtEZXXrGkFk    --- Aula 02 - Baixa dos softwares Apache SPARK, Anaconda e Java(JDK)
  https://www.youtube.com/watch?v=L3oWBs0aMvI    --- Aula 03 - Instalar os softwares Java JDK, Anaconda, Pyspark
  https://www.youtube.com/watch?v=WXnVnwy-Zko    --- Aula 04 - Configuração das variáveis de ambiente no Windows 
  https://www.youtube.com/watch?v=xKOSnC2pICo    --- Aula 05 - Executando os softwares Apache SPARK e PYSPARK

Outros tutorias:
https://www.youtube.com/channel/UCgDwTlMPBnX-6HCynVc6GnQ/videos
```

### Grava o resultado em um arquivo texto denominado analise_tweets.txt
```
import sys
file_path = 'analise_tweets.txt'
sys.stdout = open(file_path, "w")
```

### Declaração das bibliotecas do pyspark
```
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType, TimestampType
from pyspark.sql.types import ArrayType, DoubleType, BooleanType
from pyspark.sql.functions import col,array_contains, sum
from pyspark.sql.functions import split, trim, concat, lit
from pyspark.sql.functions import to_date, to_timestamp, substring, when, asc, desc
```

### Declaração da Sessão do spark
```
spark = SparkSession.builder.appName("DataFrame").getOrCreate()
sc = spark.sparkContext.setLogLevel('OFF')
```

### Criação de uma estrutura com os campos do arquivo
```
# Campos: id,tweet_text,tweet_date,sentiment,query_used
schema = StructType() \
    .add("id", StringType(), True) \
    .add("tweet_text", StringType(), True) \
    .add("tweet_date", StringType(), True) \
    .add("sentiment", StringType(), True) \
    .add("query_used", StringType(), True)
```

### Leitura do arquivo csv Tweets.csv
```
df = spark.read.format("csv") \
               .option("header", True) \
               .option("delimiter", ',') \
               .schema(schema) \
               .load("Tweets.csv")
```

### Conta a quantidade de linhas do dataframe df
```
qtd_tweet = df.count()
```

### Escreva a quantidade total de tweets lidos do dataframe 
```
strMsg = "\nQuantidade Total de Tweets: " + str(qtd_tweet)
print(strMsg)
```

###  Realiza um tratamento dos dados lidos
```
# Formato do Timestamp do Tweet existente no arquivo = Tue Aug 21 04:35:39 +0000 2018
# campo tweet_date
df1=df.withColumn('tmp_Ano', when(col('tweet_date').substr(27, 4) == "2018", 2018)
                            .when(col('tweet_date').substr(27, 4) == "2019", 2019)
                            .when(col('tweet_date').substr(27, 4) == "2020", 2020)
                            .otherwise(0)
                 )  \
      .withColumn('tmp_Mes',  when(col('tweet_date').substr(5, 3) == "Jan", 1)
                             .when(col('tweet_date').substr(5, 3) == "Feb", 2)
                             .when(col('tweet_date').substr(5, 3) == "Mar", 3)
                             .when(col('tweet_date').substr(5, 3) == "Apr", 4)
                             .when(col('tweet_date').substr(5, 3) == "May", 5)
                             .when(col('tweet_date').substr(5, 3) == "Jun", 6)
                             .when(col('tweet_date').substr(5, 3) == "Jul", 7)
                             .when(col('tweet_date').substr(5, 3) == "Aug", 8)
                             .when(col('tweet_date').substr(5, 3) == "Sep", 9)
                             .when(col('tweet_date').substr(5, 3) == "Oct", 10)
                             .when(col('tweet_date').substr(5, 3) == "Nov", 11)
                             .when(col('tweet_date').substr(5, 3) == "Dec", 12)
                             .otherwise(0)
                 )
```

### Converte o Mês e o Ano para inteiro e grava o resultdo no dataframe df2
```
df2 = df1.withColumn('Ano', col('tmp_Ano').cast("Integer"))  \
         .withColumn('Mês', col('tmp_Mes').cast("Integer"))
```

### Conta a quantidade de tweets e os agrupa por Mês e Ano, gravando os no dataframe df3
```
df3 = df2.groupBy(col('Ano'), col('Mês')) \
         .count()
```

### Seleciona os dados do dataframe df3 e grava no dataframe df4
```
df4 = df3.select(col('Ano').alias("Ano da publicação do Tweet"), \
           col('Mês').alias("Mês da publicação do Tweet"), \
           col('count').alias("Quantidade de Tweets"))
```

### Filtra os anos que não foram possíveis de identificar nas linhas de cada tweet
```
df_tweet_sem_ano = df4.filter((df3.Ano == 0))
```

### Soma o campo "Quantidade de Tweets" do dataframe df_tweet_sem_ano
```
df5 = df_tweet_sem_ano.select(sum(col('Quantidade de Tweets')))
qtd_tweet_sem_ano = df5.collect()[0][0]
```

### Escreva a quantidade de tweets sem identificação de data
```
strMsg = "Quantidade de Tweets sem identificação de data: " + str(qtd_tweet_sem_ano)
print(strMsg)
```

###  Filtra os anos que não foram possíveis de identificar nas linhas do tweet e escreva o resultado no arquivo
```
print("\nQuantidade de Tweets publicados por Mês/Ano")
df4.filter( (df3.Ano != 0) ).show()
```

### Identifica as hashtag que estão no Trending topics000000000000000000
```
df6 = df.filter(col("tweet_text").contains("#"))
df7 = df6.withColumn("tmp_hashtag_1", split(col("tweet_text"), "#").getItem(0)).withColumn("tmp_hashtag_2", split(col("tweet_text"), "#").getItem(1))
df8 = df7.select(col('tmp_hashtag_2'))
df9 = df8.withColumn("hashtag", split(col("tmp_hashtag_2"), " ").getItem(0)).withColumn("col_2", split(col("tmp_hashtag_2"), " ").getItem(1))
df10 = df9.select(trim(col('hashtag')).alias("hashtag"))
df11 = df10.filter(col('hashtag') != "")
df12 = df11.groupBy(col('hashtag')).count()
df13 = df12.orderBy(col('count').desc())
df13.select(concat(lit("#"), col('hashtag')).alias("#Hashtag"), col('count').alias("Quantidade")).show(10)
```


