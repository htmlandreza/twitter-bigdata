#!/bin/bash

if [ -z $1 ]
then
	echo 'usage:' $0 'número_da_atividade'
	exit 0
else
    PACKAGE=twitter.bigdata
    URL_MASTER=spark://Andrezas-MacBook-Air.local:7077
    DADOS_DIR=/Users/andrezamoreira/Documents/streaming
    USA_TWEETS=$DADOS_DIR/usa_tweets
    USA_TWEETS_STREAM=$DADOS_DIR/usa_tweets_stream
    
    #ATIVIDADES
    case $1 in
        1)
            CLASS=$PACKAGE.MostUsedHashtags
            PARAMS=$USA_TWEETS_STREAM
            ;;
        2)
            # Hashtags Mais Usadas por Data
            CLASS=$PACKAGE.MostUsedHashtagsDate
            PARAMS=$USA_TWEETS_STREAM
            ;;
        3)
            CLASS=$PACKAGE.MostUsedHashtagsDateJSON
            PARAMS=$USA_TWEETS_STREAM
            ;;
        4)
            CLASS=$PACKAGE.MostUsedWords
            PARAMS=$USA_TWEETS_STREAM
            ;;
        *)
            echo 'Atividade inválido:' $1
            exit 1
            ;;
    esac
    JAR=target/scala-2.11/twitter-streaming_2.11-1.0.jar
    echo 'Executando atividade' $1 '...'
    #java -cp $CP $CLASS $PARAMS
    spark-submit --master $URL_MASTER --class $CLASS $JAR $PARAMS 
    exit 0
fi
