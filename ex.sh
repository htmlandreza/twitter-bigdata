#!/bin/bash

if [ -z $1 ]
then
	echo 'usage:' $0 'número_do_exemplo'
	exit 0
else
    PACKAGE=bigdata.mba
    URL_MASTER=spark://Andrezas-MacBook-Air.local:7077
    DADOS_DIR=/Users/andrezamoreira/Documents/streaming
    ATIVIDADES=$DADOS_DIR/atividades
    ATIVIDADES_STREAM=$DADOS_DIR/atividades_stream
    USA_TWEETS=$DADOS_DIR/usa_tweets
    USA_TWEETS_STREAM=$DADOS_DIR/usa_tweets_stream
    case $1 in
        1)
            CLASS=$PACKAGE.Ex01_NetworkWordCount
            PARAMS=`echo 'localhost 9999'`
            ;;
        2)
            CLASS=$PACKAGE.Ex02_DiretorioLocalFixo
            PARAMS=$ATIVIDADES
            ;;
        3)
            CLASS=$PACKAGE.Ex03_DiretorioLocalStream
            PARAMS=$ATIVIDADES_STREAM
            ;;
        4)
            CLASS=$PACKAGE.Ex04_Hashtags
            PARAMS=$USA_TWEETS_STREAM
            ;;
        5)
            CLASS=$PACKAGE.Ex05_HashtagsWindows
            PARAMS=$USA_TWEETS_STREAM
            ;;
        6)
            CLASS=$PACKAGE.Ex06_HashtagsWindowsArquivo
            PARAMS=$USA_TWEETS_STREAM
            ;;
        8)
            CLASS=$PACKAGE.Ex08_HashtagsWindowsArquivoCSV
            PARAMS=$USA_TWEETS_STREAM
            ;;
        *)
            echo 'Exemplo inválido:' $1
            exit 1
            ;;
    esac
    JAR=target/scala-2.11/twitter-streaming_2.11-1.0.jar
    echo 'Executando exemplo' $1 '...'
    #java -cp $CP $CLASS $PARAMS
    spark-submit --master $URL_MASTER --class $CLASS $JAR $PARAMS 
    exit 0
fi
