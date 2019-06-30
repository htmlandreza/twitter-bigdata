# Trabalho de Big Data Science
API do Twiiter em Tempo Real para fazer Análises.

Trabalho desenvolvido para o MBA em Engenharia de Software/UFRJ.

[Link da apresentação do trabalho.](https://github.com/htmlandreza/twitter-bigdata/blob/master/BIG%20DATA.pdf)

## Hashtags por Intervalo de Data
Foi usado o __Simulador de Streaming__, passando os dados que estão setados na pasta _data_ e é possivel executar por meio do Terminal:

```
    sbt package

    sh twitter.sh *
```
### Alterar o * por um dos números abaixo (1, 2, 3 ou 4):
1. Contador de Hashtags
2. Contador de Hashtags por Data
3. Contador de Hashtags por Intervalo de Datas, que gera JSON para o Tableau
4. Contador de Palavras

### Links do Tableau para visualização:
- [Hashtags mais citadas por Jornais Brasileiros entre 19/06 à 25/06](https://public.tableau.com/profile/andreza.moreira#!/vizhome/Twitter_Hashtags_JornaisBrasileiros/Bolha)
- [Hashtags mais citadas entre os jornais brasileiros](https://public.tableau.com/profile/andreza.moreira#!/vizhome/Twitter_Hashtags_JornaisBrasileiros/Dias)
- [Hashtags mais usadas por jornais brasileiros](https://public.tableau.com/profile/andreza.moreira#!/vizhome/Twitter_Hashtags_JornaisBrasileiros/Mes)
