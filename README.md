# cog

## Stages

1. PDF to pages ~> [TextSplitter](./src/main/scala/TextSplitter.scala).
2. Words to lemmas ~> [Lemmatizer](./src/main/scala/nlp/Lemmatizer.scala) with [LemmaGen] and extensions.
3. Pages to sentences ~> [PagesProcessor](./src/main/scala/PagesProcessor.scala).
4. Detecting intersections ~> `Detector`



## Development

```bash
sbt "run local[8] ./data/files/"

./bin/run_on_local_cluster.sh
./bin/run_spark-shell.sh
```

## Test suite

```bash
sbt test
```

- [Oto Brglez](https://github.com/otobrglez/cog)


[LemmaGen]: http://lemmatise.ijs.si/