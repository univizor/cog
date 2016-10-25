# cog

## Stages

1. PDF to pages ~> [TextSplitter](./src/main/scala/TextSplitter.scala).
2. Pages to sentences ~> [PagesProcessor](./src/main/scala/PagesProcessor.scala).
3. Detecting intersections ~> `Detector

## Development

```bash
sbt "run local[8] ./data/files/"

./bin/run_on_local_cluster.sh
./bin/run_spark-shell.sh
```

- [Oto Brglez](https://github.com/otobrglez/cog)
