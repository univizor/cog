# cog

[![Build status][build-status-image]][build-status]

## Stages

1. [TextSplitter] converts PDFs to documents with sentacnes.
2. `Detector` detect document similarities.

## Test suite

```bash
sbt -sbt-version 0.13.13 test
```

- [Oto Brglez](https://github.com/otobrglez/cog)


[LemmaGen]: http://lemmatise.ijs.si/
[TextSplitter]: ./src/main/scala/TextSplitter.scala
[build-status]: https://travis-ci.org/univizor/cog
[build-status-image]: https://travis-ci.org/univizor/cog.svg?branch=master
