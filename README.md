# cog

[![Build status][build-status-image]][build-status]

## Stages

1. [TextSplitter] "clans" and converts PDFs to documents with embedded sentences.
2. [SentenceMatcher] computes TF-IDF frequency and clusters documents with K-means II into
manageable clusters. Sentences withing same clusters are then compared for fuzzy text similarity.
Similarity can than be ad-hoc mined from `sentences` via some other tools,...

## Test suite

```bash
sbt -sbt-version 0.13.13 test
```

- [Oto Brglez](https://github.com/otobrglez/cog)


[LemmaGen]: http://lemmatise.ijs.si/
[TextSplitter]: ./src/main/scala/TextSplitter.scala
[SentenceMatcher]: ./src/main/scala/SentenceMatcher.scala
[build-status]: https://travis-ci.org/univizor/cog
[build-status-image]: https://travis-ci.org/univizor/cog.svg?branch=master
