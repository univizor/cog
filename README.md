# cog

[![Build status][build-status-image]][build-status]

## Stages

1. [TextSplitter] "cleans" and converts PDFs to documents with embedded sentences.
2. [SentenceMatcher] computes [TF-IDF frequency][tf-idf] and clusters documents with [K-means][k-means] into
manageable clusters. Sentences within the same clusters are then compared with their [Sørensen–Dice coefficient][fuzzy-similarity].

After that, data can be ad-hoc mined from `sentences` via some other tool,...

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
[tf-idf]: https://en.wikipedia.org/wiki/Tf%E2%80%93idf
[k-means]: https://en.wikipedia.org/wiki/K-means%2B%2B
[fuzzy-similarity]: https://en.wikipedia.org/wiki/S%C3%B8rensen%E2%80%93Dice_coefficient
