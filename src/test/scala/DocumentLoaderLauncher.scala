object DocumentLoaderLauncher extends App {
  System.setProperty("spark.master", "local[*]")
  var newArgs = Array("./data/files/*", "./data/documents")
  DocumentLoader.main(newArgs)
}
