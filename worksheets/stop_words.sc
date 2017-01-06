val STOP_WORDS = Seq[String]("kazalo", "...", "seznam", "literatura", "simboli").map(s => s"""-"$s"""").mkString(" AND ")
STOP_WORDS

"danes je bil lep dan ...... to so bile pikice".matches("[dan]")

"tole je ---------------------- test".matches(".*(\-\-\-|demo).*")