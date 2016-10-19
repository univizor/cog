
case class Person(name: String) {}


class PeopleRepository {
  private var list = List.empty[Person]

  def add(person: Person) = {
    list = list.+:(person)
  }

  def find(name: String): Option[Person] = list.find(_.name == name)

  def before(name: String) = {
    list.takeWhile(_.name != name)
  }
}


val repository = new PeopleRepository
repository.add(Person("Oto"))
repository.add(Person("Martina"))
repository.add(Person("Jack"))
repository.add(Person("Tinkara"))

repository.find("Demo")
repository.find("Oto")

repository.before("Jack")



