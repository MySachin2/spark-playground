package practice

import grapple.json.{ *, given }

case class Person(name: String, age: Int)

object Person:
  given JsonInput[Person] with
    def read(json: JsonValue) = Person(json("name"), json("age"))
  
  given JsonOutput[Person] with
    def write(u: Person) = Json.obj("name" -> u.name, "age" -> u.age)