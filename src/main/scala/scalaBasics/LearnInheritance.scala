package com.suanki
package scalaBasics

object LearnInheritance extends App {

  //private, protected only, no default method

  class Animal {

    val createureType="Wild"

    private def take = {
      println("test is private")
    }

    protected def take1(): Unit = {

      println("test is protected")

    }

     def eat = println("nomnom")


  }

  class Cat extends Animal {

   final def crunch = {
      println("curnch crunch")
    }

    val c: Cat = new Cat
    c.take1()
    //c.take() is not accessible b/c it's private

    c.crunch

  }


  //constuctor
  sealed class Person(name: String, age: Int) {

    def this(name: String) = this(name, 0)

    def bodyPart(A:Int,b:Int): Unit ={

    }

    def bodyPart(A:Int)={

    }


  }

  class Adult(name: String, age: Int, idCard: String) extends Person(name, age)

  class minor(name: String, age: Int) extends Person(name)


  //overriding
  class Dog extends Animal{

    override val createureType: String = "Domestic"

    override def eat: Unit = {
      super.eat
      println("this is from dog eat")
    }


  }


  val dog=new Dog

  dog.eat
  println(dog.createureType)

val a:Person=new Person("sujeet",22)

  a.bodyPart(11)
  a.bodyPart(34,22)


  val c:Animal=new Dog()

  c.eat


  //preventin overrides
  //1- user final on member
  //2- user final on class
  //3- sealed  to allow only in same file

}
