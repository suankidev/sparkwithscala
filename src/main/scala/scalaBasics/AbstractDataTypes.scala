package com.suanki
package scalaBasics

object AbstractDataTypes extends App{

  //abstract
  //can't be instantiated
  abstract class Animal{
    val creatureType:String
    def eat:Unit
  }

  class Dog extends Animal{
    override val creatureType: String = "Canine"

    override def eat: Unit = println("overidden method of animal")


  }

  //traits
  trait Carnivore{

    def eat(animal:Animal):Unit

  }

  class Crocodile extends Animal with Carnivore{
    override val creatureType: String = "croc"
    def eat=println("nomnom")

    override def eat(animal: Animal): Unit = println(s"I'm a croc I'm eating ${animal.creatureType}")
  }


  val dog1:Dog=new Dog

  val croc:Crocodile=new Crocodile

  croc.eat(dog1)

  //traits vs abstract

  /*

  1. traits do not have constructor parameters
  2. both abstract class and traits:
              can have abstract and non abstract fields
  3. multiple traits may be inherited by the same class
  4. traits = behaviour
     abstract = things


   */




}
