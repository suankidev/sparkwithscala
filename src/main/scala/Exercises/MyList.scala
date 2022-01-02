package com.suanki
package Exercises

abstract class MyList {

  def head():Int
  def tail():MyList
  def isEmpty():Boolean
  def add(element:Int):MyList
  override def toString: String =
    ""

}

object MyListDemo extends App {


}
