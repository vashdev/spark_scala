package com.sparkbricks

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

object Solution {
  def main(args: Array[String]): Unit = {
    // val xnums = Array(2, 7, 11, 15) target=9
    val xnums = Array(3,2,4)
    val target = 6
    val x=twoSum(xnums,target)
    println("result)")
   x.foreach(println)
  }
    def twoSum(nums: Array[Int], target: Int): Array[Int] = {
      var a = ArrayBuffer[Int]()
      var map =  scala.collection.mutable.Map[Int, Int]()
      for (e <- 0 until(nums.length-1 )) {  map+=(nums(e) -> e)}
      var i = 0
      while ( {
        i < nums.length
      }) {
        val complement = target - nums(i)
        val x:Int=map.getOrElse(complement, 0)
        if (map.contains(complement)  &&  ( x !=i)  ) {
          a += map.getOrElse(complement, 0);
          a += i;

          //}}
        }
        {
          i += 1; i - 1
        }

      }

      return a.toArray


    }



}