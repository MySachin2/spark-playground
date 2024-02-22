package practice

import scala.annotation.tailrec


object NanaajiOps {

  def isPalindromeNaive(str: String): Boolean = {
    str.reverse == str
  }

  def isPalindrome(str: String): Boolean = {
    if str.isBlank then true
    else {
      val charAtStart = str.charAt(0)
      val charAtEnd = str.charAt(str.length - 1)
      val isSameChar = charAtStart == charAtEnd
      isSameChar && isPalindrome(str.slice(1, str.length - 1))
    }
  }

  def isPalindromeTR(str: String): Boolean = {
    @tailrec
    def loop(substr: String, acc: Boolean): Boolean = {
      if !acc then false
      else if substr.isBlank then true
      else {
        val charAtStart = substr.charAt(0)
        val charAtEnd = substr.charAt(substr.length - 1) // O(1) op on length
        val isSameChar = charAtStart == charAtEnd
        loop(substr.slice(1, substr.length - 1), acc && isSameChar)
      }
    }
    loop(str, true)
  }

  def findSmallestPositiveInt(xs: List[Int]): Int = {
    val sorted = xs.sortWith((a, b) => a < b)
    @tailrec
    def loop(xs: List[Int], result: Int): Int = xs match
      case Nil => result
      case head :: tail =>
        if result == head then loop(tail, result + 1)
        else result
    loop(sorted, 1)
  }

  def reverseStatement(str: String): String = {
    str
      .split(" ")
      .toList
      .reverse
      .mkString(" ")
  }


  def testCase(input: String, output: Any): Unit = {
    println(s"Output for ${input} => ${output}")
  }

  def main(args: Array[String]): Unit = {
    testCase("malayalam", isPalindrome("malayalam"))
    testCase("hello", isPalindrome("hello"))


    val unSorted = List(2, 3, 4, 8, 99, 1)
    testCase(unSorted.toString(), findSmallestPositiveInt(unSorted))

    val statement = "I am a programmer"
    testCase(statement, reverseStatement(statement))
  }
}
