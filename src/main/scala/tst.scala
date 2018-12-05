import java.security.MessageDigest
import scala.util.hashing.MurmurHash3
import java.util.BitSet



object tst {
  def main(args: Array[String]): Unit = {
//    def md5(s: String) : Array[Byte]={
//      var ret = MessageDigest.getInstance("MD5").digest(s.getBytes)
//      ret
//    }
//
//
//    var v = md5("hellosdfsewefwefwefw")
//    for(i <- v){
//      println(i)
//    }
//
//    println("******")
//
//    var a = md5("applesfsefsfeww")
//    for(i <- a){
//      println(i)
//    }
//
//
//    println(md5("Hello"))

    val bitSetSize = 1 << 13
    val bitSet = new BitSet(bitSetSize)


    var res1 = MurmurHash3.stringHash("hello", 1)
    var res2 = MurmurHash3.stringHash("hello", 2)
    var res3 = MurmurHash3.stringHash("hello", 3)
    var res4 = MurmurHash3.stringHash("hello", 4)
    var res5 = MurmurHash3.stringHash("hello", 5)
    var res6 = MurmurHash3.stringHash("hello", 5)

    println(bitSetSize)
    println(bitSet.size())
    println(res1)
    println(res2)
    println(res3)
    println(res4)
    println(res5)
    println(res6)


  }
}
