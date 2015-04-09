package com.treode.server
import com.treode.twitter.finagle.http.{BadRequestException}
import com.treode.async.misc.parseUnsignedLong
import scala.collection.mutable.HashMap
import org.parboiled2._
import scala.util.{ Success, Failure }

object SchemaParser {

  case class Table (name: String, id: Long)

  class SchemaParser (val input: ParserInput) extends Parser {
    implicit def wspStr(s: String): Rule0 = rule {
      zeroOrMore (WhiteSpace) ~ str(s) ~ zeroOrMore(WhiteSpace)
    }
    def TableDefinitions = rule { oneOrMore (TableDefinition) ~ EOI }
    def TableDefinition = rule { TableKeyword ~ Identifier ~ atomic("{") ~ IdField ~ atomic("}") ~> (Table(_,_)) }
    def IdField = rule {
      atomic("id") ~ atomic(":") ~ capture (Number) ~> { 
        parseUnsignedLong (_) match {
          case Some (v) => v
      }}
    }
    def TableKeyword = rule { str("table") ~ oneOrMore (WhiteSpace) }
    def Identifier = rule { capture (CharPredicate.Alpha ~ zeroOrMore (CharPredicate.AlphaNum | "-" | "_")) ~> (_.toString) }
    def Number = rule { (HexLongInt | OctLongInt | DecLongInt) }
    def HexLongInt = rule { "0x" ~ oneOrMore (CharPredicate.HexDigit) }
    def DecLongInt = rule { CharPredicate.Digit19 ~ zeroOrMore (CharPredicate.Digit) }
    def OctLongInt = rule { "0" ~ oneOrMore (CharPredicate ('0' to '7'))}
    def WhiteSpace = rule { anyOf (" \t \n") }
  }

  def getSchema(schema: String): Schema = {
    val map = new HashMap [String, Long]()
    val parser = new SchemaParser (schema) 
    parser.TableDefinitions.run() match {
      case Success (tableList: Seq [Table]) => {
        println (tableList)                     
        for (table <- tableList) {
          table match {
            case Table (name, id) => map += (name -> id)
        }}
      }
      case Failure (error: ParseError) => {
        val errorMsg = parser.formatError(error, new ErrorFormatter(showTraces = false))
        throw new BadRequestException (errorMsg)
      }
    }
    new Schema (map)
  }
}
