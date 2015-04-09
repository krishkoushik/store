package com.treode.server

import org.scalatest._
import com.treode.server.SchemaParser._
import com.treode.twitter.finagle.http.{BadRequestException}

class SchemaParserSpec extends FreeSpec with Matchers {
  
  "When inputs are proper" - {
    "The parser should parse without errors if everything is correct" in {
      getSchema ("table table1 { id : 1 }")
    }}

  "When inputs are not proper" - {
    "The parser should throw BadRequestException if table name is incorrect" in {
  	  a [BadRequestException] should be thrownBy {
  	    getSchema ("table 4tableV { id : 1 }")
  	  }}
    "The parser should throw BadRequestException if table id is incorrect" in {
  	  a [BadRequestException] should be thrownBy {
  	    getSchema ("table tableV { id : 1 } table tableU { id : op }")	    
  	  }} 
  	"The parser should throw BadRequestException if syntax is incorrect" in {
  	  a [BadRequestException] should be thrownBy {
  	    getSchema ("tableU { id : 1 }")
  	  }}
  	"The parser should throw BadRequestException if colon is missing" in {
  	  a [BadRequestException] should be thrownBy {
  	    getSchema ("table table2 { id 1 }")
  	  }}  
  }}
    
