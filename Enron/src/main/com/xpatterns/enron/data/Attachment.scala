package com.xpatterns.enron.data

case class Attachment(name: String, contentType: String, content: Array[Byte], size: Long) extends Serializable with Equals {

  def canEqual(other: Any) = {
    other.isInstanceOf[Attachment]
  }

  override def equals(other: Any) = {
    other match {
      case that: Attachment => that.canEqual(Attachment.this) && name == that.name && contentType == that.contentType && content.deep == that.content.deep && size == that.size
      case _ => false
    }
  }

  override def hashCode() = {
    val prime = 41
    var result = 1
    Option(name) match {
      case None => result = prime * result + 0
      case _ => result = prime * result + name.hashCode()
    }
    Option(contentType) match {
      case None => result = prime * result + 0
      case _ => result = prime * result + contentType.hashCode()
    }
    Option(content) match {
      case None => result = prime * result + 0
      case _ => result = prime * result + content.hashCode()
    }
    Option(size) match {
      case None => result = prime * result + 0
      case _ => result = prime * result + size.hashCode()
    }
    result
  }

  override def toString = "name= " + name + ", contentType= " + contentType + ", content= " + content + ", size= " + size

}