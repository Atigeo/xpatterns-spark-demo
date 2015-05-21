package com.xpatterns.enron.data

import java.io._
import java.nio.charset.{ Charset, CodingErrorAction }
import java.util.Date
import javax.mail.internet.MimeMessage
import javax.mail.{ Flags, Header, Multipart, Part, Session }
import com.sun.mail.util.BASE64DecoderStream
import org.apache.commons.io.IOUtils
import org.apache.log4j.Logger
import scala.io._
import scala.util.{ Try, Success, Failure }
import org.apache.commons.codec.binary.Hex
import com.esotericsoftware.kryo.serializers.FieldSerializer.Optional

case class Message(
  val owner: String,
  val subject: String,
  val from: Seq[String],
  val replyTo: Seq[String],
  val sentDate: String,
  val toRecipients: Seq[String],
  val ccRecipients: Seq[String],
  val bccRecipients: Seq[String],
  val receivedDate: String,
  val content: String,

  val messageId: String,
  val contentId: String,
  val attachments: Seq[Attachment],
  val isReplica: Boolean,
  val replicas: Seq[String]) extends Serializable with Equals {

  val recipients: Seq[String] = {
    toRecipients ++ ccRecipients ++ bccRecipients
  }

  var id: String = getSha256Hash(Some(owner))

  override def equals(other: Any) = {
    other match {
      case that: Message => that.canEqual(Message.this) && id == that.id && isReplica == that.isReplica && owner == that.owner && subject == that.subject && from == that.from && replyTo == that.replyTo && sentDate == that.sentDate && toRecipients == that.toRecipients && ccRecipients == that.ccRecipients && bccRecipients == that.bccRecipients && receivedDate == that.receivedDate && content == that.content && messageId == that.messageId && contentId == that.contentId && attachments == that.attachments
      case _ => false
    }
  }

  def canEqual(other: Any) = {
    other.isInstanceOf[Message]
  }

  override def hashCode() = {
    var result = 1
    result = result + computeHashcode(id, result)
    result = result + computeHashcode(owner, result)
    result = result + computeHashcode(subject, result)
    result = result + computeHashcode(from, result)
    result = result + computeHashcode(replyTo, result)
    result = result + computeHashcode(sentDate, result)
    result = result + computeHashcode(toRecipients, result)
    result = result + computeHashcode(ccRecipients, result)
    result = result + computeHashcode(bccRecipients, result)
    result = result + computeHashcode(receivedDate, result)
    result = result + computeHashcode(content, result)

    result = result + computeHashcode(messageId, result)
    result = result + computeHashcode(contentId, result)
    result = result + computeHashcode(attachments, result)

    result
  }

  def computeHashcode(field: Any, partialResult: Int): Int = {
    val prime = 41
    var result = 1
    Option(field) match {
      case None => result = prime * partialResult
      case _ => result = prime * partialResult + field.hashCode()
    }

    result
  }

  def getSha256Hash(owner: Option[String]) = {
    val shaInstance = java.security.MessageDigest.getInstance("SHA-256")
    
    val hash: Array[Byte] = shaInstance.digest((
      owner.getOrElse("") +
      subject +
      from +
      replyTo +
      sentDate +
      toRecipients +
      ccRecipients +
      bccRecipients +
      receivedDate +
      content +
      messageId).getBytes("UTF-8"))

    new String(Hex.encodeHex(hash))
  }
}

object Message {
  val log = Logger.getLogger(classOf[Message])
  private val session: Session = Session.getDefaultInstance(new java.util.Properties(), null)

  def readEml(inputStream: InputStream): Message = {
    val emlMsg = new MimeMessage(session, inputStream)

    val from = arrayToList(emlMsg.getFrom).map(_.toString())

    var allMessages: List[Message] = List[Message]()
    val contentAndAttachemts = getContentAndAttachments(emlMsg)
    from.foreach(owner => {
      val message = new Message(
        owner,
        nullableToString(emlMsg.getSubject),
        arrayToList(emlMsg.getFrom).map(_.toString()),
        arrayToList(emlMsg.getReplyTo).map(_.toString()),
        "",
        arrayToList(emlMsg.getRecipients(javax.mail.Message.RecipientType.TO)).map(_.toString()),
        arrayToList(emlMsg.getRecipients(javax.mail.Message.RecipientType.CC)).map(_.toString()),
        arrayToList(emlMsg.getRecipients(javax.mail.Message.RecipientType.BCC)).map(_.toString()),
        "",
        contentAndAttachemts._1,
        nullableToString(emlMsg.getMessageID),
        nullableToString(emlMsg.getContentID),
        contentAndAttachemts._2,
        false,
        List())
      allMessages = allMessages ++ List(message)
    })
    allMessages.last
  }

  private def getContentAndAttachments(eml: MimeMessage): (String, List[Attachment]) = {
    var resultedContent: String = ""
    var attachmets = List[Attachment]()
    val content = eml.getContent()

    if (content.isInstanceOf[Multipart]) {
      val multipart = content.asInstanceOf[Multipart]
      for (bodyPartIndex <- 0 until multipart.getCount()) {
        val bodyPart = multipart.getBodyPart(bodyPartIndex)

        val bodyPartName = bodyPart.getFileName()
        Option(bodyPartName) match {
          case None => {
            val part = bodyPart.asInstanceOf[Part]
            resultedContent = resultedContent + " " + getContentString(part)
          }
          case _ => {

            val part = bodyPart.asInstanceOf[Part]

            var attachmentContent: Array[Byte] = null
            try {
              attachmentContent = org.apache.commons.io.IOUtils.toByteArray(part.getInputStream())
              val attachment = new Attachment(bodyPartName, bodyPart.getDataHandler().getContentType(), attachmentContent, bodyPart.getSize())
              attachmets = attachmets :+ attachment
            } catch {

              case e: IOException => {
                log.warn("IOException encountered while processing attachment: ", e)
              }
              case e: Exception => {
                log.warn("Exception encountered while processing attachemnt : ", e)
              }
            }
          }
        }

      }
    } else {
      val filename = eml.getFileName()
      Option(filename) match {
        case None => {
          resultedContent = getContentString(eml)
        }
        case _ => {

          var attachmentContent: Array[Byte] = null
          try {
            attachmentContent = org.apache.commons.io.IOUtils.toByteArray(eml.getInputStream())
            val attachment = new Attachment(filename, nullableToString(eml.getContentType), attachmentContent, eml.getSize())
            attachmets = attachmets :+ attachment
          } catch {
            case e: IOException => {
              log.warn("IOException encountered while processing attachment: ", e)
            }
            case e: Exception => {
              log.warn("Exception encountered while processing attachemnt : ", e)
            }

          }
        }
      }
    }

    Tuple2(resultedContent, attachmets)
  }

  def getContentString(part: Part): String = {
    val codec = Codec("UTF-8")
    codec.onMalformedInput(CodingErrorAction.REPLACE)
    codec.onUnmappableCharacter(CodingErrorAction.REPLACE)
    return scala.io.Source.fromInputStream(part.getInputStream())(codec).mkString
  }

  private def nullableToString(a: Any): String = {
    if (a == null) ""
    else a.toString
  }

  private def arrayToList[T](as: Array[T]): List[T] = {
    if (as == null) List()
    else as.toList
  }
}