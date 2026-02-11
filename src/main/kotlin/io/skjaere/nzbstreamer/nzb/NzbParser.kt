package io.skjaere.nzbstreamer.nzb

import org.w3c.dom.Element
import org.w3c.dom.NodeList
import java.io.ByteArrayInputStream
import javax.xml.parsers.DocumentBuilderFactory

object NzbParser {
    private const val NZB_NAMESPACE = "http://www.newzbin.com/DTD/2003/nzb"

    fun parse(data: ByteArray): NzbDocument {
        val factory = DocumentBuilderFactory.newInstance().apply {
            isNamespaceAware = true
        }
        val builder = factory.newDocumentBuilder()
        val document = builder.parse(ByteArrayInputStream(data))
        document.documentElement.normalize()

        val fileElements = document.documentElement.getElementsByTagNameNS(NZB_NAMESPACE, "file")
        val files = (0 until fileElements.length).map { i ->
            parseFile(fileElements.item(i) as Element)
        }

        return NzbDocument(files)
    }

    private fun parseFile(element: Element): NzbFile {
        val poster = element.getAttribute("poster")
        val date = element.getAttribute("date").toLong()
        val subject = element.getAttribute("subject")

        val groupElements = element.getElementsByTagNameNS(NZB_NAMESPACE, "group")
        val groups = (0 until groupElements.length).map { i ->
            groupElements.item(i).textContent.trim()
        }

        val segmentElements = element.getElementsByTagNameNS(NZB_NAMESPACE, "segment")
        val segments = (0 until segmentElements.length).map { i ->
            parseSegment(segmentElements.item(i) as Element)
        }.sortedBy { it.number }

        return NzbFile(
            poster = poster,
            date = date,
            subject = subject,
            groups = groups,
            segments = segments
        )
    }

    private fun parseSegment(element: Element): NzbSegment {
        return NzbSegment(
            bytes = element.getAttribute("bytes").toLong(),
            number = element.getAttribute("number").toInt(),
            articleId = element.textContent.trim()
        )
    }
}
