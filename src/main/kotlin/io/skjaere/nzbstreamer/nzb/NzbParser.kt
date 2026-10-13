package io.skjaere.nzbstreamer.nzb

import org.w3c.dom.Element
import org.w3c.dom.NodeList
import org.xml.sax.InputSource
import java.io.ByteArrayInputStream
import java.io.StringReader
import javax.xml.parsers.DocumentBuilderFactory
import javax.xml.parsers.ParserConfigurationException

object NzbParser {
    private const val NZB_NAMESPACE = "http://www.newzbin.com/DTD/2003/nzb"
    private val HARDENING_FEATURES = listOf(
        "http://apache.org/xml/features/nonvalidating/load-external-dtd",
        "http://xml.org/sax/features/external-general-entities",
        "http://xml.org/sax/features/external-parameter-entities",
    )

    fun parse(data: ByteArray): NzbDocument {
        val factory = DocumentBuilderFactory.newInstance().apply {
            isNamespaceAware = true
            // These feature URIs are Xerces/SAX specific. Other DOM implementations, such as
            // Android's, throw for features they do not know, so each one is best-effort.
            HARDENING_FEATURES.forEach { feature ->
                try {
                    setFeature(feature, false)
                } catch (_: ParserConfigurationException) {
                }
            }
        }
        val builder = factory.newDocumentBuilder().apply {
            // Portable guard against fetching DTDs and external entities (XXE), whichever
            // of the features above the platform honours.
            setEntityResolver { _, _ -> InputSource(StringReader("")) }
        }
        val document = builder.parse(ByteArrayInputStream(data))
        document.documentElement.normalize()

        val fileElements = document.documentElement.getElementsByTagNameNS(NZB_NAMESPACE, "file")
        val files = (0 until fileElements.length).map { i ->
            parseFile(fileElements.item(i) as Element)
        }

        return NzbDocument(files = files, password = parsePassword(document.documentElement))
    }

    /**
     * Extracts the archive password from `<head><meta type="password">…</meta></head>`.
     * The NZB v1.1 spec defines `meta` elements under `head` carrying typed key/value
     * pairs — `type="password"` is the de-facto standard for encrypted releases.
     * Returns null if no head/meta/password element is present.
     */
    private fun parsePassword(root: Element): String? {
        val heads = root.getElementsByTagNameNS(NZB_NAMESPACE, "head")
        for (i in 0 until heads.length) {
            val head = heads.item(i) as Element
            val metas = head.getElementsByTagNameNS(NZB_NAMESPACE, "meta")
            for (j in 0 until metas.length) {
                val meta = metas.item(j) as Element
                if (meta.getAttribute("type") == "password") {
                    val value = meta.textContent.trim()
                    if (value.isNotEmpty()) return value
                }
            }
        }
        return null
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
