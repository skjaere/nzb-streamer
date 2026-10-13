package io.skjaere.nzbstreamer.nzb

import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Test
import java.io.File
import javax.xml.XMLConstants
import javax.xml.parsers.DocumentBuilder
import javax.xml.parsers.DocumentBuilderFactory
import javax.xml.parsers.ParserConfigurationException
import kotlin.test.assertEquals
import kotlin.test.assertFalse

/**
 * Android's DocumentBuilderFactory is not Xerces: setFeature throws
 * ParserConfigurationException for the Apache/SAX feature URIs NzbParser used to set,
 * which made every NZB fail to parse on Android.
 */
class NzbParserPortabilityTest {
    private val nzb = """
        <?xml version="1.0" encoding="UTF-8"?>
        <!DOCTYPE nzb PUBLIC "-//newzBin//DTD NZB 1.1//EN" "http://www.newzbin.com/DTD/nzb/nzb-1.1.dtd">
        <nzb xmlns="http://www.newzbin.com/DTD/2003/nzb">
          <file poster="p" date="1700000000" subject="&quot;a.mkv&quot; yEnc (1/1)">
            <groups><group>alt.binaries.test</group></groups>
            <segments><segment bytes="100" number="1">abc@test</segment></segments>
          </file>
        </nzb>
    """.trimIndent().toByteArray()

    @AfterEach
    fun restoreFactory() {
        System.clearProperty(FACTORY_PROPERTY)
    }

    @Test
    fun `parses with a DOM implementation that rejects Xerces-specific features`() {
        System.setProperty(FACTORY_PROPERTY, RejectingFeaturesFactory::class.java.name)
        val doc = NzbParser.parse(nzb)
        assertEquals(1, doc.files.size)
        assertEquals("abc@test", doc.files.single().segments.single().articleId)
    }

    @Test
    fun `does not resolve external entities`() {
        val secret = File.createTempFile("secret", ".txt").apply { writeText("TOP-SECRET"); deleteOnExit() }
        val xxe = """
            <?xml version="1.0"?>
            <!DOCTYPE nzb [ <!ENTITY xxe SYSTEM "${secret.toURI()}"> ]>
            <nzb xmlns="http://www.newzbin.com/DTD/2003/nzb">
              <file poster="&xxe;" date="1" subject="s">
                <groups><group>g</group></groups>
                <segments><segment bytes="1" number="1">id@x</segment></segments>
              </file>
            </nzb>
        """.trimIndent().toByteArray()
        val poster = runCatching { NzbParser.parse(xxe).files.single().poster }.getOrNull().orEmpty()
        assertFalse("TOP-SECRET" in poster, "external entity was resolved")
    }

    private companion object {
        const val FACTORY_PROPERTY = "javax.xml.parsers.DocumentBuilderFactory"
    }
}

/** Behaves like Android's factory: only secure-processing is a recognised feature. */
class RejectingFeaturesFactory : DocumentBuilderFactory() {
    private val delegate = DocumentBuilderFactory.newDefaultInstance()

    override fun setFeature(name: String, value: Boolean) {
        if (name != XMLConstants.FEATURE_SECURE_PROCESSING) throw ParserConfigurationException("Unsupported feature: $name")
        delegate.setFeature(name, value)
    }

    override fun getFeature(name: String): Boolean = delegate.getFeature(name)
    override fun setAttribute(name: String, value: Any?) = delegate.setAttribute(name, value)
    override fun getAttribute(name: String): Any? = delegate.getAttribute(name)
    override fun newDocumentBuilder(): DocumentBuilder {
        delegate.isNamespaceAware = isNamespaceAware
        delegate.isValidating = isValidating
        delegate.isExpandEntityReferences = isExpandEntityReferences
        return delegate.newDocumentBuilder()
    }
}
