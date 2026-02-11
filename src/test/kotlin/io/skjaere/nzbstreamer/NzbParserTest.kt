package io.skjaere.nzbstreamer

import io.skjaere.nzbstreamer.nzb.NzbParser
import org.junit.jupiter.api.Test
import kotlin.test.assertEquals

class NzbParserTest {

    @Test
    fun `parse single file NZB`() {
        val xml = """
            <?xml version="1.0" encoding="UTF-8"?>
            <!DOCTYPE nzb PUBLIC "-//newzBin//DTD NZB 1.1//EN" "http://www.newzbin.com/DTD/nzb/nzb-1.1.dtd">
            <nzb xmlns="http://www.newzbin.com/DTD/2003/nzb">
                <file poster="user@example.com" date="1234567890" subject="test file (1/1)">
                    <groups>
                        <group>alt.binaries.test</group>
                    </groups>
                    <segments>
                        <segment bytes="500000" number="1">segment1@example.com</segment>
                    </segments>
                </file>
            </nzb>
        """.trimIndent()

        val nzb = NzbParser.parse(xml.toByteArray())

        assertEquals(1, nzb.files.size)
        val file = nzb.files[0]
        assertEquals("user@example.com", file.poster)
        assertEquals(1234567890L, file.date)
        assertEquals("test file (1/1)", file.subject)
        assertEquals(listOf("alt.binaries.test"), file.groups)
        assertEquals(1, file.segments.size)
        assertEquals(500000L, file.segments[0].bytes)
        assertEquals(1, file.segments[0].number)
        assertEquals("segment1@example.com", file.segments[0].articleId)
    }

    @Test
    fun `parse multi-file multi-segment NZB with sorted segments`() {
        val xml = """
            <?xml version="1.0" encoding="UTF-8"?>
            <nzb xmlns="http://www.newzbin.com/DTD/2003/nzb">
                <file poster="poster1" date="100" subject="file1">
                    <groups>
                        <group>alt.binaries.test</group>
                        <group>alt.binaries.misc</group>
                    </groups>
                    <segments>
                        <segment bytes="300" number="3">seg3@example.com</segment>
                        <segment bytes="100" number="1">seg1@example.com</segment>
                        <segment bytes="200" number="2">seg2@example.com</segment>
                    </segments>
                </file>
                <file poster="poster2" date="200" subject="file2">
                    <groups>
                        <group>alt.binaries.test</group>
                    </groups>
                    <segments>
                        <segment bytes="400" number="1">seg4@example.com</segment>
                    </segments>
                </file>
            </nzb>
        """.trimIndent()

        val nzb = NzbParser.parse(xml.toByteArray())

        assertEquals(2, nzb.files.size)

        val file1 = nzb.files[0]
        assertEquals(3, file1.segments.size)
        assertEquals(1, file1.segments[0].number)
        assertEquals(2, file1.segments[1].number)
        assertEquals(3, file1.segments[2].number)
        assertEquals(2, file1.groups.size)

        val file2 = nzb.files[1]
        assertEquals(1, file2.segments.size)
    }
}
