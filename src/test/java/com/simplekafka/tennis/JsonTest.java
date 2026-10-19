package com.simplekafka.tennis;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

class JsonTest {

    @Test
    void parsesNestedObjectsAndArrays() {
        Object v = Json.parse("{\"a\": [1, 2.5, -3e2], \"b\": {\"c\": true, \"d\": false, \"e\": null}, \"f\": \"x\"}");
        Map<?, ?> m = (Map<?, ?>) v;
        List<?> a = (List<?>) m.get("a");
        assertEquals(3, a.size());
        assertEquals(1.0, (Double) a.get(0), 0.0);
        assertEquals(2.5, (Double) a.get(1), 0.0);
        assertEquals(-300.0, (Double) a.get(2), 0.0);
        Map<?, ?> b = (Map<?, ?>) m.get("b");
        assertEquals(Boolean.TRUE, b.get("c"));
        assertEquals(Boolean.FALSE, b.get("d"));
        assertNull(b.get("e"));
        assertTrue(b.containsKey("e"));
        assertEquals("x", m.get("f"));
    }

    @Test
    void parsesEmptyContainersAndWhitespace() {
        assertEquals(0, ((List<?>) Json.parse("  [ ]  ")).size());
        assertEquals(0, ((Map<?, ?>) Json.parse("\n{ }\n")).size());
    }

    @Test
    void parsesEscapesAndUnicode() {
        assertEquals("a\"b\\c/d\n\t", Json.parse("\"a\\\"b\\\\c\\/d\\n\\t\""));
        assertEquals("éA", Json.parse("\"\\u00e9\\u0041\""));
    }

    @Test
    void keepsLargeNumbersInStringsExact() {
        String token = "25848910878437077501756509083939933374150392894827859467866515129858396269344";
        Map<?, ?> m = (Map<?, ?>) Json.parse("{\"t\":\"" + token + "\"}");
        assertEquals(token, m.get("t"));
    }

    @Test
    void rejectsBadInput() {
        assertThrows(IllegalArgumentException.class, () -> Json.parse("{\"a\":1} extra"));
        assertThrows(IllegalArgumentException.class, () -> Json.parse("\"unterminated"));
        assertThrows(IllegalArgumentException.class, () -> Json.parse("{a:1}"));
        assertThrows(IllegalArgumentException.class, () -> Json.parse("[1 2]"));
        assertThrows(IllegalArgumentException.class, () -> Json.parse("{\"a\":}"));
        assertThrows(IllegalArgumentException.class, () -> Json.parse(""));
        assertThrows(IllegalArgumentException.class, () -> Json.parse(null));
        assertThrows(IllegalArgumentException.class, () -> Json.parse("tru"));
    }

    @Test
    void quoteEscapesAndRoundTrips() {
        String nasty = "He said \"hi\"\\ \n tab\t ctrl\u0001 é";
        String quoted = Json.quote(nasty);
        assertTrue(quoted.startsWith("\"") && quoted.endsWith("\""));
        assertEquals(nasty, Json.parse(quoted));
    }
}
