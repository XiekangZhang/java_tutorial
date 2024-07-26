package de.xiekang.talend;

import com.fasterxml.jackson.core.JsonProcessingException;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class JSONUtilsTest {

    final static String testJson = "{\n" +
            "    \"xpds_responseHeader\": {\n" +
            "        \"sessionID\": 7316553,\n" +
            "        \"sessionKey\": \"afd7cef3e09d34bc91a2c8f5a4effc815e231efe4290464dde7b7fbb310a088e\",\n" +
            "        \"version\": 0,\n" +
            "        \"timestamp\": \"2024-05-10T11:00:13.196+01:00\",\n" +
            "        \"errorType\": \"\",\n" +
            "        \"errorMessage\": \"\",\n" +
            "        \"errorMessageId\": \"\",\n" +
            "        \"ignoreWarning\": \"\",\n" +
            "        \"listHeader\": {\n" +
            "            \"lastRow\": \"1\",\n" +
            "            \"offset\": 0\n" +
            "        },\n" +
            "        \"meta\": \"\"\n" +
            "    },\n" +
            "    \"xpds_responseBody\": [\n" +
            "        {\n" +
            "            \"zaehlerStand\": \"6606\",\n" +
            "            \"ableseDatum\": \"2023-10-31\",\n" +
            "            \"ableseKennzeichen\": \"P\",\n" +
            "            \"ablesehinweis\": \"(P) Schätzung (programmseitig)\",\n" +
            "            \"setAktive\": \"0\",\n" +
            "            \"Status\": \"S\",\n" +
            "            \"Vorgang\": \"(27) Standmitteilung\",\n" +
            "            \"Anschlussnutzer\": \"Stahl,Sarah\",\n" +
            "            \"ErstelltDatum\": \"2023-11-07\",\n" +
            "            \"ErstelltZeit\": \"10:04:20+01:00\",\n" +
            "            \"AenderungDatum\": \"2023-11-13\",\n" +
            "            \"AenderungZeit\": \"10:41:31+01:00\",\n" +
            "            \"LetzteAenderung\": \"2024-04-23T13:20:57.867111+01:00\",\n" +
            "            \"Einheit\": \"\",\n" +
            "            \"Vorgangsdatum\": \"2023-10-31\",\n" +
            "            \"KonvSatzart\": 20,\n" +
            "            \"LaufendeNr\": 3,\n" +
            "            \"Satzart\": 27\n" +
            "        },\n" +
            "        {\n" +
            "            \"zaehlerStand\": \"7613\",\n" +
            "            \"ableseDatum\": \"2023-12-31\",\n" +
            "            \"ableseKennzeichen\": \"2\",\n" +
            "            \"ablesehinweis\": \"(2) Ablesung durch Kunden\",\n" +
            "            \"setAktive\": \"0\",\n" +
            "            \"Status\": \"\",\n" +
            "            \"Vorgang\": \"(27) Standmitteilung\",\n" +
            "            \"Anschlussnutzer\": \"Stahl,Sarah\",\n" +
            "            \"ErstelltDatum\": \"2024-01-02\",\n" +
            "            \"ErstelltZeit\": \"10:28:50+01:00\",\n" +
            "            \"AenderungDatum\": \"2024-01-02\",\n" +
            "            \"AenderungZeit\": \"10:28:50+01:00\",\n" +
            "            \"LetzteAenderung\": \"2024-04-23T13:20:57.867111+01:00\",\n" +
            "            \"Einheit\": \"\",\n" +
            "            \"Vorgangsdatum\": \"2023-12-31\",\n" +
            "            \"KonvSatzart\": 20,\n" +
            "            \"LaufendeNr\": 1,\n" +
            "            \"Satzart\": 27\n" +
            "        },\n" +
            "        {\n" +
            "            \"zaehlerStand\": \"8000\",\n" +
            "            \"ableseDatum\": \"2024-02-19\",\n" +
            "            \"ableseKennzeichen\": \"I\",\n" +
            "            \"ablesehinweis\": \"(I) per Internet mitgeteilt\",\n" +
            "            \"setAktive\": \"0\",\n" +
            "            \"Status\": \"S\",\n" +
            "            \"Vorgang\": \"(27) Standmitteilung\",\n" +
            "            \"Anschlussnutzer\": \"Stahl,Sarah\",\n" +
            "            \"ErstelltDatum\": \"2024-02-19\",\n" +
            "            \"ErstelltZeit\": \"09:12:17+01:00\",\n" +
            "            \"AenderungDatum\": \"2024-02-19\",\n" +
            "            \"AenderungZeit\": \"09:17:20+01:00\",\n" +
            "            \"LetzteAenderung\": \"2024-04-23T13:20:57.867111+01:00\",\n" +
            "            \"Einheit\": \"\",\n" +
            "            \"Vorgangsdatum\": \"2024-02-19\",\n" +
            "            \"KonvSatzart\": 20,\n" +
            "            \"LaufendeNr\": 2,\n" +
            "            \"Satzart\": 27\n" +
            "        },\n" +
            "        {\n" +
            "            \"zaehlerStand\": \"6727\",\n" +
            "            \"ableseDatum\": \"2023-11-13\",\n" +
            "            \"ableseKennzeichen\": \"2\",\n" +
            "            \"ablesehinweis\": \"(2) Ablesung durch Kunden\",\n" +
            "            \"setAktive\": \"0\",\n" +
            "            \"Status\": \"\",\n" +
            "            \"Vorgang\": \"(27) Standmitteilung\",\n" +
            "            \"Anschlussnutzer\": \"Stahl,Sarah\",\n" +
            "            \"ErstelltDatum\": \"2023-11-13\",\n" +
            "            \"ErstelltZeit\": \"10:40:54+01:00\",\n" +
            "            \"AenderungDatum\": \"2023-11-13\",\n" +
            "            \"AenderungZeit\": \"10:40:54+01:00\",\n" +
            "            \"LetzteAenderung\": \"2024-04-23T13:20:57.867111+01:00\",\n" +
            "            \"Einheit\": \"\",\n" +
            "            \"Vorgangsdatum\": \"2023-11-13\",\n" +
            "            \"KonvSatzart\": 20,\n" +
            "            \"LaufendeNr\": 1,\n" +
            "            \"Satzart\": 27\n" +
            "        },\n" +
            "        {\n" +
            "            \"zaehlerStand\": \"8000\",\n" +
            "            \"ableseDatum\": \"2024-02-20\",\n" +
            "            \"ableseKennzeichen\": \"I\",\n" +
            "            \"ablesehinweis\": \"(I) per Internet mitgeteilt\",\n" +
            "            \"setAktive\": \"0\",\n" +
            "            \"Status\": \"S\",\n" +
            "            \"Vorgang\": \"(27) Standmitteilung\",\n" +
            "            \"Anschlussnutzer\": \"Stahl,Sarah\",\n" +
            "            \"ErstelltDatum\": \"2024-02-20\",\n" +
            "            \"ErstelltZeit\": \"08:32:43+01:00\",\n" +
            "            \"AenderungDatum\": \"2024-02-20\",\n" +
            "            \"AenderungZeit\": \"08:33:07+01:00\",\n" +
            "            \"LetzteAenderung\": \"2024-04-23T13:20:57.867111+01:00\",\n" +
            "            \"Einheit\": \"\",\n" +
            "            \"Vorgangsdatum\": \"2024-02-20\",\n" +
            "            \"KonvSatzart\": 20,\n" +
            "            \"LaufendeNr\": 2,\n" +
            "            \"Satzart\": 27\n" +
            "        },\n" +
            "        {\n" +
            "            \"zaehlerStand\": \"8000\",\n" +
            "            \"ableseDatum\": \"2024-02-20\",\n" +
            "            \"ableseKennzeichen\": \"I\",\n" +
            "            \"ablesehinweis\": \"(I) per Internet mitgeteilt\",\n" +
            "            \"setAktive\": \"0\",\n" +
            "            \"Status\": \"S\",\n" +
            "            \"Vorgang\": \"(27) Standmitteilung\",\n" +
            "            \"Anschlussnutzer\": \"Stahl,Sarah\",\n" +
            "            \"ErstelltDatum\": \"2024-02-20\",\n" +
            "            \"ErstelltZeit\": \"08:33:17+01:00\",\n" +
            "            \"AenderungDatum\": \"2024-02-20\",\n" +
            "            \"AenderungZeit\": \"08:34:17+01:00\",\n" +
            "            \"LetzteAenderung\": \"2024-04-23T13:20:57.867111+01:00\",\n" +
            "            \"Einheit\": \"\",\n" +
            "            \"Vorgangsdatum\": \"2024-02-20\",\n" +
            "            \"KonvSatzart\": 20,\n" +
            "            \"LaufendeNr\": 2,\n" +
            "            \"Satzart\": 27\n" +
            "        },\n" +
            "        {\n" +
            "            \"zaehlerStand\": \"5937\",\n" +
            "            \"ableseDatum\": \"2023-08-08\",\n" +
            "            \"ableseKennzeichen\": \"2\",\n" +
            "            \"ablesehinweis\": \"(2) Ablesung durch Kunden\",\n" +
            "            \"setAktive\": \"0\",\n" +
            "            \"Status\": \"*\",\n" +
            "            \"Vorgang\": \"(20) Startsatz\",\n" +
            "            \"Anschlussnutzer\": \"Stahl,Sarah\",\n" +
            "            \"ErstelltDatum\": \"2023-09-11\",\n" +
            "            \"ErstelltZeit\": \"06:48:18+01:00\",\n" +
            "            \"AenderungDatum\": \"2023-11-12\",\n" +
            "            \"AenderungZeit\": \"10:29:15+01:00\",\n" +
            "            \"LetzteAenderung\": \"2024-04-23T13:20:57.867111+01:00\",\n" +
            "            \"Einheit\": \"\",\n" +
            "            \"Vorgangsdatum\": \"2023-08-08\",\n" +
            "            \"KonvSatzart\": 0,\n" +
            "            \"LaufendeNr\": 3,\n" +
            "            \"Satzart\": 20\n" +
            "        },\n" +
            "        {\n" +
            "            \"zaehlerStand\": \"8000\",\n" +
            "            \"ableseDatum\": \"2024-02-19\",\n" +
            "            \"ableseKennzeichen\": \"I\",\n" +
            "            \"ablesehinweis\": \"(I) per Internet mitgeteilt\",\n" +
            "            \"setAktive\": \"0\",\n" +
            "            \"Status\": \"S\",\n" +
            "            \"Vorgang\": \"(27) Standmitteilung\",\n" +
            "            \"Anschlussnutzer\": \"Stahl,Sarah\",\n" +
            "            \"ErstelltDatum\": \"2024-02-19\",\n" +
            "            \"ErstelltZeit\": \"11:02:09+01:00\",\n" +
            "            \"AenderungDatum\": \"2024-02-19\",\n" +
            "            \"AenderungZeit\": \"11:25:15+01:00\",\n" +
            "            \"LetzteAenderung\": \"2024-04-23T13:20:57.867111+01:00\",\n" +
            "            \"Einheit\": \"\",\n" +
            "            \"Vorgangsdatum\": \"2024-02-19\",\n" +
            "            \"KonvSatzart\": 20,\n" +
            "            \"LaufendeNr\": 2,\n" +
            "            \"Satzart\": 27\n" +
            "        },\n" +
            "        {\n" +
            "            \"zaehlerStand\": \"5937\",\n" +
            "            \"ableseDatum\": \"2023-08-08\",\n" +
            "            \"ableseKennzeichen\": \"2\",\n" +
            "            \"ablesehinweis\": \"(2) Ablesung durch Kunden\",\n" +
            "            \"setAktive\": \"0\",\n" +
            "            \"Status\": \"K\",\n" +
            "            \"Vorgang\": \"(20) Startsatz\",\n" +
            "            \"Anschlussnutzer\": \"Stahl,Sarah\",\n" +
            "            \"ErstelltDatum\": \"2023-09-11\",\n" +
            "            \"ErstelltZeit\": \"06:48:18+01:00\",\n" +
            "            \"AenderungDatum\": \"2023-09-11\",\n" +
            "            \"AenderungZeit\": \"06:48:18+01:00\",\n" +
            "            \"LetzteAenderung\": \"2024-04-23T13:20:57.867111+01:00\",\n" +
            "            \"Einheit\": \"\",\n" +
            "            \"Vorgangsdatum\": \"2023-08-08\",\n" +
            "            \"KonvSatzart\": 0,\n" +
            "            \"LaufendeNr\": 3,\n" +
            "            \"Satzart\": 20\n" +
            "        },\n" +
            "        {\n" +
            "            \"zaehlerStand\": \"5937\",\n" +
            "            \"ableseDatum\": \"2023-08-08\",\n" +
            "            \"ableseKennzeichen\": \"2\",\n" +
            "            \"ablesehinweis\": \"(2) Ablesung durch Kunden\",\n" +
            "            \"setAktive\": \"0\",\n" +
            "            \"Status\": \"K\",\n" +
            "            \"Vorgang\": \"(20) Startsatz\",\n" +
            "            \"Anschlussnutzer\": \"Stahl,Sarah\",\n" +
            "            \"ErstelltDatum\": \"2023-09-11\",\n" +
            "            \"ErstelltZeit\": \"06:48:18+01:00\",\n" +
            "            \"AenderungDatum\": \"2023-09-13\",\n" +
            "            \"AenderungZeit\": \"01:20:23+01:00\",\n" +
            "            \"LetzteAenderung\": \"2024-04-23T13:20:57.867111+01:00\",\n" +
            "            \"Einheit\": \"\",\n" +
            "            \"Vorgangsdatum\": \"2023-08-08\",\n" +
            "            \"KonvSatzart\": 0,\n" +
            "            \"LaufendeNr\": 3,\n" +
            "            \"Satzart\": 20\n" +
            "        },\n" +
            "        {\n" +
            "            \"zaehlerStand\": \"9000\",\n" +
            "            \"ableseDatum\": \"2024-04-10\",\n" +
            "            \"ableseKennzeichen\": \"2\",\n" +
            "            \"ablesehinweis\": \"(2) Ablesung durch Kunden\",\n" +
            "            \"setAktive\": \"1\",\n" +
            "            \"Status\": \"\",\n" +
            "            \"Vorgang\": \"(27) Standmitteilung\",\n" +
            "            \"Anschlussnutzer\": \"Stahl,Sarah\",\n" +
            "            \"ErstelltDatum\": \"2024-04-23\",\n" +
            "            \"ErstelltZeit\": \"13:20:57+01:00\",\n" +
            "            \"AenderungDatum\": \"2024-04-23\",\n" +
            "            \"AenderungZeit\": \"13:20:57+01:00\",\n" +
            "            \"LetzteAenderung\": \"2024-04-23T13:20:57.867111+01:00\",\n" +
            "            \"Einheit\": \"\",\n" +
            "            \"Vorgangsdatum\": \"2024-04-10\",\n" +
            "            \"KonvSatzart\": 20,\n" +
            "            \"LaufendeNr\": 1,\n" +
            "            \"Satzart\": 27\n" +
            "        },\n" +
            "        {\n" +
            "            \"zaehlerStand\": \"6533\",\n" +
            "            \"ableseDatum\": \"2023-10-31\",\n" +
            "            \"ableseKennzeichen\": \"P\",\n" +
            "            \"ablesehinweis\": \"(P) Schätzung (programmseitig)\",\n" +
            "            \"setAktive\": \"0\",\n" +
            "            \"Status\": \"S\",\n" +
            "            \"Vorgang\": \"(27) Standmitteilung(programmseitig)\",\n" +
            "            \"Anschlussnutzer\": \"Stahl,Sarah\",\n" +
            "            \"ErstelltDatum\": \"2023-11-07\",\n" +
            "            \"ErstelltZeit\": \"10:04:20+01:00\",\n" +
            "            \"AenderungDatum\": \"2023-11-07\",\n" +
            "            \"AenderungZeit\": \"10:04:20+01:00\",\n" +
            "            \"LetzteAenderung\": \"2024-04-23T13:20:57.867111+01:00\",\n" +
            "            \"Einheit\": \"\",\n" +
            "            \"Vorgangsdatum\": \"2023-10-31\",\n" +
            "            \"KonvSatzart\": 5,\n" +
            "            \"LaufendeNr\": 1,\n" +
            "            \"Satzart\": 27\n" +
            "        },\n" +
            "        {\n" +
            "            \"zaehlerStand\": \"6533\",\n" +
            "            \"ableseDatum\": \"2023-10-31\",\n" +
            "            \"ableseKennzeichen\": \"#\",\n" +
            "            \"ablesehinweis\": \"(#) Schätzung durch Vertrieb\",\n" +
            "            \"setAktive\": \"0\",\n" +
            "            \"Status\": \"*\",\n" +
            "            \"Vorgang\": \"(27) Standmitteilung\",\n" +
            "            \"Anschlussnutzer\": \"Stahl,Sarah\",\n" +
            "            \"ErstelltDatum\": \"2023-11-07\",\n" +
            "            \"ErstelltZeit\": \"10:04:20+01:00\",\n" +
            "            \"AenderungDatum\": \"2023-11-13\",\n" +
            "            \"AenderungZeit\": \"13:06:42+01:00\",\n" +
            "            \"LetzteAenderung\": \"2024-04-23T13:20:57.867111+01:00\",\n" +
            "            \"Einheit\": \"\",\n" +
            "            \"Vorgangsdatum\": \"2023-10-31\",\n" +
            "            \"KonvSatzart\": 20,\n" +
            "            \"LaufendeNr\": 3,\n" +
            "            \"Satzart\": 27\n" +
            "        },\n" +
            "        {\n" +
            "            \"zaehlerStand\": \"8827\",\n" +
            "            \"ableseDatum\": \"2024-03-18\",\n" +
            "            \"ableseKennzeichen\": \"2\",\n" +
            "            \"ablesehinweis\": \"(2) Ablesung durch Kunden\",\n" +
            "            \"setAktive\": \"0\",\n" +
            "            \"Status\": \"\",\n" +
            "            \"Vorgang\": \"(27) Standmitteilung\",\n" +
            "            \"Anschlussnutzer\": \"Stahl,Sarah\",\n" +
            "            \"ErstelltDatum\": \"2024-03-18\",\n" +
            "            \"ErstelltZeit\": \"15:29:15+01:00\",\n" +
            "            \"AenderungDatum\": \"2024-03-18\",\n" +
            "            \"AenderungZeit\": \"15:29:15+01:00\",\n" +
            "            \"LetzteAenderung\": \"2024-04-23T13:20:57.867111+01:00\",\n" +
            "            \"Einheit\": \"\",\n" +
            "            \"Vorgangsdatum\": \"2024-03-18\",\n" +
            "            \"KonvSatzart\": 20,\n" +
            "            \"LaufendeNr\": 1,\n" +
            "            \"Satzart\": 27\n" +
            "        }\n" +
            "    ]\n" +
            "}\n";
    final static String test2 = "[\n" +
            "        {\n" +
            "            \"zaehlerStand\": \"6606\",\n" +
            "            \"ableseDatum\": \"2023-10-31\",\n" +
            "            \"ableseKennzeichen\": \"P\",\n" +
            "            \"ablesehinweis\": \"(P) Schätzung (programmseitig)\",\n" +
            "            \"setAktive\": \"0\",\n" +
            "            \"Status\": \"S\",\n" +
            "            \"Vorgang\": \"(27) Standmitteilung\",\n" +
            "            \"Anschlussnutzer\": \"Stahl,Sarah\",\n" +
            "            \"ErstelltDatum\": \"2023-11-07\",\n" +
            "            \"ErstelltZeit\": \"10:04:20+01:00\",\n" +
            "            \"AenderungDatum\": \"2023-11-13\",\n" +
            "            \"AenderungZeit\": \"10:41:31+01:00\",\n" +
            "            \"LetzteAenderung\": \"2024-04-23T13:20:57.867111+01:00\",\n" +
            "            \"Einheit\": \"\",\n" +
            "            \"Vorgangsdatum\": \"2023-10-31\",\n" +
            "            \"KonvSatzart\": 20,\n" +
            "            \"LaufendeNr\": 3,\n" +
            "            \"Satzart\": 27\n" +
            "        },\n" +
            "        {\n" +
            "            \"zaehlerStand\": \"7613\",\n" +
            "            \"ableseDatum\": \"2023-12-31\",\n" +
            "            \"ableseKennzeichen\": \"2\",\n" +
            "            \"ablesehinweis\": \"(2) Ablesung durch Kunden\",\n" +
            "            \"setAktive\": \"0\",\n" +
            "            \"Status\": \"\",\n" +
            "            \"Vorgang\": \"(27) Standmitteilung\",\n" +
            "            \"Anschlussnutzer\": \"Stahl,Sarah\",\n" +
            "            \"ErstelltDatum\": \"2024-01-02\",\n" +
            "            \"ErstelltZeit\": \"10:28:50+01:00\",\n" +
            "            \"AenderungDatum\": \"2024-01-02\",\n" +
            "            \"AenderungZeit\": \"10:28:50+01:00\",\n" +
            "            \"LetzteAenderung\": \"2024-04-23T13:20:57.867111+01:00\",\n" +
            "            \"Einheit\": \"\",\n" +
            "            \"Vorgangsdatum\": \"2023-12-31\",\n" +
            "            \"KonvSatzart\": 20,\n" +
            "            \"LaufendeNr\": 1,\n" +
            "            \"Satzart\": 27\n" +
            "        },\n" +
            "        {\n" +
            "            \"zaehlerStand\": \"8000\",\n" +
            "            \"ableseDatum\": \"2024-02-19\",\n" +
            "            \"ableseKennzeichen\": \"I\",\n" +
            "            \"ablesehinweis\": \"(I) per Internet mitgeteilt\",\n" +
            "            \"setAktive\": \"0\",\n" +
            "            \"Status\": \"S\",\n" +
            "            \"Vorgang\": \"(27) Standmitteilung\",\n" +
            "            \"Anschlussnutzer\": \"Stahl,Sarah\",\n" +
            "            \"ErstelltDatum\": \"2024-02-19\",\n" +
            "            \"ErstelltZeit\": \"09:12:17+01:00\",\n" +
            "            \"AenderungDatum\": \"2024-02-19\",\n" +
            "            \"AenderungZeit\": \"09:17:20+01:00\",\n" +
            "            \"LetzteAenderung\": \"2024-04-23T13:20:57.867111+01:00\",\n" +
            "            \"Einheit\": \"\",\n" +
            "            \"Vorgangsdatum\": \"2024-02-19\",\n" +
            "            \"KonvSatzart\": 20,\n" +
            "            \"LaufendeNr\": 2,\n" +
            "            \"Satzart\": 27\n" +
            "        },\n" +
            "        {\n" +
            "            \"zaehlerStand\": \"6727\",\n" +
            "            \"ableseDatum\": \"2023-11-13\",\n" +
            "            \"ableseKennzeichen\": \"2\",\n" +
            "            \"ablesehinweis\": \"(2) Ablesung durch Kunden\",\n" +
            "            \"setAktive\": \"0\",\n" +
            "            \"Status\": \"\",\n" +
            "            \"Vorgang\": \"(27) Standmitteilung\",\n" +
            "            \"Anschlussnutzer\": \"Stahl,Sarah\",\n" +
            "            \"ErstelltDatum\": \"2023-11-13\",\n" +
            "            \"ErstelltZeit\": \"10:40:54+01:00\",\n" +
            "            \"AenderungDatum\": \"2023-11-13\",\n" +
            "            \"AenderungZeit\": \"10:40:54+01:00\",\n" +
            "            \"LetzteAenderung\": \"2024-04-23T13:20:57.867111+01:00\",\n" +
            "            \"Einheit\": \"\",\n" +
            "            \"Vorgangsdatum\": \"2023-11-13\",\n" +
            "            \"KonvSatzart\": 20,\n" +
            "            \"LaufendeNr\": 1,\n" +
            "            \"Satzart\": 27\n" +
            "        },\n" +
            "        {\n" +
            "            \"zaehlerStand\": \"8000\",\n" +
            "            \"ableseDatum\": \"2024-02-20\",\n" +
            "            \"ableseKennzeichen\": \"I\",\n" +
            "            \"ablesehinweis\": \"(I) per Internet mitgeteilt\",\n" +
            "            \"setAktive\": \"0\",\n" +
            "            \"Status\": \"S\",\n" +
            "            \"Vorgang\": \"(27) Standmitteilung\",\n" +
            "            \"Anschlussnutzer\": \"Stahl,Sarah\",\n" +
            "            \"ErstelltDatum\": \"2024-02-20\",\n" +
            "            \"ErstelltZeit\": \"08:32:43+01:00\",\n" +
            "            \"AenderungDatum\": \"2024-02-20\",\n" +
            "            \"AenderungZeit\": \"08:33:07+01:00\",\n" +
            "            \"LetzteAenderung\": \"2024-04-23T13:20:57.867111+01:00\",\n" +
            "            \"Einheit\": \"\",\n" +
            "            \"Vorgangsdatum\": \"2024-02-20\",\n" +
            "            \"KonvSatzart\": 20,\n" +
            "            \"LaufendeNr\": 2,\n" +
            "            \"Satzart\": 27\n" +
            "        },\n" +
            "        {\n" +
            "            \"zaehlerStand\": \"8000\",\n" +
            "            \"ableseDatum\": \"2024-02-20\",\n" +
            "            \"ableseKennzeichen\": \"I\",\n" +
            "            \"ablesehinweis\": \"(I) per Internet mitgeteilt\",\n" +
            "            \"setAktive\": \"0\",\n" +
            "            \"Status\": \"S\",\n" +
            "            \"Vorgang\": \"(27) Standmitteilung\",\n" +
            "            \"Anschlussnutzer\": \"Stahl,Sarah\",\n" +
            "            \"ErstelltDatum\": \"2024-02-20\",\n" +
            "            \"ErstelltZeit\": \"08:33:17+01:00\",\n" +
            "            \"AenderungDatum\": \"2024-02-20\",\n" +
            "            \"AenderungZeit\": \"08:34:17+01:00\",\n" +
            "            \"LetzteAenderung\": \"2024-04-23T13:20:57.867111+01:00\",\n" +
            "            \"Einheit\": \"\",\n" +
            "            \"Vorgangsdatum\": \"2024-02-20\",\n" +
            "            \"KonvSatzart\": 20,\n" +
            "            \"LaufendeNr\": 2,\n" +
            "            \"Satzart\": 27\n" +
            "        },\n" +
            "        {\n" +
            "            \"zaehlerStand\": \"5937\",\n" +
            "            \"ableseDatum\": \"2023-08-08\",\n" +
            "            \"ableseKennzeichen\": \"2\",\n" +
            "            \"ablesehinweis\": \"(2) Ablesung durch Kunden\",\n" +
            "            \"setAktive\": \"0\",\n" +
            "            \"Status\": \"*\",\n" +
            "            \"Vorgang\": \"(20) Startsatz\",\n" +
            "            \"Anschlussnutzer\": \"Stahl,Sarah\",\n" +
            "            \"ErstelltDatum\": \"2023-09-11\",\n" +
            "            \"ErstelltZeit\": \"06:48:18+01:00\",\n" +
            "            \"AenderungDatum\": \"2023-11-12\",\n" +
            "            \"AenderungZeit\": \"10:29:15+01:00\",\n" +
            "            \"LetzteAenderung\": \"2024-04-23T13:20:57.867111+01:00\",\n" +
            "            \"Einheit\": \"\",\n" +
            "            \"Vorgangsdatum\": \"2023-08-08\",\n" +
            "            \"KonvSatzart\": 0,\n" +
            "            \"LaufendeNr\": 3,\n" +
            "            \"Satzart\": 20\n" +
            "        },\n" +
            "        {\n" +
            "            \"zaehlerStand\": \"8000\",\n" +
            "            \"ableseDatum\": \"2024-02-19\",\n" +
            "            \"ableseKennzeichen\": \"I\",\n" +
            "            \"ablesehinweis\": \"(I) per Internet mitgeteilt\",\n" +
            "            \"setAktive\": \"0\",\n" +
            "            \"Status\": \"S\",\n" +
            "            \"Vorgang\": \"(27) Standmitteilung\",\n" +
            "            \"Anschlussnutzer\": \"Stahl,Sarah\",\n" +
            "            \"ErstelltDatum\": \"2024-02-19\",\n" +
            "            \"ErstelltZeit\": \"11:02:09+01:00\",\n" +
            "            \"AenderungDatum\": \"2024-02-19\",\n" +
            "            \"AenderungZeit\": \"11:25:15+01:00\",\n" +
            "            \"LetzteAenderung\": \"2024-04-23T13:20:57.867111+01:00\",\n" +
            "            \"Einheit\": \"\",\n" +
            "            \"Vorgangsdatum\": \"2024-02-19\",\n" +
            "            \"KonvSatzart\": 20,\n" +
            "            \"LaufendeNr\": 2,\n" +
            "            \"Satzart\": 27\n" +
            "        },\n" +
            "        {\n" +
            "            \"zaehlerStand\": \"5937\",\n" +
            "            \"ableseDatum\": \"2023-08-08\",\n" +
            "            \"ableseKennzeichen\": \"2\",\n" +
            "            \"ablesehinweis\": \"(2) Ablesung durch Kunden\",\n" +
            "            \"setAktive\": \"0\",\n" +
            "            \"Status\": \"K\",\n" +
            "            \"Vorgang\": \"(20) Startsatz\",\n" +
            "            \"Anschlussnutzer\": \"Stahl,Sarah\",\n" +
            "            \"ErstelltDatum\": \"2023-09-11\",\n" +
            "            \"ErstelltZeit\": \"06:48:18+01:00\",\n" +
            "            \"AenderungDatum\": \"2023-09-11\",\n" +
            "            \"AenderungZeit\": \"06:48:18+01:00\",\n" +
            "            \"LetzteAenderung\": \"2024-04-23T13:20:57.867111+01:00\",\n" +
            "            \"Einheit\": \"\",\n" +
            "            \"Vorgangsdatum\": \"2023-08-08\",\n" +
            "            \"KonvSatzart\": 0,\n" +
            "            \"LaufendeNr\": 3,\n" +
            "            \"Satzart\": 20\n" +
            "        },\n" +
            "        {\n" +
            "            \"zaehlerStand\": \"5937\",\n" +
            "            \"ableseDatum\": \"2023-08-08\",\n" +
            "            \"ableseKennzeichen\": \"2\",\n" +
            "            \"ablesehinweis\": \"(2) Ablesung durch Kunden\",\n" +
            "            \"setAktive\": \"0\",\n" +
            "            \"Status\": \"K\",\n" +
            "            \"Vorgang\": \"(20) Startsatz\",\n" +
            "            \"Anschlussnutzer\": \"Stahl,Sarah\",\n" +
            "            \"ErstelltDatum\": \"2023-09-11\",\n" +
            "            \"ErstelltZeit\": \"06:48:18+01:00\",\n" +
            "            \"AenderungDatum\": \"2023-09-13\",\n" +
            "            \"AenderungZeit\": \"01:20:23+01:00\",\n" +
            "            \"LetzteAenderung\": \"2024-04-23T13:20:57.867111+01:00\",\n" +
            "            \"Einheit\": \"\",\n" +
            "            \"Vorgangsdatum\": \"2023-08-08\",\n" +
            "            \"KonvSatzart\": 0,\n" +
            "            \"LaufendeNr\": 3,\n" +
            "            \"Satzart\": 20\n" +
            "        },\n" +
            "        {\n" +
            "            \"zaehlerStand\": \"9000\",\n" +
            "            \"ableseDatum\": \"2024-04-10\",\n" +
            "            \"ableseKennzeichen\": \"2\",\n" +
            "            \"ablesehinweis\": \"(2) Ablesung durch Kunden\",\n" +
            "            \"setAktive\": \"1\",\n" +
            "            \"Status\": \"\",\n" +
            "            \"Vorgang\": \"(27) Standmitteilung\",\n" +
            "            \"Anschlussnutzer\": \"Stahl,Sarah\",\n" +
            "            \"ErstelltDatum\": \"2024-04-23\",\n" +
            "            \"ErstelltZeit\": \"13:20:57+01:00\",\n" +
            "            \"AenderungDatum\": \"2024-04-23\",\n" +
            "            \"AenderungZeit\": \"13:20:57+01:00\",\n" +
            "            \"LetzteAenderung\": \"2024-04-23T13:20:57.867111+01:00\",\n" +
            "            \"Einheit\": \"\",\n" +
            "            \"Vorgangsdatum\": \"2024-04-10\",\n" +
            "            \"KonvSatzart\": 20,\n" +
            "            \"LaufendeNr\": 1,\n" +
            "            \"Satzart\": 27\n" +
            "        },\n" +
            "        {\n" +
            "            \"zaehlerStand\": \"6533\",\n" +
            "            \"ableseDatum\": \"2023-10-31\",\n" +
            "            \"ableseKennzeichen\": \"P\",\n" +
            "            \"ablesehinweis\": \"(P) Schätzung (programmseitig)\",\n" +
            "            \"setAktive\": \"0\",\n" +
            "            \"Status\": \"S\",\n" +
            "            \"Vorgang\": \"(27) Standmitteilung(programmseitig)\",\n" +
            "            \"Anschlussnutzer\": \"Stahl,Sarah\",\n" +
            "            \"ErstelltDatum\": \"2023-11-07\",\n" +
            "            \"ErstelltZeit\": \"10:04:20+01:00\",\n" +
            "            \"AenderungDatum\": \"2023-11-07\",\n" +
            "            \"AenderungZeit\": \"10:04:20+01:00\",\n" +
            "            \"LetzteAenderung\": \"2024-04-23T13:20:57.867111+01:00\",\n" +
            "            \"Einheit\": \"\",\n" +
            "            \"Vorgangsdatum\": \"2023-10-31\",\n" +
            "            \"KonvSatzart\": 5,\n" +
            "            \"LaufendeNr\": 1,\n" +
            "            \"Satzart\": 27\n" +
            "        },\n" +
            "        {\n" +
            "            \"zaehlerStand\": \"6533\",\n" +
            "            \"ableseDatum\": \"2023-10-31\",\n" +
            "            \"ableseKennzeichen\": \"#\",\n" +
            "            \"ablesehinweis\": \"(#) Schätzung durch Vertrieb\",\n" +
            "            \"setAktive\": \"0\",\n" +
            "            \"Status\": \"*\",\n" +
            "            \"Vorgang\": \"(27) Standmitteilung\",\n" +
            "            \"Anschlussnutzer\": \"Stahl,Sarah\",\n" +
            "            \"ErstelltDatum\": \"2023-11-07\",\n" +
            "            \"ErstelltZeit\": \"10:04:20+01:00\",\n" +
            "            \"AenderungDatum\": \"2023-11-13\",\n" +
            "            \"AenderungZeit\": \"13:06:42+01:00\",\n" +
            "            \"LetzteAenderung\": \"2024-04-23T13:20:57.867111+01:00\",\n" +
            "            \"Einheit\": \"\",\n" +
            "            \"Vorgangsdatum\": \"2023-10-31\",\n" +
            "            \"KonvSatzart\": 20,\n" +
            "            \"LaufendeNr\": 3,\n" +
            "            \"Satzart\": 27\n" +
            "        },\n" +
            "        {\n" +
            "            \"zaehlerStand\": \"8827\",\n" +
            "            \"ableseDatum\": \"2024-03-18\",\n" +
            "            \"ableseKennzeichen\": \"2\",\n" +
            "            \"ablesehinweis\": \"(2) Ablesung durch Kunden\",\n" +
            "            \"setAktive\": \"0\",\n" +
            "            \"Status\": \"\",\n" +
            "            \"Vorgang\": \"(27) Standmitteilung\",\n" +
            "            \"Anschlussnutzer\": \"Stahl,Sarah\",\n" +
            "            \"ErstelltDatum\": \"2024-03-18\",\n" +
            "            \"ErstelltZeit\": \"15:29:15+01:00\",\n" +
            "            \"AenderungDatum\": \"2024-03-18\",\n" +
            "            \"AenderungZeit\": \"15:29:15+01:00\",\n" +
            "            \"LetzteAenderung\": \"2024-04-23T13:20:57.867111+01:00\",\n" +
            "            \"Einheit\": \"\",\n" +
            "            \"Vorgangsdatum\": \"2024-03-18\",\n" +
            "            \"KonvSatzart\": 20,\n" +
            "            \"LaufendeNr\": 1,\n" +
            "            \"Satzart\": 27\n" +
            "        }\n" +
            "    ]\n" +
            "}\n";
    static List<Map<String, Object>> maps = new ArrayList<>();

    @BeforeAll
    static void initialization() {
        maps = JSONUtils.JSONFilter(testJson,
                "$.xpds_responseBody[?(@.setAktive == \"0\" && @.Status == \"\")]");
    }

    @Test
    void getAllSortedJSON() {
        List<String> stringList = JSONUtils.getAllSortedJSON(maps, "ableseDatum");
        System.out.println(stringList);
    }


    @Test
    void getSortedJSON() {
        String jsonstring = JSONUtils.getSortedJSON(maps, "ableseDatum", 0);
        assertEquals("{\"zaehlerStand\":\"8827\",\"ableseDatum\":\"2024-03-18\",\"ableseKennzeichen\":\"2\",\"ablesehinweis\":\"(2) Ablesung durch Kunden\",\"setAktive\":\"0\",\"Status\":\"\",\"Vorgang\":\"(27) Standmitteilung\",\"Anschlussnutzer\":\"Stahl,Sarah\",\"ErstelltDatum\":\"2024-03-18\",\"ErstelltZeit\":\"15:29:15+01:00\",\"AenderungDatum\":\"2024-03-18\",\"AenderungZeit\":\"15:29:15+01:00\",\"LetzteAenderung\":\"2024-04-23T13:20:57.867111+01:00\",\"Einheit\":\"\",\"Vorgangsdatum\":\"2024-03-18\",\"KonvSatzart\":20,\"LaufendeNr\":1,\"Satzart\":27}",
                jsonstring);
    }

    @Test
    void getAllSortedJSONWithConvert() {
        List<Map<String, Object>> mapList = JSONUtils.convertStringtoListOfMap(test2);
        List<String> stringList = JSONUtils.getAllSortedJSON(mapList, "ableseDatum");
        System.out.println(stringList);
    }


    @Test
    void getSortedJSONWithConvert() {
        List<Map<String, Object>> mapList = JSONUtils.convertStringtoListOfMap(test2);
        String jsonstring = JSONUtils.getSortedJSON(mapList, "ableseDatum", 0);
        assertEquals("{\"zaehlerStand\":\"9000\",\"ableseDatum\":\"2024-04-10\",\"ableseKennzeichen\":\"2\",\"ablesehinweis\":\"(2) Ablesung durch Kunden\",\"setAktive\":\"1\",\"Status\":\"\",\"Vorgang\":\"(27) Standmitteilung\",\"Anschlussnutzer\":\"Stahl,Sarah\",\"ErstelltDatum\":\"2024-04-23\",\"ErstelltZeit\":\"13:20:57+01:00\",\"AenderungDatum\":\"2024-04-23\",\"AenderungZeit\":\"13:20:57+01:00\",\"LetzteAenderung\":\"2024-04-23T13:20:57.867111+01:00\",\"Einheit\":\"\",\"Vorgangsdatum\":\"2024-04-10\",\"KonvSatzart\":20,\"LaufendeNr\":1,\"Satzart\":27}",
                jsonstring);
    }

    @Test
    void convertListOfMaptoJSON() {
        List<Map<String, Object>> mapList = JSONUtils.convertStringtoListOfMap(test2);
        String result = JSONUtils.convertListOfMaptoJSON(mapList);
        System.out.println(mapList);
        System.out.println(result);
    }

    @Test
    void getFilterTest() {
        List<Map<String, Object>> result = JSONUtils.JSONFilter(testJson, "$.xpds_responseBody[?(@.Satzart == 27)]");
        System.out.println(result);
        System.out.println(result.get(0).get("LetzteAenderung"));

        String test = "{\n" +
                "    \"kunden\": {\n" +
                "        \"something\": {},\n" +
                "        \"kundenSonstige\": [\n" +
                "            {\n" +
                "                \"attribute\": \"a1\",\n" +
                "                \"wert\": \"12345\"\n" +
                "            },\n" +
                "            {\n" +
                "                \"attribute\": \"a2\",\n" +
                "                \"wert\": \"45678\"\n" +
                "            }\n" +
                "        ]\n" +
                "    }\n" +
                "}";

        List<Map<String, Object>> result1 = JSONUtils.JSONFilter(test, "$.kunden.kundenSonstige[?(@.attribute == 'a2')]");
        System.out.println(result1.get(0).get("wert"));
        assertEquals("45678", result1.get(0).get("wert"));
    }

    @Test
    void getFilterTest1() {
        String vertragErstellenResponse = "{\n" +
                "    \"metaList\": [\n" +
                "        {\n" +
                "            \"code\": \"na-tenantNA\",\n" +
                "            \"message\": \"Schema WERK11 ist in Wartung\",\n" +
                "            \"origin\": null,\n" +
                "            \"referTo\": null,\n" +
                "            \"referToData\": null,\n" +
                "            \"type\": \"ACCESS_ERROR\"\n" +
                "        }\n" +
                "    ],\n" +
                "    \"data\": null\n" +
                "}\n";
        List<Map<String, Object>> errorMessage = JSONUtils.JSONFilter(vertragErstellenResponse, "$.metaList[?(@.message != null)]");
        if (!errorMessage.isEmpty())
            System.out.println(errorMessage.get(0).get("message"));
    }

    @Test
    void convertJSONToString() {
        String vertragErstellenResponse = "{\n" +
                "    \"header\": {\n" +
                "        \"meta\": \"\"\n" +
                "    },\n" +
                "    \"body\": {\n" +
                "        \"columnsJson\": \"\",\n" +
                "        \"userInputJson\": {\n" +
                "            \"vertragsId\": \"690248283G000000023\",\n" +
                "            \"ereignisdatum\": \"2024-06-21\"\n" +
                "        }\n" +
                "    }\n" +
                "}";
        System.out.println(JSONUtils.convertJSONToString(vertragErstellenResponse, "$.body.userInputJson"));
    }

    @Test
    void convertJSONToListOfMaps() {
        String testJSON = "{\n" +
                "    \"vertrag\": {\n" +
                "        \"vertragsId\": \"xxxxxxxxx\",\n" +
                "        \"vertragsart\": \"\",\n" +
                "        \"sparte\": \"GAS\",\n" +
                "        \"vertragsabschlussTyp\": \"D2D\",\n" +
                "        \"vertragsabschlussDatum\": \"2024-01-03\",\n" +
                "        \"vertragsstatus\": \"\",\n" +
                "        \"tarifKennzeichen\": \"HT|NT\",\n" +
                "        \"vertriebspartner\": \"\",\n" +
                "        \"vertragspartner\": {\n" +
                "            \"kundennummer\": \"123456789\",\n" +
                "            \"debitornummer\": \"1\",\n" +
                "            \"abschlag\": {\n" +
                "                \"betrag\": 100.0,\n" +
                "                \"mwStSatz\": 19.0,\n" +
                "                \"waehrung\": \"EUR\",\n" +
                "                \"frist\": \"MONAT\",\n" +
                "                \"faelligkeitsdatum\": \"2024-04-01\"\n" +
                "            }\n" +
                "        },\n" +
                "        \"vertragskonditionen\": {\n" +
                "            \"kuendigungsfrist\": {\n" +
                "                \"dauer\": 12,\n" +
                "                \"einheit\": \"WOCHEN|TAG|MONAT\",\n" +
                "                \"startdatum\": \"yyyy-MM-dd\",\n" +
                "                \"enddatum\": \"yyyy-MM-dd\"\n" +
                "            },\n" +
                "            \"vertragslaufzeit\": {\n" +
                "                \"dauer\": 12,\n" +
                "                \"einheit\": \"WOCHEN|TAG|MONAT\",\n" +
                "                \"startdatum\": \"yyyy-MM-dd\",\n" +
                "                \"enddatum\": \"yyyy-MM-dd\" \n" +
                "            },\n" +
                "            \"vertragsverlaengerung\": {\n" +
                "                \"dauer\": 12,\n" +
                "                \"einheit\": \"WOCHEN|TAG|MONAT\", \n" +
                "                \"startdatum\": \"yyyy-MM-dd\",\n" +
                "                \"enddatum\": \"yyyy-MM-dd\"\n" +
                "            }\n" +
                "        },\n" +
                "        \"verbrauchsstelle\": {\n" +
                "            \"lieferbeginn\": \"yyyy-MM-dd\",\n" +
                "            \"objektNummer\": \"xxxxxxxxx\",\n" +
                "            \"netznutzungProduktId\": \"xxxxxxxxx\",\n" +
                "            \"marktlokation\": {\n" +
                "                \"marktlokationIdent\": \"xxxxxxxxxx\",\n" +
                "                \"netzparameter\": {\n" +
                "                    \"marktgebiet\": \"THE\",\n" +
                "                    \"bilanzKreisKey\": \"\", \n" +
                "                    \"netzebene\": \"\",\n" +
                "                    \"konzessionsabgabe\": \"\",\n" +
                "                    \"verteilnetz\": {\n" +
                "                        \"netzbetreibercodenr\": \"\"\n" +
                "                    }\n" +
                "                }\n" +
                "            },\n" +
                "            \"messlokation\": {\n" +
                "                \"messlokationsIdent\": \"xxxxxxxxxxxx\",\n" +
                "                \"lastprofil\": \"H0\", \n" +
                "                \"messebene\": \"XX\",\n" +
                "                \"zaehler\": {\n" +
                "                    \"geraetenummer\": \"\",\n" +
                "                    \"messstellenbetreiber\": \"\"\n" +
                "                }\n" +
                "            }\n" +
                "        },\n" +
                "        \"prognostizierterVerbrauch\": [\n" +
                "            {\n" +
                "                \"attribute\": \"KWH/JAHR\",\n" +
                "                \"wert\": 1500\n" +
                "            },\n" +
                "            {\n" +
                "                \"attribute\": \"HaushaltAnzahl\",\n" +
                "                \"wert\": 3\n" +
                "            },\n" +
                "            {\n" +
                "                \"attribute\": \"HausGroesse in m2\",\n" +
                "                \"wert\": 47.5\n" +
                "            }\n" +
                "        ],\n" +
                "        \"tarif\": {\n" +
                "            \"produktId\": \"\"\n" +
                "        },\n" +
                "        \"vertragSonstigeAttribute\": [\n" +
                "            {\n" +
                "                \"attribute\": \"zaehlerstand\",\n" +
                "                \"wert\": 12345\n" +
                "            },\n" +
                "            {\n" +
                "                \"attribute\": \"ablesekennzeichen\",\n" +
                "                \"wert\": \"2\"\n" +
                "            },\n" +
                "            {\n" +
                "                \"attribute\": \"ablesedatum\",\n" +
                "                \"wert\": \"2024-01-01\"\n" +
                "            },\n" +
                "            {\n" +
                "                \"attribute\": \"zugangsArt\",\n" +
                "                \"wert\": \"BE\"\n" +
                "            },\n" +
                "            {\n" +
                "                \"attribute\": \"energieSteuer\",\n" +
                "                \"wert\": \"R\"\n" +
                "            },\n" +
                "            {\n" +
                "                \"attribute\": \"abgangsart\",\n" +
                "                \"wert\": \"\"\n" +
                "            },\n" +
                "            {\n" +
                "                \"attribute\": \"versorgungsArt\",\n" +
                "                \"wert\": \" \"\n" +
                "            },\n" +
                "            {\n" +
                "                \"attribute\": \"laufzeitSync\",\n" +
                "                \"wert\": true\n" +
                "            },\n" +
                "            {\n" +
                "                \"attribute\": \"ereignisdatum\",\n" +
                "                \"wert\": \"2024-06-14\"\n" +
                "            },\n" +
                "            {\n" +
                "                \"attribute\": \"abrechnungsgruppen\",\n" +
                "                \"wert\": \"Ablesung 30.01., Abrechnung 14.02.\"\n" +
                "            },\n" +
                "            {\n" +
                "                \"attribute\": \"kundenGruppenSchluessel\",\n" +
                "                \"wert\": \"675\"\n" +
                "            },\n" +
                "            {\n" +
                "                \"attribute\": \"haendlerKuerzel\",\n" +
                "                \"wert\": \"EVM \"\n" +
                "            },\n" +
                "            {\n" +
                "                \"attribute\": \"prozessId\",\n" +
                "                \"wert\": \"vertragssatz-ueberschreiben\"\n" +
                "            }\n" +
                "        ]\n" +
                "    }\n" +
                "}";
        JSONUtils.convertToFlattenJSON(testJSON);
    }

    @Test
    void convertStringToJSON() {
        String test = "\"{\\\"Lastprofil\\\":\\\"H0\\\",\\\"Anschlusswert6\\\":0,\\\"BonusbetragBrutto\\\":0.00,\\\"Versorgungsart\\\":\\\"\\\",\\\"EinheitMindestlaufzeit\\\":\\\"\\\",\\\"Kuendigungsgrund\\\":\\\"\\\",\\\"Verbrauchsart\\\":\\\"\\\",\\\"LaufzeitSync\\\":true,\\\"Anschlusswert12\\\":0.00000,\\\"EinheitKuendigungsfrist2\\\":\\\"\\\",\\\"Marktgebiet\\\":\\\"\\\",\\\"Messdienstleister\\\":\\\"\\\",\\\"Abschlagsbeginn\\\":\\\"2024-07-25\\\",\\\"UebertragungEDMSysteme\\\":\\\"\\\",\\\"Typ\\\":\\\"\\\",\\\"Anschlusswert2\\\":0,\\\"AblesekennzeichenAlt\\\":\\\"\\\",\\\"AutrargsIdVertragsPartner\\\":\\\"\\\",\\\"Zeitreihentyp\\\":\\\"SLS\\\",\\\"Stromsteuer\\\":\\\"\\\",\\\"Netzkopplungspunkt\\\":\\\"\\\",\\\"HauptNebenzaehler\\\":\\\"\\\",\\\"WievieltesZaehlwerk\\\":1,\\\"Anschlusswert15\\\":0.00000,\\\"FreifeldCrm4\\\":\\\"\\\",\\\"InfoKennzeichen\\\":\\\"\\\",\\\"TempMessstelle\\\":\\\"\\\",\\\"FreifeldCrm6\\\":\\\"\\\",\\\"NummerHauptzaehler\\\":0,\\\"Kuendigungsfrist1\\\":1,\\\"BonusschablonenID\\\":0,\\\"prognVerbrauch\\\":0.00000,\\\"FreifeldCrm9\\\":\\\"\\\",\\\"MURefGerateNummer\\\":0,\\\"Zaehlerfabrikat\\\":0,\\\"Bonusbeibehalten\\\":\\\"\\\",\\\"UnterVertriebspartner\\\":\\\"\\\",\\\"Vertragsverlaengerung\\\":0,\\\"Ereignisdatum\\\":\\\"2024-07-24\\\",\\\"PSKommEinhMSB\\\":0,\\\"Sperrkennzeichen\\\":\\\"\\\",\\\"FreifeldCrm8\\\":\\\"\\\",\\\"Lieferart\\\":\\\"\\\",\\\"Zaehleinrichtung\\\":\\\"\\\",\\\"IDZaehlzeitMelo\\\":0,\\\"Netzbetreibernummer\\\":134,\\\"FreifeldCrm1\\\":\\\"\\\",\\\"VertragslaufzeitBis\\\":\\\"2024-08-25\\\",\\\"AbschlagsvorgabeistNull\\\":false,\\\"MURefGeraeteArt\\\":\\\"\\\",\\\"MindestlaufzeitBonus\\\":0,\\\"Vertriebspartner\\\":\\\"evm\\\",\\\"Lieferbeginn\\\":\\\"2024-07-24\\\",\\\"Anschlusswert1\\\":0,\\\"FabrikatHauptzaehler\\\":0,\\\"IDZaehlzeitRegisterMelo\\\":0,\\\"Provision\\\":0.00000000,\\\"FreifeldCrm5\\\":\\\"\\\",\\\"Marktlokation\\\":\\\"D0002710029\\\",\\\"wievieltesZaehlwerkHauptzaehler\\\":0,\\\"Kuendigungsfrist2\\\":0,\\\"ZaehlzeitRegisterMelo\\\":0,\\\"Anschlusswert4\\\":0,\\\"Abschlagsvorgabe\\\":120.00,\\\"Berechnen\\\":\\\"J\\\",\\\"Mehrwertsteuersatz\\\":0.0000,\\\"Kundengruppenschluessel\\\":15,\\\"Haendlerkennung\\\":\\\"EVM\\\",\\\"Energiemengenabrechnung\\\":\\\"\\\",\\\"Freifeld5\\\":\\\"\\\",\\\"KuendigungZum\\\":\\\"\\\",\\\"VertragslaufzeitVon\\\":\\\"2024-07-25\\\",\\\"FreifeldCrm2\\\":\\\"\\\",\\\"Obis\\\":\\\"1-1:1.8.0\\\",\\\"EnergieartHauptzaehler\\\":\\\"\\\",\\\"AbschlagsvorgabeBeibehalten\\\":true,\\\"Vertragsart\\\":\\\"\\\",\\\"Zaehlernummer\\\":0,\\\"HTNT\\\":\\\"\\\",\\\"AngemeldeterVerbrauch\\\":1500.00000,\\\"IDZaehlzeitMalo\\\":0,\\\"Steuerungskennzeichen\\\":\\\"\\\",\\\"PreisAusENET\\\":\\\"J\\\",\\\"MURefGeraeteFabrikat\\\":0,\\\"NettoBetragBonus\\\":0.00,\\\"PreisschluesselWandlUmwandlMSB\\\":0,\\\"VertragsmengeJahr\\\":0,\\\"Befestigungsart\\\":\\\"\\\",\\\"Forderungsbetrag\\\":120.00,\\\"Bilanzierungskreiskennung\\\":\\\"11XKEVAG-V-----7\\\",\\\"Anschlusswert9\\\":0,\\\"Einspeiselieferant\\\":\\\"\\\",\\\"TechnSteuereinr\\\":\\\"\\\",\\\"Zaehlerstand\\\":0,\\\"FreifeldCrm7\\\":\\\"\\\",\\\"IDZaehlzeitRegisterMalo\\\":0,\\\"MindestmengeJahr\\\":0,\\\"BonusText\\\":\\\"\\\",\\\"Waermenutzung\\\":\\\"\\\",\\\"Unterbrechbarkeit\\\":\\\"\\\",\\\"Anschlusswert5\\\":0,\\\"NettoBonusProzent\\\":0.00,\\\"Messstellenbetreiber\\\":\\\"\\\",\\\"Strasse\\\":\\\"\\\",\\\"angemMengebeibehalt\\\":\\\"\\\",\\\"Mengenumwertertyp\\\":\\\"\\\",\\\"Zaehlertyp\\\":\\\"\\\",\\\"additiv\\\":\\\"J\\\",\\\"Abrechnungskennzeichen\\\":\\\"\\\",\\\"Anschlusswert7\\\":0,\\\"Anschlusswert3\\\":0,\\\"Mandant\\\":\\\"V\\\",\\\"Oeffnungsklausel\\\":\\\"\\\",\\\"GeraeteartHauptzaehler\\\":\\\"\\\",\\\"EinheitKuenFrist1\\\":\\\"T\\\",\\\"FreifeldCrm10\\\":\\\"\\\",\\\"Ablesekarte\\\":696230706,\\\"Einmalabzug\\\":0,\\\"Kommunikationseinr\\\":\\\"\\\",\\\"Geraeteart\\\":\\\"ZF\\\",\\\"Anschlusswert11\\\":0.00000,\\\"Ablesekennzeichen\\\":\\\"\\\",\\\"Energieart\\\":\\\"E\\\",\\\"RWEKennzeichenHauptzaehler\\\":0,\\\"FreifeldCrm3\\\":\\\"\\\",\\\"ZaehlzeitRegisterMalo\\\":0,\\\"PreisschluesselAbrechnung\\\":0,\\\"SpezifischeArbeit\\\":0.000,\\\"MindestmengeBonus\\\":0.00,\\\"ZaehlerstandAlt\\\":0,\\\"Kundenwert\\\":0.0000,\\\"ProzessId\\\":22,\\\"Bezirk\\\":9231,\\\"EreignisdatumKey\\\":\\\"2024-07-26\\\",\\\"LetzteAenderung\\\":\\\"2024-07-25-15.05.22.872047\\\",\\\"VertragsID\\\":\\\"696230706E000000001\\\"}\"";
        test = JSONUtils.convertStringToJSON(test);
        System.out.println(JSONUtils.convertToFlattenJSON(test));
    }
}