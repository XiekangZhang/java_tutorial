import de.xiekang.talend.DataMasking;

import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.lang.Integer;

public class Main {
    static Logger logger = Logger.getLogger(Main.class.getName());

    public static void main(String[] args) {
        System.out.println((int) (double) (77 / 50));
        String encoded = DataMasking.BASE64_ENCODER.apply("LIMA_11POSTEING-3321770".getBytes(StandardCharsets.UTF_8));
        System.out.println(encoded);
        System.out.println(new String(DataMasking.BASE64_DECODER.apply(encoded.getBytes(StandardCharsets.UTF_8))));
        System.out.println(new String(DataMasking.BASE64_DECODER.apply("TElNQV8xMVBPU1RFSU5HLTMzMjE3NzA".getBytes(StandardCharsets.UTF_8))));

        /**
        String mainKey = DataMasking.createRandomString(16);
        String[] stringsTobeMasked = {"\"{\\\"WarningCode\\\": [ \\\"AEP0379\\\"]}\""};
        logger.log(Level.WARNING, "main key: " + mainKey);


        for (String s : stringsTobeMasked) {
            logger.log(Level.WARNING, "original value: " + s);
            String masked = DataMasking.encryptAESGCM(s, mainKey);
            logger.log(Level.WARNING, "masked value: " + masked);
            String unmasked = DataMasking.decryptAESGCM(masked, mainKey);
            logger.log(Level.WARNING, "unmasked value: " + unmasked);
            logger.log(Level.WARNING, "-----------------------------------");
            //mainkey: 0WEwDTJIjPKURdOY
            //[encrypted]5Fa4UbP84jNcBy2abkUwoRDQ5aJIkybYD59bPtlrnSGyEE67cwS1VnKBmGQkrdDoVZ1NmkLTcUqUzk8IW/e7LsEu1Q==
            //[encrypted]MMTwvuu8T884/nphmqCvVj3r1Aj5NQvSumQWWv5yruZHmDH9TAM8J4qzgDqgh1033e4cA1Aq1RyA4atjDDtiEg==
        }

        String info = "xxxxxlL92HzloucSUprF3Jv4JmD3J7q3PQa6nE/59V7Du69eu9/w6m2YwMpk9lul2Stje9McMVagCF3JJyrHr0c6ulILGBz+eLAnheXT6A==";
        String key = info.substring(0, 16);
        String i = info.substring(16);
        System.out.println(key + ": " + i);
        logger.log(Level.WARNING, DataMasking.decryptAESGCM(i, key));**/


        /**
         String endpoint = "V1/vertraege/%s/zaehlerstand/vorgangsdatum/%s";
         String.format("V1/vertraege/%s/zaehlerstand/vorgangsdatum/%s", "123", "2020-01-01".replaceAll("\"", ""));

         String url = "\"{\"aaa\": \"bbb\",\"downloadUri\": \"https://www.google.com\", \"ccc\": \"ddd\"}\"";
         logger.log(Level.WARNING, url.replace(url.substring(url.indexOf("https"), url.indexOf("\"", url.indexOf("https"))), "https://www.bing.com")); **/
        //String s = "123465789";
        //Pattern pattern = Pattern.compile("-?\\d+(\\.\\d+)?");
        //Matcher matcher = pattern.matcher(s);
        //logger.log(Level.WARNING, "matcher: " + matcher.matches());
        //test("lsjfisjelkfsjkljdkfjskdflsdfslkdfkjklfjkjslfjierowirjekljflksl\n lsjdfiweutjirsjglksjgkhsdjfajskljlkdjfksjdkfjskdllllllllllljkdjfsldkkkkkkkkkkkkkkkkkkkkkkkkkjff");
        //merge();
        /**ArrayList<String> f = new ArrayList<>();
        f.add("element 1");
        //f.add("element 2");
        System.out.println(f.stream().collect(Collectors.joining(", Änderung um ")));
        HashMap<String, ArrayList<String>> t = new HashMap<>();**/
    }


    public static List<String> test(String input) {
        List<String> t = new ArrayList<>(Arrays.asList(input.split("\n")));
        logger.log(Level.WARNING, "t: " + t);
        List<String> a = new ArrayList<>();

        t.forEach(s -> {
            a.addAll(Arrays.asList(s.split("(?<=\\G.{80})")));
            System.out.println(a);
        });

        return t;
    }

    public static void merge() {
        String one = "{\"body\":{\"Kundennummer\":123456789,\"Debitornummer\":0,\"Geraetenummer\":\"\",\"Notizart\":\"K\",\"Kontakttyp\":\"S\",\"Kontaktgrund\":\"304\",\"Kontaktrichtung\":\"E\",\"Kontaktmedium\":\"X\",\"Kontaktzeitpunkt\":\"2024-03-07 14:00:25\",\"Kontaktstatus\":\"O\"}}";
        String two = "\"Data\": [ {\"Notiz\": \"Das ist eine Test Notiz. \"}, {\"Notiz\": \" Nur ein Kunde mit Vertrag kann ein Notiz erstellen. \"}, {\"Notiz\": \" Der Kunde ist zufrieden. \"}]";
        String merged = one.substring(0, one.lastIndexOf("}}")) + "," + two + one.substring(one.lastIndexOf("}}"));
        logger.log(Level.WARNING, "merged: " + merged);

        String string = "\"{\\\"BonusbetragBrutto\\\":0.0,\\\"Mandant\\\":\\\"V\\\",\\\"Messlokation\\\":\\\"DExxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx\\\",\\\"Zaehlernummer\\\":0,\\\"Zaehlerstand\\\":12345,\\\"BonusschablonenID\\\":0,\\\"Typ\\\":\\\"\\\",\\\"Messstellenbetreiber\\\":\\\"\\\",\\\"Ersatzwertbildungsgrund\\\":\\\"\\\",\\\"PreisAusENET\\\":\\\"J\\\",\\\"Ablesekennzeichen\\\":\\\"x\\\",\\\"Versorgungsart\\\":\\\"\\\",\\\"Netzbetreibernummer\\\":469,\\\"Ereignisdatum\\\":\\\"yyyy-MM-dd\\\",\\\"Lastprofil\\\":\\\"H0\\\",\\\"ObjektNummer\\\":\\\"9976 208 0021 01 0000\\\",\\\"Einspeiselieferant\\\":\\\"\\\",\\\"Energieart\\\":\\\"E\\\",\\\"Mehrwertsteuersatz\\\":0,\\\"Messdienstleister\\\":\\\"\\\",\\\"ProzessId\\\":227,\\\"Bezirk\\\":9976,\\\"EreignisdatumKey\\\":\\\"yyyy-MM-dd\\\",\\\"LetzteAenderung\\\":\\\"2024-04-23-15.24.18.531594\\\",\\\"VertragsID\\\":\\\"\\\",\\\"ZaehlerstandCheckbox\\\":false,\\\"unbart\\\":\\\"V\\\",\\\"LaufendeNummer\\\":0,\\\"konvertierteSatzart\\\":0}\"";
        String test = "\"\\\"{\\\\\\\"BonusbetragBrutto\\\\\\\":0.0,\\\\\\\"Mandant\\\\\\\":\\\\\\\"V\\\\\\\",\\\\\\\"Messlokation\\\\\\\":\\\\\\\"DExxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx\\\\\\\",\\\\\\\"Zaehlernummer\\\\\\\":0,\\\\\\\"Zaehlerstand\\\\\\\":12345,\\\\\\\"BonusschablonenID\\\\\\\":0,\\\\\\\"Typ\\\\\\\":\\\\\\\"\\\\\\\",\\\\\\\"Messstellenbetreiber\\\\\\\":\\\\\\\"\\\\\\\",\\\\\\\"Ersatzwertbildungsgrund\\\\\\\":\\\\\\\"\\\\\\\",\\\\\\\"PreisAusENET\\\\\\\":\\\\\\\"J\\\\\\\",\\\\\\\"Ablesekennzeichen\\\\\\\":\\\\\\\"x\\\\\\\",\\\\\\\"Versorgungsart\\\\\\\":\\\\\\\"\\\\\\\",\\\\\\\"Netzbetreibernummer\\\\\\\":469,\\\\\\\"Ereignisdatum\\\\\\\":\\\\\\\"yyyy-MM-dd\\\\\\\",\\\\\\\"Lastprofil\\\\\\\":\\\\\\\"H0\\\\\\\",\\\\\\\"ObjektNummer\\\\\\\":\\\\\\\"9976 208 0021 01 0000\\\\\\\",\\\\\\\"Einspeiselieferant\\\\\\\":\\\\\\\"\\\\\\\",\\\\\\\"Energieart\\\\\\\":\\\\\\\"E\\\\\\\",\\\\\\\"Mehrwertsteuersatz\\\\\\\":0,\\\\\\\"Messdienstleister\\\\\\\":\\\\\\\"\\\\\\\",\\\\\\\"ProzessId\\\\\\\":227,\\\\\\\"Bezirk\\\\\\\":9976,\\\\\\\"EreignisdatumKey\\\\\\\":\\\\\\\"yyyy-MM-dd\\\\\\\",\\\\\\\"LetzteAenderung\\\\\\\":\\\\\\\"2024-04-23-15.24.18.531594\\\\\\\",\\\\\\\"VertragsID\\\\\\\":\\\\\\\"\\\\\\\",\\\\\\\"ZaehlerstandCheckbox\\\\\\\":false,\\\\\\\"unbart\\\\\\\":\\\\\\\"V\\\\\\\",\\\\\\\"LaufendeNummer\\\\\\\":0,\\\\\\\"konvertierteSatzart\\\\\\\":0}\\\"\"";
    }


}
